import requests
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import email.utils
import os

# ======= CONFIG =======
MONGO_URI = os.getenv("MONGO_URI")  # GitHub Secret
DB_NAME = "xml_timings"
SOURCE_COLLECTION = "feed_timings"
HISTORY_COLLECTION = "feed_timings_history"

IST_OFFSET = timedelta(hours=5, minutes=30)

# ---------- Helpers ---------- #

def utc_now():
    return datetime.now(timezone.utc)

def to_ist(dt_utc):
    return dt_utc + IST_OFFSET

def normalize_last_modified(raw_value):
    """
    Normalize Last-Modified header to a stable UTC datetime.
    Prevents false refresh detections due to formatting differences.
    """
    try:
        dt = email.utils.parsedate_to_datetime(raw_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        # Remove microseconds for stable comparison
        dt = dt.replace(microsecond=0)

        return dt
    except:
        return None


def fetch_last_modified(url):
    """HEAD request + normalize Last-Modified."""
    try:
        head = requests.head(url, timeout=10, allow_redirects=True)
    except:
        return None

    raw = head.headers.get("Last-Modified")
    if not raw:
        return None

    return normalize_last_modified(raw)


# ---------- Core Processing ---------- #

def process_feed(feed, history):
    employerId = feed["employerId"]
    employerName = feed["employerName"]
    xml_url = feed["xml_url"]

    print(f"🔍 Checking {employerId}")

    last_mod_utc = fetch_last_modified(xml_url)

    # No valid last-modified? Skip
    if last_mod_utc is None:
        print("⚠️ No valid Last-Modified — skipping\n")
        return

    last_mod_ist = to_ist(last_mod_utc)

    now_utc = utc_now()
    now_ist = to_ist(now_utc)
    today = now_utc.strftime("%Y-%m-%d")

    # Fetch existing history doc
    doc = history.find_one({"_id": employerId})

    # -------- FIRST ENTRY -------- #
    if not doc:
        history.insert_one({
            "_id": employerId,
            "employerId": employerId,
            "employerName": employerName,
            "xml_url": xml_url,
            "refresh_count": 1,
            "xml_last_updated_utc": last_mod_utc,
            "xml_last_updated_ist": last_mod_ist,
            "refresh_log": [
                {
                    "date": today,
                    "count": 1,
                    "times": [
                        {
                            "checked_utc": now_utc,
                            "checked_ist": now_ist,
                            "xml_last_updated_utc": last_mod_utc,
                            "xml_last_updated_ist": last_mod_ist
                        }
                    ]
                }
            ]
        })
        return

    # -------- SAFE UPDATE CHECK -------- #
    prev = doc.get("xml_last_updated_utc")

    # 🛑 If previous = current → DO NOTHING
    if prev == last_mod_utc:
        print("⏩ No new update — skipping\n")
        return

    # -------- FEED UPDATED -------- #
    print("🔥 Feed updated — logging\n")

    total = doc.get("refresh_count", 0) + 1
    daily = doc.get("refresh_log", [])

    today_entry = next((d for d in daily if d["date"] == today), None)

    new_event = {
        "checked_utc": now_utc,
        "checked_ist": now_ist,
        "xml_last_updated_utc": last_mod_utc,
        "xml_last_updated_ist": last_mod_ist
    }

    if today_entry:
        today_entry["times"].append(new_event)
        today_entry["count"] += 1
    else:
        daily.append({
            "date": today,
            "count": 1,
            "times": [new_event]
        })

    # Update the main document
    history.update_one(
        {"_id": employerId},
        {
            "$set": {
                "refresh_count": total,
                "xml_last_updated_utc": last_mod_utc,
                "xml_last_updated_ist": last_mod_ist,
                "refresh_log": daily
            }
        }
    )


# ---------- Runner ---------- #

def main():
    if not MONGO_URI:
        print("❌ ERROR: MONGO_URI not provided")
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    feeds = list(db[SOURCE_COLLECTION].find({}))
    history = db[HISTORY_COLLECTION]

    print("\n🚀 Starting XML Monitoring (Stable Mode)...\n")

    with ThreadPoolExecutor(max_workers=12) as executor:
        for feed in feeds:
            executor.submit(process_feed, feed, history)


if __name__ == "__main__":
    main()
