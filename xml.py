import requests
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import time
import os

# ======= CONFIG =======

# Mongo URI will be injected via GitHub Secret
MONGO_URI = os.getenv("MONGO_URI")

# Everything else remains in code
DB_NAME = "xml_timings"
SOURCE_COLLECTION = "feed_timings"
HISTORY_COLLECTION = "feed_timings_history"

IST_OFFSET = timedelta(hours=5, minutes=30)

# ---------- Helpers ---------- #

def utc_now():
    """Return UTC now (timezone aware)."""
    return datetime.now(timezone.utc)


def to_ist(dt_utc):
    """IST = UTC + 5 hours 30 min."""
    return dt_utc + IST_OFFSET


def fetch_last_modified(url):
    """Fetch Last-Modified header."""
    try:
        head = requests.head(url, timeout=10)
    except:
        return None

    last_mod = head.headers.get("Last-Modified")
    if not last_mod:
        return None

    try:
        lm = datetime.strptime(last_mod, "%a, %d %b %Y %H:%M:%S %Z")
        lm = lm.replace(tzinfo=timezone.utc)
        return lm
    except:
        return None


# ---------- Processing Logic ---------- #

def process_feed(feed, history):
    employerId = feed.get("employerId")
    xml_url = feed.get("xml_url")
    employerName = feed.get("employerName")

    print(f"🔍 Checking {employerId}")

    last_mod_utc = fetch_last_modified(xml_url)
    if last_mod_utc is None:
        print("⚠️ No Last-Modified — skipping\n")
        return

    last_mod_ist = to_ist(last_mod_utc)

    now_utc = utc_now()
    now_ist = to_ist(now_utc)
    today = now_utc.strftime("%Y-%m-%d")

    # Existing history
    doc = history.find_one({"_id": employerId})

    # ---------- FIRST TIME ----------
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

    # ---------- NO CHANGE ----------
    if doc.get("xml_last_updated_utc") == last_mod_utc:
        print("⏩ No change detected\n")
        return

    # ---------- FEED UPDATED ----------
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
        print("❌ ERROR: Environment variable MONGO_URI not set")
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    feeds = list(db[SOURCE_COLLECTION].find({}))
    history = db[HISTORY_COLLECTION]

    print("\n🚀 Starting XML Monitoring...\n")

    with ThreadPoolExecutor(max_workers=12) as executor:
        for feed in feeds:
            executor.submit(process_feed, feed, history)


if __name__ == "__main__":
    main()
