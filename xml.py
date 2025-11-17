import requests
from pymongo import MongoClient, ReturnDocument
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import email.utils
import os

# ======= CONFIG =======
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "xml_timings"
SOURCE_COLLECTION = "feed_timings"
HISTORY_COLLECTION = "feed_timings_history"
LOCK_COLLECTION = "feed_update_lock"

IST_OFFSET = timedelta(hours=5, minutes=30)

# ---------- Helpers ---------- #

def utc_now():
    return datetime.now(timezone.utc)

def to_ist(dt_utc):
    return dt_utc + IST_OFFSET

def normalize_last_modified(raw):
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.replace(microsecond=0)
    except:
        return None

def fetch_last_modified(url):
    try:
        head = requests.head(url, timeout=10, allow_redirects=True)
    except:
        return None

    raw = head.headers.get("Last-Modified")
    if not raw:
        return None

    return normalize_last_modified(raw)

# ---------- Locking System ---------- #

def acquire_lock(lock_col, employerId, last_mod_utc):
    """
    Try to insert lock document. If it exists → skip update.
    Lock expires every 10 minutes.
    """

    now = utc_now()
    expire_time = now - timedelta(minutes=10)

    # Remove expired locks
    lock_col.delete_many({"timestamp": {"$lt": expire_time}})

    # Try to insert lock
    result = lock_col.find_one_and_update(
        {"_id": employerId},
        {
            "$setOnInsert": {
                "_id": employerId,
                "timestamp": now,
                "last_mod_utc": last_mod_utc
            }
        },
        upsert=True,
        return_document=ReturnDocument.BEFORE
    )

    # If result is None → lock acquired
    # If result exists → someone else is updating → skip
    return result is None


# ---------- Core Processing ---------- #

def process_feed(feed, history, lock_col):
    employerId = feed["employerId"]
    employerName = feed["employerName"]
    xml_url = feed["xml_url"]

    print(f"🔍 Checking {employerId}")

    last_mod_utc = fetch_last_modified(xml_url)
    if last_mod_utc is None:
        print("⚠️ No valid Last-Modified — skipping\n")
        return

    last_mod_ist = to_ist(last_mod_utc)

    now_utc = utc_now()
    now_ist = to_ist(now_utc)
    today = now_utc.strftime("%Y-%m-%d")

    doc = history.find_one({"_id": employerId})

    # -------- FIRST ENTRY -------- #
    if not doc:
        if not acquire_lock(lock_col, employerId, last_mod_utc):
            print("⏩ Another thread writing — skipping\n")
            return

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

    prev = doc.get("xml_last_updated_utc")

    # -------- NO CHANGE -------- #
    if prev == last_mod_utc:
        print("⏩ No new update — skipping\n")
        return

    # -------- TRY TO ACQUIRE LOCK -------- #
    if not acquire_lock(lock_col, employerId, last_mod_utc):
        print("⏩ Another thread already updating — skipping\n")
        return

    # -------- UPDATE -------- #
    print("🔥 Feed updated — logging")

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
        print("❌ ERROR: MONGO_URI not provided")
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    feeds = list(db[SOURCE_COLLECTION].find({}))
    history = db[HISTORY_COLLECTION]
    lock_col = db[LOCK_COLLECTION]

    print("\n🚀 Starting XML Monitoring (NO-DUPLICATE MODE)...\n")

    with ThreadPoolExecutor(max_workers=12) as executor:
        for feed in feeds:
            executor.submit(process_feed, feed, history, lock_col)


if __name__ == "__main__":
    main()
