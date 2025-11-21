import logging, json, os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout

# ===================== CONFIG =====================
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_WORKERS = 8
THREAD_TIMEOUT = 20  # seconds per domain

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")

local = MongoClient(LOCAL_MONGO_URI, serverSelectionTimeoutMS=5000)
CLIENT_CACHE = {}

# ===================== TIME HELPERS =====================
def now_times():
    utc = datetime.now(timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return utc, ist

def quiet_hours(ist):
    return (ist.hour == 2 and ist.minute >= 30) or (3 <= ist.hour < 11) or (ist.hour == 11 and ist.minute < 30)

def make_aware(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

# ===================== CHAT IDS =====================
def get_chat_ids():
    if os.path.exists("chatids.json"):
        return json.load(open("chatids.json"))
    return {}

# ===================== DB ROUTING =====================
def pick_db(dtype, domain):
    dtype = (dtype or "").lower()
    clean = domain.split("/")[0].split(".")[0]

    if "proxy" in dtype:
        return None, None
    if "sub" in dtype:
        return "directclients_prod", "Target_P4_Opt"
    return f"{clean}_prod", "Target_P4_Opt"

def get_remote_client(db):
    if db in CLIENT_CACHE:
        return CLIENT_CACHE[db]

    rec = local["mongo_creds"]["creds"].find_one({"domain": db}, {"mongo_uri": 1})
    if not rec:
        return None

    client = MongoClient(
        rec["mongo_uri"],
        serverSelectionTimeoutMS=4000,
        connectTimeoutMS=4000,
        socketTimeoutMS=4000
    )
    CLIENT_CACHE[db] = client
    return client

# ===================== FETCH =====================
def fast_fetch(col, emp, day_start, utc):
    q = {
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": {"$gte": day_start, "$lt": utc}
    }
    if emp:
        q["employerId"] = emp
    return list(col.find(q, {"gpost_date": 1}))

def fetch_queue_count(col, emp):
    q = {"gpost": 3, "job_status": {"$ne": 3}}
    if emp:
        q["employerId"] = emp
    return col.count_documents(q)

# ===================== METRICS =====================
def compute_metrics(docs, quota, utc):
    hr1_start = utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr1_end   = hr1_start + timedelta(hours=1)

    hr2_start = hr1_start - timedelta(hours=1)
    hr2_end   = hr1_start

    posted = hr = prev = 0

    for d in docs:
        ts = make_aware(d["gpost_date"])
        posted += 1
        if hr1_start <= ts < hr1_end:
            hr += 1
        elif hr2_start <= ts < hr2_end:
            prev += 1

    return posted, hr, prev, max(0, quota - posted)

# ===================== PROCESS PER DOMAIN =====================
def process_domain(dom, utc):
    name = dom["Domain"].strip().rstrip("/")
    dtype = dom.get("Domain Type", "")
    emp = dom.get("EmployerId")
    quota = dom.get("Quota", 0)

    db, coll = pick_db(dtype, name)
    if not db:
        return None

    client = get_remote_client(db)
    if not client:
        return None

    try:
        client.admin.command("ping")
    except:
        return None

    col = client[db][coll]
    day_start = utc.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        docs = fast_fetch(col, emp, day_start, utc)
    except:
        return None

    posted, hr, prev, left = compute_metrics(docs, quota, utc)

    try:
        queue = fetch_queue_count(col, emp)
    except:
        queue = 0

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "Diff": hr - prev,
        "Queue": queue,
        "QuotaLeft": left
    }

# ===================== PRINT =====================
def print_summary(rows):
    print("\n======== RESULTS ========\n")
    for r in rows:
        print(f"{r['Domain']}")
        print(f"  Posted: {r['Posted']}")
        print(f"  Hr: {r['Hr']} | Prev: {r['Prev']} | Diff: {r['Diff']}")
        print(f"  Queue: {r['Queue']} | Left: {r['QuotaLeft']}")
        print("------------------------------------")
    print("==========================\n")

# ===================== ALERTS =====================
def build_alerts(rows, utc, ist):
    if quiet_hours(ist):
        return []

    alerts = []

    # 1. Posting stopped this hour
    stopped = [
        r for r in rows
        if r["Hr"] == 0 and r["Prev"] > 0
    ]

    # 2. Queue stuck (work exists but no posting)
    queue_stuck = [
        r for r in rows
        if r["Hr"] == 0 and r["Queue"] > 0 and r["QuotaLeft"] > 0
    ]

    # 3. Major slowdown vs previous hour
    slowdown = [
        r for r in rows
        if r["Prev"] > 20 and r["Hr"] < r["Prev"] * 0.6
    ]

    # 4. Never started
    not_started = [r for r in rows if r["Posted"] == 0]

    # 5. No queue but quota left
    no_queue = [
        r for r in rows
        if r["Queue"] == 0 and r["QuotaLeft"] > 0
    ]

    def block(title, arr):
        msg = f"⚠️ <b>{title}</b>\nUTC {utc:%H:%M} | IST {ist:%H:%M}\n\n"
        for r in arr:
            msg += (
                f"<b>{r['Domain']}</b>\n"
                f"Posted: {r['Posted']} | Hr: {r['Hr']} | Prev: {r['Prev']}\n"
                f"Queue: {r['Queue']} | Left: {r['QuotaLeft']}\n\n"
            )
        return msg

    if stopped:     alerts.append(block("Posting Stopped", stopped))
    if queue_stuck: alerts.append(block("Queue Stuck — No Posting Flow", queue_stuck))
    if slowdown:    alerts.append(block("Posting Drop > 40%", slowdown))
    if not_started: alerts.append(block("Not Started", not_started))
    if no_queue:    alerts.append(block("No Queue but Quota Left", no_queue))

    return alerts

# ===================== MAIN =====================
def main():
    start = datetime.now()
    utc, ist = now_times()

    domains = list(local["domain_postings"]["domains"].find({}, {"_id": 0}))
    print("Loaded domains:", len(domains))

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_domain, d, utc) for d in domains]

        for f in futures:
            try:
                r = f.result(timeout=THREAD_TIMEOUT)
                if r:
                    results.append(r)
            except FutureTimeout:
                print("⚠️ Thread timeout for one domain — skipping.")
            except:
                pass

    if not results:
        print("❌ No data.")
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)

    print_summary(results)

    alerts = build_alerts(results, utc, ist)
    for cid in get_chat_ids().values():
        for a in alerts:
            send(cid, a)

    print(f"\nDone in {(datetime.now() - start).total_seconds():.2f}s\n")
    logging.info("Run OK.")

if __name__ == "__main__":
    main()
