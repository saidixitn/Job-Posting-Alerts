import logging, json, os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

# ===================== CONFIG =====================
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_WORKERS = 8
THREAD_TIMEOUT = 20  # seconds per domain

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")

local = MongoClient(LOCAL_MONGO_URI, serverSelectionTimeoutMS=5000)
CLIENT_CACHE = {}

# ===================== TELEGRAM SEND =====================
def send(cid, text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            json={"chat_id": cid, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        logging.error(f"Telegram send failed: {e}")

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

    if dtype == "proxy":
        return None, None
    if "sub" in dtype:
        return "directclients_prod", "Target_P4_Opt"
    return f"{clean}_prod", "Target_P4_Opt"

# ===================== FIXED MONGO CONNECTION =====================
def get_remote_client(db):
    if db in CLIENT_CACHE:
        return CLIENT_CACHE[db]

    rec = local["mongo_creds"]["creds"].find_one({"domain": db}, {"mongo_uri": 1})
    if not rec:
        logging.error(f"No mongo URI found for db={db}")
        return None

    uri = rec["mongo_uri"].strip()
    if uri.endswith("?"):
        uri = uri[:-1]

    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=80000,
            socketTimeoutMS=80000,
            tls=False,
            directConnection=True
        )
        client.admin.command("ping")
    except Exception as e:
        logging.error(f"Mongo connect failed for {db}: {e}")
        return None

    CLIENT_CACHE[db] = client
    return client

# ===================== FETCH =====================
def fast_fetch(col, emp, start_time, utc):
    q = {
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": {"$gte": start_time, "$lt": utc}
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
    hr2_start = hr1_start - timedelta(hours=1)

    hr1_end = hr1_start + timedelta(hours=1)
    hr2_end = hr1_start

    posted = hr = prev = 0

    for d in docs:
        ts = make_aware(d["gpost_date"])
        posted += 1
        if hr1_start <= ts < hr1_end:
            hr += 1
        elif hr2_start <= ts < hr2_end:
            prev += 1

    return posted, hr, prev, max(0, quota - posted)

# ===================== PROXY LOGIC =====================
def process_proxy_domain(dom, utc):

    name = dom["Domain"].strip().rstrip("/")
    emp = dom.get("EmployerId")
    quota = dom.get("Quota") or 5000

    db_name = dom.get("DB", "prod_jobiak_ai")
    coll_name = "jobsGoogleSubmittedLog"

    client = get_remote_client(db_name)
    if not client:
        return None

    col = client[db_name][coll_name]

    start_time = utc - timedelta(hours=2)

    match_stage = {"createdAt": {"$gte": start_time, "$lt": utc}}
    if emp:
        match_stage["employerId"] = emp

    try:
        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "hour": {
                            "$dateToString": {
                                "format": "%Y-%m-%d %H",
                                "date": "$createdAt"
                            }
                        }
                    },
                    "count": {"$sum": 1}
                }
            }
        ]
        aggr = list(col.aggregate(pipeline))
    except Exception as e:
        logging.error(f"Proxy aggregation failed for {name}: {e}")
        return None

    posted = sum(a["count"] for a in aggr)

    hr1_start = utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr2_start = hr1_start - timedelta(hours=1)

    hr1_key = hr1_start.strftime("%Y-%m-%d %H")
    hr2_key = hr2_start.strftime("%Y-%m-%d %H")

    hr = prev = 0

    for a in aggr:
        hour = a["_id"]["hour"]
        count = a["count"]
        if hour == hr1_key:
            hr = count
        elif hour == hr2_key:
            prev = count

    left = max(0, quota - posted)

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "Diff": hr - prev,
        "Queue": 0,
        "QuotaLeft": left
    }

# ===================== NORMAL DOMAIN LOGIC =====================
def process_domain(dom, utc):
    name = dom["Domain"].strip().rstrip("/")
    dtype = (dom.get("Domain Type", "") or "").lower()

    if dtype == "proxy":
        return process_proxy_domain(dom, utc)

    emp = dom.get("EmployerId")
    quota = dom.get("Quota") or 5000

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

    start_time = utc.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        docs = fast_fetch(col, emp, start_time, utc)
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

# ===================== STATE STORAGE =====================
def load_prev_state(domain):
    return local["domain_postings"]["domain_state"].find_one(
        {"domain": domain}, {"_id": 0}
    )

def save_state(row, utc):
    doc = {
        "domain": row["Domain"],
        "posted_prev": row["Posted"],
        "hr_prev": row["Hr"],
        "queue_prev": row["Queue"],
        "updatedAt": utc
    }
    local["domain_postings"]["domain_state"].update_one(
        {"domain": row["Domain"]},
        {"$set": doc},
        upsert=True
    )

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

    stopped = []
    queue_stuck = []
    drop = []
    push_more = []

    state_coll = local["domain_postings"]["domain_state"]
    domain_coll = local["domain_postings"]["domains"]

    for r in rows:

        name = r["Domain"]
        curr_posted = r["Posted"]
        curr_queue = r["Queue"]
        curr_hr = r["Hr"]
        quota_left = r["QuotaLeft"]

        # normalize for lookup
        norm = name.strip().lower().rstrip("/")

        raw = domain_coll.find_one({"Domain": norm})

        if raw and "Domain Type" in raw:
            dtype = raw["Domain Type"].strip().lower()
        else:
            dtype = ""

        prev = state_coll.find_one({"domain": name}) or {}
        if not prev:
            continue

        prev_posted = prev.get("posted_prev", 0)
        prev_hr = prev.get("hr_prev", 0)

        # 1️⃣ Posting Stopped
        if curr_posted == prev_posted and quota_left > 0:
            stopped.append(r)

        # 2️⃣ Queue Stuck (only non-proxy)
        if dtype != "proxy":
            if curr_queue > 0 and curr_posted == prev_posted and quota_left > 0:
                queue_stuck.append(r)

        # 3️⃣ Posting Drop
        if prev_hr > 0 and curr_hr < prev_hr:
            drop.append(r)

        # 4️⃣ Push More Jobs – exclude proxy
        if dtype != "proxy":
            if curr_queue < quota_left:
                push_more.append(r)

    alert_groups = {
        "Posting Stopped": stopped,
        "Queue Stuck — No Posting Flow": queue_stuck,
        "Posting Drop Than Previous Hr": drop,
        "Push More Jobs — Queue Too Small": push_more,
    }

    alerts = []
    for title, items in alert_groups.items():
        if not items:
            continue

        msg = (
            f"⚠️ <b>{title}</b>\n"
            f"UTC {utc:%H:%M} | IST {ist:%H:%M}\n"
            f"<b>{len(items)} domain(s) affected</b>\n\n"
        )

        for r in items:
            msg += (
                f"• <b>{r['Domain']}</b>\n"
                f"  Hr: {r['Hr']} | PrevHr: {r['Prev']}\n"
                f"  Queue: {r['Queue']} | Left: {r['QuotaLeft']}\n\n"
            )

        alerts.append(msg)

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
            except Exception as e:
                logging.error(f"Thread error: {e}")

    if not results:
        print("❌ No data.")
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)

    print_summary(results)

    alerts = build_alerts(results, utc, ist)

    for r in results:
        save_state(r, utc)

    for cid in get_chat_ids().values():
        for a in alerts:
            send(cid, a)

    print(f"\nDone in {(datetime.now() - start).total_seconds():.2f}s\n")
    logging.info("Run OK.")

if __name__ == "__main__":
    main()
