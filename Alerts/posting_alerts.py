import logging, os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

# ============================================================
# CONFIG
# ============================================================
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_WORKERS = 8
THREAD_TIMEOUT = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")

local = MongoClient(LOCAL_MONGO_URI, serverSelectionTimeoutMS=5000)
CLIENT_CACHE = {}

# ============================================================
# TELEGRAM SEND
# ============================================================
def send(cid, text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            json={"chat_id": cid, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        logging.error(f"Telegram send failed: {e}")

# ============================================================
# TIME
# ============================================================
def now_times():
    utc = datetime.now(timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return utc, ist

def quiet_hours(ist):
    start = ist.replace(hour=2, minute=30, second=0, microsecond=0)
    end   = ist.replace(hour=12, minute=30, second=0, microsecond=0)
    return start <= ist < end

def make_aware(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

# ============================================================
# CHAT IDS (Mongo-based)
# ============================================================
def get_chat_ids():
    rows = list(local["domain_postings"]["chat_ids"].find({}, {"_id": 0, "chat_id": 1}))
    return [r["chat_id"] for r in rows]

def get_admin_chat_id():
    row = local["domain_postings"]["chat_ids"].find_one(
        {"role": "admin"},
        {"_id": 0, "chat_id": 1}
    )
    if not row:
        logging.error("❌ No admin found in domain_postings.chat_ids")
        return None
    return row["chat_id"]

# ============================================================
# DB ROUTING
# ============================================================
def pick_db(dtype, domain):
    dtype = (dtype or "").lower()
    clean = domain.split("/")[0].split(".")[0]

    if dtype == "proxy":
        return None, None
    if "sub" in dtype:
        return "directclients_prod", "Target_P4_Opt"
    return f"{clean}_prod", "Target_P4_Opt"

# ============================================================
# FIXED MONGO CONNECTION (Atlas-safe)
# ============================================================
def get_remote_client(db):
    if db in CLIENT_CACHE:
        return CLIENT_CACHE[db]

    rec = local["mongo_creds"]["creds"].find_one({"domain": db}, {"mongo_uri": 1})
    if not rec:
        logging.error(f"No mongo URI found for db={db}")
        return None

    uri = rec["mongo_uri"].strip()

    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=10000,
            tls=False,
            directConnection=True
        )
        client.admin.command("ping")
    except Exception as e:
        logging.error(f"Mongo connect failed for {db}: {e}")
        return None

    CLIENT_CACHE[db] = client
    return client

# ============================================================
# FETCH
# ============================================================
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

# ============================================================
# METRICS
# ============================================================
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

# ============================================================
# PROXY LOGIC
# ============================================================
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
            {"$group": {
                "_id": {
                    "hour": {"$dateToString": {"format": "%Y-%m-%d %H", "date": "$createdAt"}}
                },
                "count": {"$sum": 1}
            }}
        ]
        aggr = list(col.aggregate(pipeline))
    except Exception as e:
        logging.error(f"Proxy aggregation failed for {name}: {e}")
        return None

    posted = sum(a["count"] for a in aggr)

    hr1_start = utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr2_start = hr1_start - timedelta(hours=1)

    hr = prev = 0
    hr1_key = hr1_start.strftime("%Y-%m-%d %H")
    hr2_key = hr2_start.strftime("%Y-%m-%d %H")

    for a in aggr:
        hour = a["_id"]["hour"]
        if hour == hr1_key:
            hr = a["count"]
        elif hour == hr2_key:
            prev = a["count"]

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "Diff": hr - prev,
        "Queue": 0,
        "QuotaLeft": max(0, quota - posted)
    }

# ============================================================
# NORMAL DOMAIN LOGIC
# ============================================================
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

# ============================================================
# STATE STORAGE
# ============================================================
def save_state(row, utc):
    local["domain_postings"]["domain_state"].update_one(
        {"domain": row["Domain"]},
        {"$set": {
            "posted_prev": row["Posted"],
            "hr_prev": row["Hr"],
            "queue_prev": row["Queue"],
            "updatedAt": utc
        }},
        upsert=True
    )

# ============================================================
# FORMATTER (k-format)
# ============================================================
def fmt_k(n):
    return f"{round(n/1000, 1)}k"

# ============================================================
# ALERTS
# ============================================================
def build_alerts(rows, utc, ist):
    if quiet_hours(ist):
        return [], []

    total_posted = sum(r["Posted"] for r in rows)

    # Buckets
    posting_stopped = []
    queue_stuck = []
    posting_drop = []
    push_more = []
    posting_zero_hour = []

    state_coll = local["domain_postings"]["domain_state"]
    domain_coll = local["domain_postings"]["domains"]

    for r in rows:

        name = r["Domain"]
        curr_posted = r["Posted"]
        curr_queue = r["Queue"]
        curr_hr = r["Hr"]
        quota_left = r["QuotaLeft"]

        norm = name.strip().lower().rstrip("/")
        raw = domain_coll.find_one({"Domain": norm})
        dtype = raw.get("Domain Type", "").strip().lower() if raw else ""

        prev = state_coll.find_one({"domain": name}) or {}
        prev_posted = prev.get("posted_prev", 0)
        prev_hr = prev.get("hr_prev", 0)

        # 1) Posting Stopped (Today)
        if curr_posted == prev_posted and quota_left > 0:
            posting_stopped.append(r)

        # 2) Queue Stuck
        if dtype != "proxy":
            if curr_queue > 0 and curr_posted == prev_posted and quota_left > 0:
                queue_stuck.append(r)

        # 3) Posting Drop >500 (Hour)
        if prev_hr > 0 and curr_hr < prev_hr:
            diff = prev_hr - curr_hr
            if diff > 500:
                r["DropDiff"] = diff
                posting_drop.append(r)

        # 4) Push More Jobs — Admin Only
        if dtype != "proxy":
            left_today = quota_left
            push_needed = max(0, left_today - curr_queue)
            if push_needed > 0:
                r["PushAmountK"] = fmt_k(push_needed)
                push_more.append(r)

        # 5) Zero posts this hour
        if prev_hr > 0 and curr_hr == 0:
            posting_zero_hour.append(r)

    # ---------------------------
    # Definitions for admin/user
    # ---------------------------
    admin_groups = {
        "Posting Stopped": posting_stopped,
        "Queue Stuck - No Posting Flow": queue_stuck,
        "Posting Drop >500 Than Previous Hr": posting_drop,
        "Push More Jobs": push_more,
        "Posting Stopped - No Postings This Hour": posting_zero_hour
    }

    user_groups = {
        "Posting Stopped": posting_stopped,
        "Queue Stuck - No Posting Flow": queue_stuck,
        "Posting Drop >500 Than Previous Hr": posting_drop
        "Posting Stopped - No Postings This Hour": posting_zero_hour
    }

    # ---------------------------
    # Helper to format messages
    # ---------------------------
    def build_message(title, items):
        msg = (
            f"⚠️ <b>{title}</b>\n"
            f"UTC {utc:%H:%M} | IST {ist:%H:%M}\n"
            f"<b>{len(items)} domain(s) affected</b>\n"
            f"📦 Total Posted Today: <b>{fmt_k(total_posted)}</b>\n\n"
        )

        for r in items:
            msg += f"• <b>{r['Domain']}</b>\n"
            msg += f"  Hr: {r['Hr']} | PrevHr: {r['Prev']}\n"
            msg += f"  Queue: {fmt_k(r['Queue'])}\n"

            if 'DropDiff' in r:
                msg += f"  Drop: {fmt_k(r['DropDiff'])}\n"

            if 'PushAmountK' in r:
                msg += f"  Push: {r['PushAmountK']} jobs\n"

            msg += f"  Left today: {fmt_k(r['QuotaLeft'])}\n\n"

        return msg

    # ---------------------------
    # Build actual alert lists
    # ---------------------------
    admin_alerts = []
    user_alerts = []

    for title, items in admin_groups.items():
        if items:
            admin_alerts.append(build_message(title, items))

    for title, items in user_groups.items():
        if items:
            user_alerts.append(build_message(title, items))

    return admin_alerts, user_alerts

# ============================================================
# MAIN
# ============================================================
def main():
    start = datetime.now()
    utc, ist = now_times()

    if quiet_hours(ist):
        print(f"⏳ Quiet hours (IST {ist:%H:%M}) — Skipping execution.")
        logging.info("Quiet hours — Script stopped before running.")
        return

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
            except:
                pass

    if not results:
        print("❌ No data.")
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)

    # ADMIN CHAT ID
    admin_cid = get_admin_chat_id()

    # SEND SUMMARY TO ADMIN (ONLY)
    if admin_cid:
        summary = "📊 <b>Posting Summary</b>\n\n"
        for r in results:
            summary += (
                f"• <b>{r['Domain']}</b>\n"
                f"  Posted: {fmt_k(r['Posted'])}\n"
                f"  Hr: {r['Hr']} | PrevHr: {r['Prev']}\n"
                f"  Queue: {fmt_k(r['Queue'])} | Left: {fmt_k(r['QuotaLeft'])}\n\n"
            )
        send(admin_cid, summary)

    # BUILD ALERTS (ADMIN + USER)
    admin_alerts, user_alerts = build_alerts(results, utc, ist)

    # SAVE STATE FOR ALL DOMAINS
    for r in results:
        save_state(r, utc)

    # SEND ALL ALERTS TO ADMIN
    if admin_cid:
        for a in admin_alerts:
            send(admin_cid, a)

    # SEND REGULAR ALERTS TO NON-ADMIN USERS
    chat_ids = get_chat_ids()
    for cid in chat_ids:
        if admin_cid and cid == admin_cid:
            continue
        for a in user_alerts:
            send(cid, a)

    print(f"\nDone in {(datetime.now()-start).total_seconds():.2f}s\n")
    logging.info("Run OK.")


if __name__ == "__main__":
    main()
