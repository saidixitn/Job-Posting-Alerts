import logging, json, os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== CONFIG =====================
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_WORKERS = 8

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")

local = MongoClient(LOCAL_MONGO_URI)
CLIENT_CACHE = {}


# ===================== TIME HELPERS =====================
def now_times():
    utc = datetime.now(timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return utc, ist

def quiet_hours(ist):
    return (ist.hour == 2 and ist.minute >= 30) or (3 <= ist.hour < 11) or (ist.hour == 11 and ist.minute < 30)


# ===================== TZ FIX =====================
def make_aware(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ===================== CHAT IDS =====================
def get_chat_ids():
    s = os.getenv("CHAT_IDS_JSON")
    if s:
        try: return json.loads(s)
        except: pass

    if os.path.exists("chatids.json"):
        return json.load(open("chatids.json"))

    return {}


# ===================== DB ROUTING =====================
def pick_db(dtype, domain):
    dtype = dtype.lower()
    clean = domain.split(".")[0]

    if dtype in ["programmatic", "prog"]:
        return f"{clean}_prod", "Target_P4_Opt"
    if "sub" in dtype:
        return "directclients_prod", "Target_P4_Opt"

    return None, None


def get_remote_client(db):
    if db in CLIENT_CACHE:
        return CLIENT_CACHE[db]

    rec = local["mongo_creds"]["creds"].find_one({"domain": db}, {"mongo_uri": 1})
    if not rec:
        return None

    uri = rec["mongo_uri"]
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    CLIENT_CACHE[db] = client
    return client


# ===================== FAST FETCH (1 QUERY ONLY) =====================
def fast_fetch(col, emp, day_start, utc):
    base_filter = {
        "gpost": {"$in": [3, 5]},
        "job_status": {"$ne": 3},
        "gpost_date": {"$gte": day_start, "$lt": utc},
    }

    if emp:
        base_filter["employerId"] = emp

    return list(col.find(base_filter, {"gpost": 1, "gpost_date": 1}))


def compute_metrics(docs, quota, utc):
    hr1_start = utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr1_end   = hr1_start + timedelta(hours=1)

    hr2_start = hr1_start - timedelta(hours=1)
    hr2_end   = hr1_start

    posted = queue = hr = prev = 0

    for d in docs:
        ts = make_aware(d["gpost_date"])
        g  = d["gpost"]

        if g == 5:
            posted += 1
            if hr1_start <= ts < hr1_end:
                hr += 1
            elif hr2_start <= ts < hr2_end:
                prev += 1
        elif g == 3:
            queue += 1

    return posted, hr, prev, queue, max(0, quota - posted)


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

    docs = fast_fetch(col, emp, day_start, utc)
    posted, hr, prev, queue, left = compute_metrics(docs, quota, utc)

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "Diff": hr - prev,
        "Queue": queue,
        "QuotaLeft": left
    }


# ===================== TERMINAL OUTPUT =====================
def print_summary(rows):
    print("\n======== DOMAIN RESULTS ========\n")
    for r in rows:
        print(f"Domain:       {r['Domain']}")
        print(f"Posted:       {r['Posted']}")
        print(f"Hr:           {r['Hr']}")
        print(f"Prev:         {r['Prev']}")
        print(f"Diff:         {r['Diff']}")
        print(f"Queue:        {r['Queue']}")
        print(f"Quota Left:   {r['QuotaLeft']}")
        print("------------------------------------")
    print("================================\n")


# ===================== ALERTS =====================
def build_alerts(rows, utc, ist):
    if quiet_hours(ist):
        return []

    alerts = []

    drops       = [r for r in rows if abs(r["Diff"]) >= 500]
    no_queue    = [r for r in rows if r["Queue"] == 0 and r["QuotaLeft"] > 0]
    not_started = [r for r in rows if r["Posted"] == 0]
    stopped     = [r for r in rows if r["Hr"] == 0 and r["Queue"] == 0]

    def block(title, arr):
        msg = f"⚠️ <b>{title}</b>\nUTC {utc:%H:%M} | IST {ist:%H:%M}\n\n"
        for r in arr:
            msg += (
                f"<b>{r['Domain']}</b>\n"
                f"Posted: {r['Posted']}\n"
                f"Hr: {r['Hr']} (prev {r['Prev']}) | Diff: {r['Diff']}\n"
                f"Queue: {r['Queue']}\n"
                f"Quota Left: {r['QuotaLeft']}\n\n"
            )
        return msg

    if drops: alerts.append(block("Posting Change > 500", drops))
    if no_queue: alerts.append(block("No Queue but Quota Left", no_queue))
    if not_started: alerts.append(block("No Postings Started", not_started))
    if stopped: alerts.append(block("Posting Stopped", stopped))

    return alerts


# ===================== TELEGRAM =====================
def send(cid, msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except:
        pass


# ===================== MAIN =====================
def main():
    start_time = datetime.now()  # TIMER ADDED

    utc, ist = now_times()

    domains = list(local["domain_postings"]["domains"].find({}, {"_id": 0}))
    print("Loaded domains:", len(domains))

    if not domains:
        print("No domains found in database.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(process_domain, d, utc) for d in domains]):
            r = f.result()
            if r:
                results.append(r)

    if not results:
        print("No valid results for any domain.")
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)

    print_summary(results)

    alerts = build_alerts(results, utc, ist)
    chat_ids = get_chat_ids()

    for cid in chat_ids.values():
        for a in alerts:
            send(cid, a)

    # EXECUTION TIME PRINT
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"\nScript finished in {duration:.2f} seconds\n")

    logging.info("Run OK.")


if __name__ == "__main__":
    main()
