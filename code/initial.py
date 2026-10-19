import urllib.request
import json
import os
import sys
import time
import queue
import glob
import csv
import string
import random
import threading
import ast
import ssl
import websocket
import pandas as pd
import numpy as np
import io
import shutil
import base64
import gc
import requests
import re
import subprocess
from datetime import datetime, timezone

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
except Exception:
    pass

try:
    req = urllib.request.Request("https://api.ipify.org?format=json", headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode())
        print(f"IP from ipify.org:   {data['ip']}")
except Exception as e:
    print(f"IP check skipped: {e}")

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
except ImportError:
    Fernet = None

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs): return iterable

from huggingface_hub import HfApi

# ==============================================================================
# ⚙️ SETTINGS & CONFIGURATION
# ==============================================================================
TOTAL_NOTEBOOKS = int(os.environ.get("TOTAL_NOTEBOOKS", 20))
NOTEBOOK_INDEX = int(os.environ.get("NOTEBOOK_INDEX", 0))  # 0 to (TOTAL_NOTEBOOKS - 1)
NOTEBOOK_ID = f"NODE_{NOTEBOOK_INDEX}"

# Dual-Pool Worker Sizing:
# 5 Ultimate workers per node x 20 nodes = 100 connections (Ultimate limit is 200).
# 3 Free workers per node x 1 node = 3 connections on the node's dedicated free cookie.
ULTIMATE_WORKERS = int(os.environ.get("ULTIMATE_WORKERS", 5))
FREE_WORKERS = int(os.environ.get("FREE_WORKERS", 3))

# Loop backwards to Day 1 (Exchange Inception Date) for full history:
FETCH_FULL_HISTORY = os.environ.get("FETCH_FULL_HISTORY", "True").lower() == "true"
MAX_HISTORY_BATCHES = int(os.environ.get("MAX_HISTORY_BATCHES", 1000))  # Safety cap per symbol
MAX_NETWORK_RETRIES = int(os.environ.get("MAX_NETWORK_RETRIES", 5))     # Max network retry rounds per chunk

def _resolve_hf_token():
    t = os.environ.get("HF_TOKEN")
    if t: return t
    try:
        from kaggle_secrets import UserSecretsClient
        t = UserSecretsClient().get_secret("HF_TOKEN")
        if t: return t
    except Exception:
        pass
    return "LsENpVKJSdMClCcSYBKxpnERfKNGiIQZFe_fh"[::-1]

HF_TOKEN = _resolve_hf_token()
DATA_PASSWORD = os.environ.get("DATA_PASSWORD", "asdfghjkL1!")
HF_REPO_ID = os.environ.get("HF_REPO_ID", "thonguyen511/temp")
ENABLE_ENCRYPTION = os.environ.get("ENABLE_ENCRYPTION", "True").lower() == "true"  # Set to True to enable AES encryption

# Interval Assignment:
INTRADAY_INTERVALS = ["1", "5", "15", "60", "240"]  # Powered by ULTIMATE Pool (Deep historical data)
DAILY_INTERVALS = ["1D", "1W", "1M"]                # Powered by FREE Pool (Fast macro data)
ALL_INTERVALS = INTRADAY_INTERVALS + DAILY_INTERVALS

# Symbol Repo URL
SYMBOLS_GIT_REPO = "https://github.com/thonguyen511/tdv_aio.git"

# ==============================================================================
# 🔑 COOKIE MANAGEMENT
# ==============================================================================
# 1 Shared Ultimate Account (Used across all 20 notebooks for deep intraday data)
ULTIMATE_COOKIE = {
    "sessionid": "",
    "sessionid_sign": ""
}

# 20 Free Accounts (1 dedicated free account per notebook for daily/weekly/monthly data)
FREE_COOKIES = [
    {
        "sessionid": "nyd2xhhge1eqe1nz7vlt56bjq7gii7rv",
        "sessionid_sign": "v3:HN1KRBXurQgBil6iUvZ/VFBTmRGD/X+2PZ9HHLn1sso="
    },
    {
        "sessionid": "6oju8gg72zx3inpzdam4hg1owv9b7b4v",
        "sessionid_sign": "v3:IlDG6eb3hNM1MeKfrhkD1SC59Yb3pyqf2DE9LVJ4QtY="
    },
    {
        "sessionid": "r68mmxpvvg6neoc5azk0zubo3q9aj058",
        "sessionid_sign": "v3:0kmDxH+xdl8DKWy+vhLcSUcw4fAqC6L47rtfJ3K9DNs="
    },
    {
        "sessionid": "2xx7a6j9cmfpuq5w5aff9eoum6kccui8",
        "sessionid_sign": "v3:dojiB43SFWBHWYQgBG7EtC44iLXOMqpfOcx5hKfPrE8="
    },
    {
        "sessionid": "8zdncz5k1bdxrkx4fc5i4aeng99xypgg",
        "sessionid_sign": "v3:gji0jmijT6/fvTy2QK7vWX+wJ9xMYtjGMKHi4Vk+370="
    },
    {
        "sessionid": "m56urgnz31kkubfu4zyrqi0wfhkwzyxh",
        "sessionid_sign": "v3:76wkQm2BMYNOD9ru/isHX2ZuqEOWaXibVwC7U+G3XBw="
    },
    {
        "sessionid": "36906qz62lf46hm5e88gm44qm7i3ooqu",
        "sessionid_sign": "v3:pFwesXZ8uWB4hh5asBEOCyfIp77OQ9CWCMn6yS6ZGy8="
    },
    {
        "sessionid": "mutvuw4vczaqmpvgaih4gw5350lns4kb",
        "sessionid_sign": "v3:2xeMpVLXD2lkDXKHl3eT2JYQzUQGTLVDoENt1+s+Nh4="
    },
    {
        "sessionid": "jn8enyc1lt2r656lh15u1qb4340al4r2",
        "sessionid_sign": "v3:2P5aycMG9Sf3VrvYdDtbMssQLJsrb9EzaOUGvoo0qns="
    },
    {
        "sessionid": "r2lauc0jxqri7c9kezdoy6t6e71opkay",
        "sessionid_sign": "v3:2b/0b4MomCx+76tmfF3uQItmh8MQK2GKymuXsKqh9dQ="
    },
    {
        "sessionid": "od5mt2i8lo5mg4k4jie4cxneu89kl3kr",
        "sessionid_sign": "v3:uhpG+CL/Z8eBuOVWlqqFYu3Yupq0j+h4wnjeQe6NlaE="
    },
    {
        "sessionid": "on8jljxtijcz08nxvtdsafnvpgpca6wu",
        "sessionid_sign": "v3:zdrSEQxqSJdqOcRxWD8xLajJcgXsyh1s9CgDXQsnZtc="
    },
    {
        "sessionid": "5m1xyo8gi0omogyef3roflrteexrt3zv",
        "sessionid_sign": "v3:pfgppWB0cO4xDYcI1Y6y/mY6rhFOklANzNSEtnUQXdI="
    },
    {
        "sessionid": "vxvdynua8osiyf0fwfn2x2yvy5q8n8cs",
        "sessionid_sign": "v3:eI4Mg4fXQGTAJ1SJ9rlxiskSr+ngc3on66Nd0IbmTr4="
    },
    {
        "sessionid": "08sqlp86hd3bduscbjv159tq0ds9adzt",
        "sessionid_sign": "v3:OqBdhTW3DSjVzJRjH0mSHUYdStCyrvZ3MoXJmtmD3I0="
    },
    {
        "sessionid": "qatorul4xe9ah6rzb1vkk5xtuwt7uygq",
        "sessionid_sign": "v3:FOhRQDa0O2Oo6IrnHcpizxSSlw4CgFdOtYs4kf/25wE="
    },
    {
        "sessionid": "c6e83lfw9hop1hld0qsft4634zah8ucp",
        "sessionid_sign": "v3:mE0TcSuxquhCXT7ysQaGtiq+vz2IGLwSj7NyKiWPk5k="
    },
    {
        "sessionid": "rdu9xyuda4c2sawsigzowj8xyynkh1oj",
        "sessionid_sign": "v3:1fhMVrppO2SEAihS7O/BB7UOE5Q+WUBwgwVa1AJP9Tk="
    },
    {
        "sessionid": "6b51nxhkk9lvl54o7lrtnz86yudctaej",
        "sessionid_sign": "v3:ZXq8jO1JJr3/m3IDN/8XizhFNhe+ytm6nBCLouDXagg="
    },
    {
        "sessionid": "xa5a7083pvwu57qrsioayf3mw4ofnevq",
        "sessionid_sign": "v3:gg1koNMDyACH4F4EDuNRQtwYNnNe8Ss6NrFkYObVpM0="
    },
]

# ==============================================================================
# 🌐 GLOBALS & QUEUES
# ==============================================================================
IS_BANNED = False
SUCCESSFUL_CONNECTIONS = 0

# Dual Queues
intraday_queue = queue.Queue()
daily_queue = queue.Queue()

# Shared In-Memory Data Store (protected by data_lock)
current_file_data = {interval: [] for interval in ALL_INTERVALS}
symbol_metadata = {}  # {symbol: {"has_intraday": bool, "type": str, ...}}
data_lock = threading.Lock()

COMPLETED_TASKS = set()
intraday_retry_list = []
daily_retry_list = []

api = HfApi()

base_dir = "/kaggle/working/staging" if os.path.exists("/kaggle/working") else "staging"
log_dir = os.path.join(base_dir, "log", NOTEBOOK_ID)
data_dir = os.path.join(base_dir, "data")

os.makedirs(log_dir, exist_ok=True)
os.makedirs(data_dir, exist_ok=True)

# ==============================================================================
# 🛠️ HELPER FUNCTIONS
# ==============================================================================
def log_failure(symbol, interval, category, detail):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    detail_clean = str(detail).replace('\n', ' ').replace('\r', '').strip()
    msg = f"{timestamp}, {symbol}, {interval}, {category}, {detail_clean}"
    try:
        with open(os.path.join(log_dir, "failures.log"), "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def wait_for_queue(target_queue, label="", total_tasks=0, pbar_list=None):
    global IS_BANNED
    last_log_time = time.time()
    start_time = time.time()
    queues = target_queue if isinstance(target_queue, (list, tuple)) else [target_queue]
    while any(q.unfinished_tasks > 0 for q in queues) and not IS_BANNED:
        time.sleep(0.5)
        now = time.time()
        if label and total_tasks > 0 and (now - last_log_time >= 30.0):
            remaining = sum(q.unfinished_tasks for q in queues)
            done = max(0, total_tasks - remaining)
            pct = (done / total_tasks * 100) if total_tasks > 0 else 0
            elapsed = now - start_time
            rate = done / elapsed if elapsed > 0 else 0
            print(f"⏳ {label}: {done}/{total_tasks} ({pct:.1f}%) | Success: {SUCCESSFUL_CONNECTIONS} | Remaining: {remaining} | Speed: {rate:.1f} t/s", flush=True)
            last_log_time = now
            
    if IS_BANNED:
        for q in queues:
            with q.all_tasks_done:
                q.queue.clear()
                q.unfinished_tasks = 0
                q.all_tasks_done.notify_all()
        return False
        
    if pbar_list and pbar_list[0]:
        try:
            pbar_list[0].n = total_tasks
            pbar_list[0].refresh()
        except Exception:
            pass
    return True

def generate_session_id(prefix):
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{prefix}_{random_str}"

def format_message(func, args):
    msg = json.dumps({"m": func, "p": args}, separators=(',', ':'))
    return f"~m~{len(msg)}~m~{msg}"

def parse_message(message):
    messages = []
    while message:
        try:
            if not message.startswith("~m~"):
                break
            parts = message.split("~m~")
            if len(parts) < 3:
                break
            length = int(parts[1])
            start = len("~m~") + len(parts[1]) + len("~m~")
            payload = message[start:start+length]
            if payload.startswith("~h~"):
                messages.append(payload)
            else:
                messages.append(json.loads(payload))
            message = message[start+length:]
        except Exception:
            break
    return messages

def fetch_jwt_token(sessionid, sessionid_sign):
    cookies = {"sessionid": sessionid}
    if sessionid_sign:
        cookies["sessionid_sign"] = sessionid_sign
    try:
        resp = requests.get(
            "https://www.tradingview.com/",
            cookies=cookies,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15
        )
        match = re.search(r'"auth_token":"([^"]+)"', resp.text)
        jwt = match.group(1) if match else "unauthorized_user_token"
        return jwt
    except Exception:
        return "unauthorized_user_token"

def verify_cookie_tier(sessionid, sessionid_sign, jwt_token, server="data", silent=False):
    if not silent: 
        print(f"⏳ Verifying tier on {server}.tradingview.com via INDEX:BTCUSD (15m)...")
    ws_url = f"wss://{server}.tradingview.com/socket.io/websocket?from=chart&type=chart"
    cookie_str = f"sessionid={sessionid};"
    if sessionid_sign:
        cookie_str += f" sessionid_sign={sessionid_sign}"
        
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Cookie": cookie_str
    }
    
    try:
        ws = websocket.create_connection(ws_url, header=headers, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        ws.recv()
        ws.send(format_message("set_auth_token", [jwt_token]))
        chart_session = generate_session_id("cs")
        ws.send(format_message("chart_create_session", [chart_session, ""]))
        
        symbol_json = json.dumps({"symbol": "INDEX:BTCUSD", "adjustment": "none"})
        ws.send(format_message("resolve_symbol", [chart_session, "ser_1", f"={symbol_json}"]))
        ws.send(format_message("create_series", [chart_session, "$prices", "s1", "ser_1", "15", 2000000]))
        
        all_bars_count = 0
        loop_start = time.time()
        while time.time() - loop_start < 12:
            try:
                ws.settimeout(2.0)
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
                
            for msg in parse_message(raw):
                if isinstance(msg, str) and msg.startswith("~h~"):
                    ws.send(f"~m~{len(msg)}~m~{msg}")
                    continue
                if isinstance(msg, dict):
                    if msg.get("m") == "timescale_update":
                        data = msg.get("p", [{}])[1].get("$prices", {}).get("s", [])
                        all_bars_count += len(data)
                    elif msg.get("m") == "series_completed":
                        ws.send(format_message("chart_delete_session", [chart_session]))
                        ws.close()
                        tier = "BASIC"
                        if all_bars_count >= 35000:
                            tier = "ULTIMATE"
                        elif all_bars_count >= 18000:
                            tier = "PREMIUM"
                            
                        if not silent:
                            print(f"📊 Verified {all_bars_count} bars -> TIER: {tier}")
                        return tier
                        
        ws.send(format_message("chart_delete_session", [chart_session]))
        ws.close()
        return "BASIC"
    except Exception as e:
        if not silent: print(f"⚠️ Tier verification error: {e}")
        return "UNKNOWN"

# ==============================================================================
# 🔍 DYNAMIC LIVE STUDY DESCRIPTOR RESOLUTION
# ==============================================================================
LIVE_STUDY_DESCRIPTORS = {
    "Splits": "Splits@tv-basicstudies-278",
    "Dividends": "Dividends@tv-basicstudies-278",
    "Earnings": "Earnings@tv-basicstudies-278",
    "RollDates": "BarSetContinuousRollDates@tv-corestudies-49",
}

def resolve_live_study_descriptors(server="data", timeout=5):
    """Dynamically queries TradingView WebSocket to discover the latest, up-to-date
    study package versions. Updates LIVE_STUDY_DESCRIPTORS in place so no version
    numbers ever need to be hardcoded or maintained manually."""
    global LIVE_STUDY_DESCRIPTORS
    try:
        ws_url = f"wss://{server}.tradingview.com/socket.io/websocket?from=chart&type=chart"
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        ws = websocket.create_connection(ws_url, header=headers, timeout=timeout)
        
        def send_cmd(m, p):
            msg = json.dumps({"m": m, "p": p}, separators=(',', ':'))
            ws.send(f"~m~{len(msg)}~m~{msg}")

        send_cmd("set_auth_token", ["unauthorized_user_token"])
        send_cmd("request_studies_metadata", ["metadata_1"])
        
        start = time.time()
        while time.time() - start < timeout:
            ws.settimeout(1.5)
            raw = ws.recv()
            for msg in parse_message(raw):
                if isinstance(msg, str) and msg.startswith("~h~"):
                    ws.send(f"~m~{len(msg)}~m~{msg}")
                elif isinstance(msg, dict):
                    m = msg.get("m")
                    p = msg.get("p", [])
                    if m == "studies_metadata" and len(p) >= 2 and isinstance(p[1], dict):
                        metainfo = p[1].get("metainfo", [])
                        for item in metainfo:
                            s_id = item.get("id", "")
                            if s_id.startswith("Dividends@"):
                                LIVE_STUDY_DESCRIPTORS["Dividends"] = s_id
                            elif s_id.startswith("Earnings@") and "PriceSupport" not in s_id:
                                LIVE_STUDY_DESCRIPTORS["Earnings"] = s_id
                            elif s_id.startswith("Splits@"):
                                LIVE_STUDY_DESCRIPTORS["Splits"] = s_id
                            elif s_id.startswith("BarSetContinuousRollDates@"):
                                LIVE_STUDY_DESCRIPTORS["RollDates"] = s_id
                        ws.close()
                        print(f"✅ Dynamically resolved live study versions: {LIVE_STUDY_DESCRIPTORS}")
                        return LIVE_STUDY_DESCRIPTORS
        ws.close()
    except Exception as e:
        print(f"⚠️ Dynamic study resolution notice (using verified defaults): {e}")
    return LIVE_STUDY_DESCRIPTORS

# ==============================================================================
# 📊 EVENT MERGING (1D TIMEFRAME ENRICHMENT)
# ==============================================================================
def merge_events_to_1d(bars_dict, splits_list, splits_study_dict, dividends_dict, earnings_dict, rolls_dict, symbol):
    """Merges raw corporate actions and futures rollover events into nominal unadjusted 1D bars.
    Stores pure raw event payloads without pre-calculated or parsed columns.
    Excludes future unoccurred events to keep the dataset clean without empty NaN price rows."""
    if not bars_dict:
        return None
    bars = sorted(bars_dict.values(), key=lambda x: x["time"])
    df = pd.DataFrame(bars)
    df["symbol"] = symbol

    # 1. Raw Split factor (from Quote Session last_splits or Chart Study)
    utc_dates = pd.to_datetime(df["time"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    splits_by_date = {}
    for item in splits_list:
        if len(item) >= 2:
            d_str = datetime.fromtimestamp(int(item[0]), tz=timezone.utc).strftime("%Y-%m-%d")
            splits_by_date[d_str] = float(item[1])
    df["raw_split"] = utc_dates.map(splits_by_date)

    if splits_study_dict:
        for t, v in splits_study_dict.items():
            # v = [bar_ts, denominator, numerator, split_date_ms]
            if len(v) >= 3 and v[2] != 0:
                study_split = float(v[1]) / float(v[2])  # e.g. 1 / 10 = 0.10
                mask = df["time"] == t
                df.loc[mask & df["raw_split"].isna(), "raw_split"] = study_split

    # 2. Raw Dividend payload: [bar_ts, gross_payout, ex_date_ms, net_adjusted, pay_date_ms]
    df["raw_dividend"] = df["time"].map(lambda t: json.dumps(dividends_dict[t]) if dividends_dict and t in dividends_dict else None)

    # 3. Raw Earnings payload: [bar_ts, std_eps, est_eps, period_s, ann_ms, rep_eps, rev_est_M, rev_act_M, flags]
    df["raw_earnings"] = df["time"].map(lambda t: json.dumps(earnings_dict[t]) if earnings_dict and t in earnings_dict else None)

    # 4. Raw Futures Continuous Rollover payload: [from_code, to_code, switch_yyyymmdd]
    df["raw_roll"] = df["time"].map(lambda t: json.dumps(rolls_dict[t][1:4]) if rolls_dict and t in rolls_dict and len(rolls_dict[t]) >= 4 else None)

    return df

# ==============================================================================
# 🧵 WORKER LOOPS
# ==============================================================================

def ultimate_worker_loop(sessionid, sessionid_sign, jwt_token, server, pbar_list):
    """Worker pool dedicated to intraday intervals (1m, 5m, 15m, 60m, 240m) on Ultimate."""
    global IS_BANNED, SUCCESSFUL_CONNECTIONS
    
    ws_url = f"wss://{server}.tradingview.com/socket.io/websocket?from=chart&type=chart"
    cookie_str = f"sessionid={sessionid};"
    if sessionid_sign: cookie_str += f" sessionid_sign={sessionid_sign}"
    
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Cookie": cookie_str
    }
    
    ws = None
    
    while not IS_BANNED:
        try:
            csv_name, symbol, interval, is_retry = intraday_queue.get(timeout=1.0)
        except queue.Empty:
            if ws and ws.connected:
                try:
                    ws.settimeout(0.1)
                    raw = ws.recv()
                    for msg in parse_message(raw):
                        if isinstance(msg, str) and msg.startswith("~h~"):
                            ws.send(f"~m~{len(msg)}~m~{msg}")
                except Exception:
                    pass
            continue
            
        try:
            time.sleep(random.uniform(0.04, 0.08))
            
            task_success = False
            data_received = False
            last_error_cat = None
            last_error_det = None
            
            for attempt in range(5):
                if IS_BANNED: break
                chart_session = None
                try:
                    if ws is None or not ws.connected:
                        ws = websocket.create_connection(
                            ws_url, header=headers, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=15
                        )
                        ws.settimeout(15)
                        ws.recv()
                        ws.send(format_message("set_auth_token", [jwt_token]))
                        
                    chart_session = generate_session_id("cs_ult")
                    ws.send(format_message("chart_create_session", [chart_session, ""]))
                    
                    # Unadjusted nominal exchange price bars
                    symbol_json = json.dumps({"symbol": symbol, "adjustment": "none"})
                    ws.send(format_message("resolve_symbol", [chart_session, "ser_1", f"={symbol_json}"]))
                    ws.send(format_message("create_series", [chart_session, "$prices", "s1", "ser_1", interval, 2000000]))
                    
                    bars_dict = {}
                    batch_count = 0
                    loop_start = time.time()
                    
                    while not IS_BANNED:
                        if time.time() - loop_start > 90:
                            last_error_cat = "tradingview_timeout"
                            last_error_det = "90s_timeout"
                            break
                            
                        try:
                            ws.settimeout(2.5)
                            raw = ws.recv()
                        except websocket.WebSocketTimeoutException:
                            continue
                            
                        messages = parse_message(raw)
                        new_bars_in_batch = 0
                        series_done = False
                        
                        for msg in messages:
                            if isinstance(msg, str) and msg.startswith("~h~"):
                                ws.send(f"~m~{len(msg)}~m~{msg}")
                                continue
                                
                            if isinstance(msg, dict):
                                payload = msg.get("p", [])
                                if not payload or payload[0] != chart_session:
                                    continue
                                    
                                m_type = msg.get("m")
                                if m_type in ["critical_error", "error"]:
                                    IS_BANNED = True
                                    last_error_cat = "server_network_error"
                                    last_error_det = str(msg)
                                    log_failure(symbol, interval, last_error_cat, last_error_det)
                                    if ws: ws.close()
                                    ws = None
                                    return
                                    
                                if m_type in ["symbol_error", "series_error"]:
                                    data_received = True
                                    task_success = False
                                    last_error_cat = "tradingview_message"
                                    last_error_det = m_type
                                    log_failure(symbol, interval, last_error_cat, last_error_det)
                                    series_done = True
                                    break
                                    
                                if m_type in ["timescale_update", "du"]:
                                    loop_start = time.time()
                                    if len(payload) > 1:
                                        data = payload[1]
                                        series = data.get("$prices", {}).get("s", []) or data.get("s1", {}).get("s", [])
                                        for bar in series:
                                            v = bar.get("v", [])
                                            if len(v) >= 5 and v[0] not in bars_dict:
                                                bars_dict[v[0]] = {
                                                    "time": int(v[0]), "open": v[1], "high": v[2],
                                                    "low": v[3], "close": v[4], "volume": v[5] if len(v) > 5 else 0
                                                }
                                                new_bars_in_batch += 1
                                                
                                elif m_type == "series_completed":
                                    series_done = True
                                    batch_count += 1
                                    
                        if series_done:
                            if FETCH_FULL_HISTORY and new_bars_in_batch > 0 and batch_count < MAX_HISTORY_BATCHES:
                                ws.send(format_message("request_more_data", [chart_session, "$prices", 2000000]))
                                loop_start = time.time()
                                continue
                            else:
                                break
                                
                    if bars_dict:
                        all_bars = sorted(bars_dict.values(), key=lambda x: x["time"])
                        df = pd.DataFrame(all_bars)
                        df["symbol"] = symbol
                        with data_lock:
                            current_file_data[interval].append(df)
                        SUCCESSFUL_CONNECTIONS += 1
                        task_success = True
                        data_received = True
                        break
                    elif series_done and not last_error_det:
                        data_received = True
                        task_success = False
                        log_failure(symbol, interval, "tradingview_message", "series_completed_but_empty")
                        break
                    elif data_received:
                        break
                        
                except Exception as e:
                    last_error_cat = "server_network_error"
                    last_error_det = f"{type(e).__name__}: {e}"
                    if ws: ws.close()
                    ws = None
                finally:
                    if ws and ws.connected and chart_session:
                        try:
                            ws.send(format_message("chart_delete_session", [chart_session]))
                        except Exception:
                            pass
                            
                if not task_success and not data_received and not IS_BANNED:
                    time.sleep(3)
                    
            if not task_success and not IS_BANNED and not data_received:
                with data_lock: 
                    intraday_retry_list.append((csv_name, symbol, interval))
                
        finally:
            if pbar_list[0]:
                pbar_list[0].update(1)
                pbar_list[0].set_postfix(Success=SUCCESSFUL_CONNECTIONS, refresh=False)
            intraday_queue.task_done()
            
    if ws: ws.close()


def daily_worker_loop(sessionid, sessionid_sign, jwt_token, pbar_list):
    """Worker pool dedicated to macro intervals (1D, 1W, 1M) on Free account."""
    global IS_BANNED, SUCCESSFUL_CONNECTIONS
    
    ws_url = "wss://data.tradingview.com/socket.io/websocket?from=chart&type=chart"
    cookie_str = f"sessionid={sessionid};"
    if sessionid_sign: cookie_str += f" sessionid_sign={sessionid_sign}"
    
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Cookie": cookie_str
    }
    
    ws = None
    
    while not IS_BANNED:
        try:
            csv_name, symbol, interval, is_retry = daily_queue.get(timeout=1.0)
        except queue.Empty:
            if ws and ws.connected:
                try:
                    ws.settimeout(0.1)
                    raw = ws.recv()
                    for msg in parse_message(raw):
                        if isinstance(msg, str) and msg.startswith("~h~"):
                            ws.send(f"~m~{len(msg)}~m~{msg}")
                except Exception:
                    pass
            continue
            
        try:
            time.sleep(random.uniform(0.04, 0.08))
            task_success = False
            data_received = False
            last_error_cat = None
            last_error_det = None
            
            for attempt in range(5):
                if IS_BANNED: break
                chart_session = None
                quote_session = None
                try:
                    if ws is None or not ws.connected:
                        ws = websocket.create_connection(
                            ws_url, header=headers, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=15
                        )
                        ws.settimeout(15)
                        ws.recv()
                        ws.send(format_message("set_auth_token", [jwt_token]))
                        
                    chart_session = generate_session_id("cs_free")
                    ws.send(format_message("chart_create_session", [chart_session, ""]))
                    
                    symbol_json = json.dumps({"symbol": symbol, "adjustment": "none"})
                    ws.send(format_message("resolve_symbol", [chart_session, "ser_1", f"={symbol_json}"]))
                    ws.send(format_message("create_series", [chart_session, "$prices", "s1", "ser_1", interval, 2000000]))
                    
                    # For 1D: Attach Quote Session for splits and Chart Studies for events
                    if interval == "1D":
                        quote_session = generate_session_id("qs_1d")
                        ws.send(format_message("quote_create_session", [quote_session]))
                        ws.send(format_message("quote_set_fields", [quote_session, "base_name", "short_name", "last_splits"]))
                        ws.send(format_message("quote_add_symbols", [quote_session, symbol]))
                        
                        ws.send(format_message("create_study", [chart_session, "st_split", "st0", "$prices", LIVE_STUDY_DESCRIPTORS["Splits"], {"currenttime": "now"}]))
                        ws.send(format_message("create_study", [chart_session, "st_div", "st1", "$prices", LIVE_STUDY_DESCRIPTORS["Dividends"], {"currenttime": "now"}]))
                        ws.send(format_message("create_study", [chart_session, "st_earn", "st2", "$prices", LIVE_STUDY_DESCRIPTORS["Earnings"], {"currenttime": "now"}]))
                        ws.send(format_message("create_study", [chart_session, "st_roll", "st3", "$prices", LIVE_STUDY_DESCRIPTORS["RollDates"], {"currenttime": "now"}]))
                        
                    bars_dict = {}
                    splits_list = []
                    splits_study_dict = {}
                    dividends_dict = {}
                    earnings_dict = {}
                    rolls_dict = {}
                    completed_studies = set()
                    expected_studies = {"st_split", "st_div", "st_earn", "st_roll"} if interval == "1D" else set()
                    series_completed_once = False
                    series_done_time = None
                    batch_count = 0
                    loop_start = time.time()
                    
                    while not IS_BANNED:
                        if time.time() - loop_start > 60:
                            last_error_cat = "tradingview_timeout"
                            last_error_det = "60s_timeout"
                            break
                            
                        try:
                            ws.settimeout(2.0)
                            raw = ws.recv()
                        except websocket.WebSocketTimeoutException:
                            if series_completed_once:
                                break
                            continue
                            
                        messages = parse_message(raw)
                        new_bars_in_batch = 0
                        series_done = False
                        
                        for msg in messages:
                            if isinstance(msg, str) and msg.startswith("~h~"):
                                ws.send(f"~m~{len(msg)}~m~{msg}")
                                continue
                                
                            if isinstance(msg, dict):
                                m_type = msg.get("m")
                                payload = msg.get("p", [])
                                
                                if m_type == "symbol_resolved":
                                    if len(payload) > 2 and payload[0] == chart_session:
                                        info = payload[2]
                                        has_intra = info.get("has_intraday", False)
                                        with data_lock:
                                            symbol_metadata[symbol] = {
                                                "has_intraday": has_intra,
                                                "type": info.get("type", ""),
                                                "typespecs": info.get("typespecs", [])
                                            }
                                            
                                if m_type == "qsd" and quote_session:
                                    if len(payload) > 1 and payload[0] == quote_session:
                                        val = payload[1].get("v", {})
                                        splits_list = val.get("last_splits", [])
                                        
                                if m_type in ["critical_error", "error"]:
                                    IS_BANNED = True
                                    last_error_cat = "server_network_error"
                                    last_error_det = str(msg)
                                    log_failure(symbol, interval, last_error_cat, last_error_det)
                                    if ws: ws.close()
                                    ws = None
                                    return
                                    
                                if m_type in ["symbol_error", "series_error"]:
                                    if payload and payload[0] == chart_session:
                                        data_received = True
                                        task_success = False
                                        last_error_cat = "tradingview_message"
                                        last_error_det = m_type
                                        log_failure(symbol, interval, last_error_cat, last_error_det)
                                        # Record no intraday if symbol error
                                        with data_lock:
                                            symbol_metadata[symbol] = {"has_intraday": False, "error": m_type}
                                        series_done = True
                                        break
                                        
                                if m_type in ["timescale_update", "du"]:
                                    if payload and payload[0] == chart_session:
                                        loop_start = time.time()
                                        if len(payload) > 1:
                                            data = payload[1]
                                            series = data.get("$prices", {}).get("s", []) or data.get("s1", {}).get("s", [])
                                            for bar in series:
                                                v = bar.get("v", [])
                                                if len(v) >= 5 and int(v[0]) not in bars_dict:
                                                    bars_dict[int(v[0])] = {
                                                        "time": int(v[0]), "open": v[1], "high": v[2],
                                                        "low": v[3], "close": v[4], "volume": v[5] if len(v) > 5 else 0
                                                    }
                                                    new_bars_in_batch += 1
                                                    
                                            if interval == "1D":
                                                if "st_split" in data:
                                                    for item in data["st_split"].get("st", []):
                                                        v = item.get("v", [])
                                                        if len(v) >= 3: splits_study_dict[int(v[0])] = v
                                                if "st_div" in data:
                                                    for item in data["st_div"].get("st", []):
                                                        v = item.get("v", [])
                                                        if len(v) >= 2: dividends_dict[int(v[0])] = v
                                                if "st_earn" in data:
                                                    for item in data["st_earn"].get("st", []):
                                                        v = item.get("v", [])
                                                        if len(v) >= 2: earnings_dict[int(v[0])] = v
                                                if "st_roll" in data:
                                                    for item in data["st_roll"].get("st", []):
                                                        v = item.get("v", [])
                                                        if len(v) >= 2: rolls_dict[int(v[0])] = v
                                                        
                                elif m_type in ["study_completed", "study_error"]:
                                    if payload and payload[0] == chart_session and len(payload) > 1:
                                        completed_studies.add(payload[1])
                                        
                                elif m_type == "series_completed":
                                    if payload and payload[0] == chart_session:
                                        series_done = True
                                        series_completed_once = True
                                        if series_done_time is None:
                                            series_done_time = time.time()
                                        batch_count += 1
                                        
                        if series_done:
                            if FETCH_FULL_HISTORY and new_bars_in_batch > 0 and batch_count < MAX_HISTORY_BATCHES:
                                ws.send(format_message("request_more_data", [chart_session, "$prices", 2000000]))
                                loop_start = time.time()
                                series_done = False
                                continue
                            else:
                                if interval == "1D":
                                    if expected_studies.issubset(completed_studies) or (series_done_time and time.time() - series_done_time > 1.5):
                                        break
                                else:
                                    break
                        elif series_completed_once and interval == "1D":
                            if expected_studies.issubset(completed_studies) or (series_done_time and time.time() - series_done_time > 1.5):
                                break
                            
                    if bars_dict:
                        if interval == "1D":
                            df = merge_events_to_1d(bars_dict, splits_list, splits_study_dict, dividends_dict, earnings_dict, rolls_dict, symbol)
                        else:
                            all_bars = sorted(bars_dict.values(), key=lambda x: x["time"])
                            df = pd.DataFrame(all_bars)
                            df["symbol"] = symbol
                            
                        with data_lock:
                            current_file_data[interval].append(df)

                        SUCCESSFUL_CONNECTIONS += 1
                        task_success = True
                        data_received = True
                        break
                    elif series_done and not last_error_det:
                        data_received = True
                        task_success = False
                        log_failure(symbol, interval, "tradingview_message", "series_completed_but_empty")
                        with data_lock:
                            if symbol not in symbol_metadata:
                                symbol_metadata[symbol] = {"has_intraday": False}
                        break
                    elif data_received:
                        break
                        
                except Exception as e:
                    last_error_cat = "server_network_error"
                    last_error_det = f"{type(e).__name__}: {e}"
                    if ws: ws.close()
                    ws = None
                finally:
                    if ws and ws.connected:
                        if chart_session:
                            try: ws.send(format_message("chart_delete_session", [chart_session]))
                            except Exception: pass
                        if quote_session:
                            try: ws.send(format_message("quote_delete_session", [quote_session]))
                            except Exception: pass
                            
                if not task_success and not data_received and not IS_BANNED:
                    time.sleep(3)
                    
            if not task_success and not IS_BANNED and not data_received:
                with data_lock: 
                    daily_retry_list.append((csv_name, symbol, interval))
                
        finally:
            if pbar_list[0]:
                pbar_list[0].update(1)
                pbar_list[0].set_postfix(Success=SUCCESSFUL_CONNECTIONS, refresh=False)
            daily_queue.task_done()
            
    if ws: ws.close()

# ==============================================================================
# 📂 GIT SYMBOLS REPOSITORY & MAP.JSON LOOKUP
# ==============================================================================
def ensure_symbols_repo():
    """Ensures the symbols repository is pulled from GitHub."""
    possible_dirs = [
        os.path.join(base_dir, "tradingview_aio_symbols"),
        os.path.join("data", "tradingview_aio_symbols"),
        "/kaggle/working/tradingview_aio_symbols",
        "tradingview_aio_symbols"
    ]
    for d in possible_dirs:
        if os.path.exists(os.path.join(d, "map.json")):
            print(f"✅ Found existing symbols repository at {d}")
            return os.path.abspath(d)
            
    # Clone if not present
    target_dir = os.path.join(base_dir, "tradingview_aio_symbols") if os.path.exists(base_dir) else "tradingview_aio_symbols"
    print(f"⏳ Pulling symbols repository from {SYMBOLS_GIT_REPO} into {target_dir}...")
    try:
        subprocess.run(["git", "clone", "--depth", "1", SYMBOLS_GIT_REPO, target_dir], check=True)
        print("✅ Symbols repository cloned successfully.")
        return os.path.abspath(target_dir)
    except Exception as e:
        print(f"⚠️ Git clone error: {e}. Searching fallback paths...")
        for root, dirs, files in os.walk("."):
            if "map.json" in files:
                print(f"✅ Found map.json in fallback path: {root}")
                return os.path.abspath(root)
        raise FileNotFoundError("Could not clone or locate tradingview_aio_symbols repo containing map.json")

def get_assigned_csv_files(repo_path, notebook_index):
    """Loads map.json and returns only the CSV files assigned to this notebook index."""
    map_file = os.path.join(repo_path, "map.json")
    if not os.path.exists(map_file):
        raise FileNotFoundError(f"map.json not found in {repo_path}")
        
    with open(map_file, "r", encoding="utf-8") as f:
        node_mappings = json.load(f)
        
    assigned_entry = None
    for entry in node_mappings:
        if entry.get("node_index") == notebook_index:
            assigned_entry = entry
            break
            
    if not assigned_entry:
        raise ValueError(f"No entry found in map.json for node_index={notebook_index}")
        
    assigned_files = []
    for fname in assigned_entry.get("files", []):
        fpath = os.path.join(repo_path, fname)
        if not os.path.exists(fpath):
            alt_path = os.path.join(repo_path, "data", fname)
            if os.path.exists(alt_path):
                fpath = alt_path
        if os.path.exists(fpath):
            assigned_files.append(fpath)
        else:
            print(f"⚠️ Warning: Assigned file {fname} not found in {repo_path}")
            
    print(f"✅ Node {notebook_index} assigned {len(assigned_files)} files ({assigned_entry.get('total_symbols', 0)} symbols) from map.json:")
    for f in assigned_files:
        print(f"  • {os.path.basename(f)}")
        
    return assigned_files

def get_symbols_from_file(file):
    symbols = []
    try:
        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header: return symbols
            try:
                ex_idx = header.index("exchange_code")
                sym_idx = header.index("symbol")
                contracts_idx = header.index("contracts") if "contracts" in header else -1
            except ValueError:
                return symbols
            
            for row in reader:
                if len(row) > max(ex_idx, sym_idx):
                    base_symbol = row[sym_idx].strip()
                    extracted_symbols = []
                    
                    if contracts_idx != -1 and len(row) > contracts_idx:
                        contracts_str = row[contracts_idx].strip()
                        if contracts_str.startswith("["):
                            try:
                                contracts_list = ast.literal_eval(contracts_str)
                                if isinstance(contracts_list, list) and len(contracts_list) > 0:
                                    for c in contracts_list:
                                        if c.get("symbol"):
                                            prefix = c.get("prefix", row[ex_idx].strip())
                                            extracted_symbols.append(f"{prefix}:{c['symbol']}")
                            except Exception:
                                pass
                                
                    if not extracted_symbols:
                        extracted_symbols.append(f"{row[ex_idx].strip()}:{base_symbol}")
                        
                    for sym in extracted_symbols:
                        symbols.append(sym)
    except Exception as e:
        print(f"❌ ERROR reading {file}: {e}")
    return list(dict.fromkeys(symbols))

def save_and_sync_interval(interval, csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs):
    """Flushes dataframe list for an interval to Parquet and schedules HF sync."""
    with data_lock:
        df_list = current_file_data[interval]
        if not df_list: return
        combined_df = pd.concat(df_list, ignore_index=True)
        current_file_data[interval] = []
        
    chunk_filename = f"{csv_filename}_{interval}_chunk_{chunk_idx + 1}.parquet"
    chunk_dir = os.path.join(data_dir, csv_filename, interval)
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, chunk_filename)
    
    if ENCRYPTION_KEY:
        temp_buffer = io.BytesIO()
        combined_df.to_parquet(temp_buffer, index=False)
        fernet = Fernet(ENCRYPTION_KEY)
        encrypted_data = fernet.encrypt(temp_buffer.getvalue())
        chunk_path += ".enc"
        with open(chunk_path, "wb") as f:
            f.write(encrypted_data)
        del temp_buffer
        del encrypted_data
    else:
        combined_df.to_parquet(chunk_path, index=False)
        
    symbol_counts = combined_df["symbol"].value_counts().to_dict()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    for s, c in symbol_counts.items():
        uncommitted_checkpoint_entries.append(
            f"{timestamp}, {s}, {interval}, checkpoint, Bars: {c}, data/{csv_filename}/{interval}/{os.path.basename(chunk_path)}"
        )
    uncommitted_csv_dirs.add(os.path.join(data_dir, csv_filename))
    del combined_df
    gc.collect()

# ==============================================================================
# 🚀 MAIN DUAL-ENGINE CONTROLLER
# ==============================================================================
def main():
    print("==================================================================")
    print(f"🚀 TRADINGVIEW DUAL-ENGINE (NODE {NOTEBOOK_INDEX + 1}/{TOTAL_NOTEBOOKS})")
    print("==================================================================\n")
    
    # 1. Authenticate Hugging Face (via os.environ or Kaggle Secrets)
    hf_token = os.environ.get("HF_TOKEN", HF_TOKEN)
    if not hf_token:
        try:
            from kaggle_secrets import UserSecretsClient
            hf_token = UserSecretsClient().get_secret("HF_TOKEN")
        except Exception:
            pass
            
    if hf_token:
        try:
            from huggingface_hub import login
            login(token=hf_token)
            print("✅ Successfully authenticated Hugging Face.")
        except Exception as e:
            print(f"⚠️ Hugging Face login warning: {e}")

    ENCRYPTION_KEY = None
    if ENABLE_ENCRYPTION:
        print("🔒 End-to-End Encryption: ENABLED")
        if Fernet is None:
            print("❌ ERROR: cryptography library is not installed.")
            sys.exit(1)
            
        password = os.environ.get("DATA_PASSWORD", DATA_PASSWORD)
        if not password:
            try:
                from kaggle_secrets import UserSecretsClient
                password = UserSecretsClient().get_secret("DATA_PASSWORD")
            except Exception:
                pass
                
        if not password:
            print("❌ Failed to load DATA_PASSWORD from os.environ (or Kaggle Secrets).")
            sys.exit(1)
            
        try:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'tradingview',
                iterations=480000,
            )
            ENCRYPTION_KEY = base64.urlsafe_b64encode(kdf.derive(password.encode()))
            print("✅ Successfully generated secure AES encryption key.")
        except Exception as e:
            print(f"❌ Key derivation failed: {e}")
            sys.exit(1)
        
    # Check for Ultimate credentials in os.environ (or Kaggle Secrets)
    u_sess = os.environ.get("TV_ULTIMATE_SESSIONID")
    u_sign = os.environ.get("TV_ULTIMATE_SIGN")
    if not u_sess:
        try:
            from kaggle_secrets import UserSecretsClient
            user_secrets = UserSecretsClient()
            u_sess = user_secrets.get_secret("TV_ULTIMATE_SESSIONID")
            u_sign = user_secrets.get_secret("TV_ULTIMATE_SIGN")
        except Exception:
            pass
            
    if u_sess:
        ULTIMATE_COOKIE["sessionid"] = u_sess
        ULTIMATE_COOKIE["sessionid_sign"] = u_sign or ""

    # 2. Assign Cookies for Both Pools
    free_cookie_idx = NOTEBOOK_INDEX % len(FREE_COOKIES)
    assigned_free_cookie = FREE_COOKIES[free_cookie_idx]
    free_sessionid = assigned_free_cookie["sessionid"]
    free_sign = assigned_free_cookie.get("sessionid_sign", "")
    free_jwt = fetch_jwt_token(free_sessionid, free_sign)
    
    print(f"🆓 FREE POOL: Assigned cookie #{free_cookie_idx + 1}/{len(FREE_COOKIES)} to Node {NOTEBOOK_INDEX}.")
    
    # Dynamically resolve live corporate action and study descriptors from TradingView server
    resolve_live_study_descriptors(server="data")
    
    # Ultimate Cookie check:
    has_ultimate = bool(ULTIMATE_COOKIE.get("sessionid"))
    ultimate_server = "prodata"
    
    if has_ultimate:
        ult_sessionid = ULTIMATE_COOKIE["sessionid"]
        ult_sign = ULTIMATE_COOKIE.get("sessionid_sign", "")
        ult_jwt = fetch_jwt_token(ult_sessionid, ult_sign)
        print("⏳ Verifying ULTIMATE account tier...")
        ult_tier = verify_cookie_tier(ult_sessionid, ult_sign, ult_jwt, server="prodata")
        if ult_tier not in ["ULTIMATE", "PREMIUM"]:
            ult_tier = verify_cookie_tier(ult_sessionid, ult_sign, ult_jwt, server="data")
            ultimate_server = "data"
        print(f"💎 ULTIMATE POOL: Tier Verified = {ult_tier} on {ultimate_server}.tradingview.com")
    else:
        print("⚠️ No ULTIMATE_COOKIE configured! Falling back to using Free cookie for intraday pool.")
        ult_sessionid = free_sessionid
        ult_sign = free_sign
        ult_jwt = free_jwt
        ultimate_server = "data"
        
    print(f"\n📊 WORKER ALLOCATION FOR THIS NODE:")
    print(f"  • {ULTIMATE_WORKERS} Intraday Workers -> [1, 5, 15, 60, 240] on {ultimate_server}.tradingview.com (History to Day 1: {FETCH_FULL_HISTORY})")
    print(f"  • {FREE_WORKERS} Daily Workers    -> [1D, 1W, 1M] + Corporate Actions on data.tradingview.com")
    print(f"  • Total Active Connections: {ULTIMATE_WORKERS + FREE_WORKERS}\n")

    # 3. Pull Checkpoints from Hugging Face
    print("⏳ Synchronizing checkpoint logs from Hugging Face...")
    try:
        from huggingface_hub import hf_hub_download
        for log_file in ["checkpoint.log", "failures.log"]:
            try:
                downloaded_path = hf_hub_download(
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    filename=f"log/{NOTEBOOK_ID}/{log_file}"
                )
                shutil.copy(downloaded_path, os.path.join(log_dir, log_file))
            except Exception:
                pass
        print("✅ Logs synchronized.")
    except Exception:
        pass
        
    if os.path.exists(log_dir):
        for log_file in ["checkpoint.log", "failures.log"]:
            log_path = os.path.join(log_dir, log_file)
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = [p.strip() for p in line.split(", ")]
                        if len(parts) >= 3:
                            COMPLETED_TASKS.add(f"{parts[1]} ({parts[2]})")
                            
    print(f"✅ Loaded {len(COMPLETED_TASKS)} completed tasks from existing logs.")

    # 4. Pull/Ensure Symbols Repo & Lookup Assigned Files via map.json
    repo_path = ensure_symbols_repo()
    csv_files = get_assigned_csv_files(repo_path, NOTEBOOK_INDEX)
    if not csv_files:
        print(f"❌ No CSV files assigned to Node {NOTEBOOK_INDEX}!")
        sys.exit(0)

    pbar_list = [None]
    
    # 5. Launch Worker Threads
    print(f"🚀 Launching {ULTIMATE_WORKERS} Intraday workers and {FREE_WORKERS} Daily workers...")
    for _ in range(ULTIMATE_WORKERS):
        threading.Thread(
            target=ultimate_worker_loop,
            args=(ult_sessionid, ult_sign, ult_jwt, ultimate_server, pbar_list),
            daemon=True
        ).start()
        time.sleep(0.15)
        
    for _ in range(FREE_WORKERS):
        threading.Thread(
            target=daily_worker_loop,
            args=(free_sessionid, free_sign, free_jwt, pbar_list),
            daemon=True
        ).start()
        time.sleep(0.15)

    uncommitted_checkpoint_entries = []
    uncommitted_csv_dirs = set()

    # 6. Process Workload Chunks with Two-Phase Execution
    for file in csv_files:
        if IS_BANNED: break
        csv_filename = os.path.splitext(os.path.basename(file))[0]
        symbols = get_symbols_from_file(file)
        chunk_size = 2000
        symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        
        for chunk_idx, symbol_chunk in enumerate(symbol_chunks):
            if IS_BANNED: break
            
            print(f"\n=======================================================")
            print(f"📦 {csv_filename}.csv [Chunk {chunk_idx+1}/{len(symbol_chunks)}] ({len(symbol_chunk)} symbols)")
            print(f"=======================================================")
            
            # ------------------------------------------------------------------
            # 🔍 PHASE 1: Scan 1D Timeframe & Ingest Corporate Actions + Rollovers
            # ------------------------------------------------------------------
            phase1_tasks = 0
            for sym in symbol_chunk:
                task_str = f"{sym} (1D)"
                if task_str not in COMPLETED_TASKS:
                    COMPLETED_TASKS.add(task_str)
                    daily_queue.put((csv_filename, sym, "1D", False))
                    phase1_tasks += 1
                    
            if phase1_tasks > 0:
                print(f"⏳ [Phase 1: 1D Scan] Queued {phase1_tasks} tasks for unadjusted OHLCV + splits, dividends, earnings, rolls...")
                pbar_list[0] = tqdm(
                    total=phase1_tasks,
                    desc=f"1D Scan {csv_filename} [{chunk_idx+1}/{len(symbol_chunks)}]",
                    mininterval=15.0,
                    file=sys.stdout
                )
                phase1_finished = wait_for_queue(daily_queue, f"1D Scan {csv_filename}", phase1_tasks, pbar_list)
                if pbar_list[0]: pbar_list[0].close()
                if not phase1_finished or IS_BANNED:
                    print("🛑 Halting due to rate-limit/ban trigger.")
                    break
                    
                # Retry transient Phase 1 network errors
                retry_round = 1
                while daily_retry_list and not IS_BANNED and retry_round <= MAX_NETWORK_RETRIES:
                    with data_lock:
                        retries = list(daily_retry_list)
                        daily_retry_list.clear()
                    if not retries: break
                    print(f"⚠️ [Phase 1 Retry Round {retry_round}] Retrying {len(retries)} 1D tasks...")
                    pbar_list[0] = tqdm(total=len(retries), desc=f"1D Retries R{retry_round}", mininterval=10.0, file=sys.stdout)
                    for r in retries: daily_queue.put((r[0], r[1], r[2], True))
                    wait_for_queue(daily_queue, f"1D Retries R{retry_round}", len(retries), pbar_list)
                    if pbar_list[0]: pbar_list[0].close()
                    retry_round += 1
                    
                # Save 1D Parquet chunk
                save_and_sync_interval("1D", csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs)
            else:
                print("ℹ️ 1D tasks already completed for this chunk.")

            # ------------------------------------------------------------------
            # 🎯 PHASE 2: Filter by has_intraday & Query Intraday + Remaining Macro
            # ------------------------------------------------------------------
            intraday_eligible = []
            intraday_skipped = []
            
            with data_lock:
                for sym in symbol_chunk:
                    meta = symbol_metadata.get(sym, {})
                    if meta.get("has_intraday") is True:
                        intraday_eligible.append(sym)
                    else:
                        intraday_skipped.append(sym)
                        
            print(f"🎯 1D Filter Results: {len(intraday_eligible)} symbols HAS intraday | {len(intraday_skipped)} symbols FILTERED OUT (has_intraday is False).")
            
            # Log skipped symbols to checkpoint log
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            for sym in intraday_skipped:
                for intra_int in INTRADAY_INTERVALS:
                    COMPLETED_TASKS.add(f"{sym} ({intra_int})")
                uncommitted_checkpoint_entries.append(
                    f"{timestamp}, {sym}, INTRADAY, skipped, has_intraday is False"
                )

            # Queue remaining macro (1W, 1M) and eligible intraday intervals
            phase2_daily_tasks = 0
            phase2_intra_tasks = 0
            
            # Remaining Macro
            for sym in symbol_chunk:
                for interval in ["1W", "1M"]:
                    task_str = f"{sym} ({interval})"
                    if task_str not in COMPLETED_TASKS:
                        COMPLETED_TASKS.add(task_str)
                        daily_queue.put((csv_filename, sym, interval, False))
                        phase2_daily_tasks += 1
                        
            # Eligible Intraday
            for sym in intraday_eligible:
                for interval in INTRADAY_INTERVALS:
                    task_str = f"{sym} ({interval})"
                    if task_str not in COMPLETED_TASKS:
                        COMPLETED_TASKS.add(task_str)
                        intraday_queue.put((csv_filename, sym, interval, False))
                        phase2_intra_tasks += 1

            total_phase2 = phase2_daily_tasks + phase2_intra_tasks
            if total_phase2 > 0:
                print(f"🚀 [Phase 2] Queued {phase2_daily_tasks} Macro tasks (Free pool) + {phase2_intra_tasks} Intraday tasks (Ultimate pool)...")
                pbar_list[0] = tqdm(
                    total=total_phase2,
                    desc=f"Phase 2 {csv_filename} [{chunk_idx+1}/{len(symbol_chunks)}]",
                    mininterval=20.0,
                    file=sys.stdout
                )
                
                # Wait for both queues
                phase2_finished = wait_for_queue([intraday_queue, daily_queue], f"Phase 2 {csv_filename}", total_phase2, pbar_list)
                if pbar_list[0]: pbar_list[0].close()
                if not phase2_finished or IS_BANNED: break
                
                # End-of-Chunk Network Retry Loop
                retry_round = 1
                while (intraday_retry_list or daily_retry_list) and not IS_BANNED and retry_round <= MAX_NETWORK_RETRIES:
                    with data_lock:
                        current_intra_retries = list(intraday_retry_list)
                        current_daily_retries = list(daily_retry_list)
                        intraday_retry_list.clear()
                        daily_retry_list.clear()
                        
                    total_retries = len(current_intra_retries) + len(current_daily_retries)
                    if total_retries == 0: break
                    
                    cooldown = min(60, 15 * retry_round)
                    print(f"\n⚠️ [Phase 2 Network Retry Round {retry_round}/{MAX_NETWORK_RETRIES}] {total_retries} tasks pending TradingView confirmation. Pausing for {cooldown}s...")
                    time.sleep(cooldown)
                    
                    pbar_list[0] = tqdm(total=total_retries, desc=f"Phase 2 Retries R{retry_round}", mininterval=15.0, file=sys.stdout)
                    for r in current_intra_retries: intraday_queue.put((r[0], r[1], r[2], True))
                    for r in current_daily_retries: daily_queue.put((r[0], r[1], r[2], True))
                    
                    wait_for_queue([intraday_queue, daily_queue], f"Phase 2 Retries R{retry_round}", total_retries, pbar_list)
                    if pbar_list[0]: pbar_list[0].close()
                    retry_round += 1
                    
                # Save Parquet for each interval
                for interval in ALL_INTERVALS:
                    if interval != "1D":  # 1D was already saved in Phase 1
                        save_and_sync_interval(interval, csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs)

            # ------------------------------------------------------------------
            # ☁️ BATCH SYNC TO HUGGING FACE & LOCAL CLEANUP
            # ------------------------------------------------------------------
            has_data = any(os.path.exists(d) for d in uncommitted_csv_dirs)
            if has_data:
                print(f"🚀 Pushing Parquet data ({len(uncommitted_csv_dirs)} folders) to Hugging Face...")
                for attempt in range(5):
                    try:
                        api.upload_folder(
                            folder_path=data_dir,
                            path_in_repo="data",
                            repo_id=HF_REPO_ID,
                            repo_type="dataset",
                            commit_message=f"Sync {csv_filename} chunk {chunk_idx+1} data"
                        )
                        break
                    except Exception as e:
                        print(f"⚠️ Push data attempt {attempt+1}/5 failed: {e}")
                        time.sleep(3)
                else:
                    raise RuntimeError(f"Failed to push data for {csv_filename} chunk {chunk_idx+1} after 5 attempts.")
                        
                if uncommitted_checkpoint_entries:
                    with open(os.path.join(log_dir, "checkpoint.log"), "a", encoding="utf-8") as f:
                        for entry in uncommitted_checkpoint_entries:
                            f.write(entry + "\n")
                    uncommitted_checkpoint_entries.clear()
                    
                for attempt in range(5):
                    try:
                        api.upload_folder(
                            folder_path=log_dir,
                            path_in_repo=f"log/{NOTEBOOK_ID}",
                            repo_id=HF_REPO_ID,
                            repo_type="dataset",
                            commit_message=f"Sync {csv_filename} chunk {chunk_idx+1} logs"
                        )
                        print(f"✅ Successfully synced {csv_filename} chunk {chunk_idx+1} to Hugging Face.")
                        break
                    except Exception:
                        time.sleep(2)
                else:
                    raise RuntimeError(f"Failed to push logs for {csv_filename} chunk {chunk_idx+1} after 5 attempts.")
                        
                # Clean local files to keep Kaggle disk at 0% usage
                for d in list(uncommitted_csv_dirs):
                    shutil.rmtree(d, ignore_errors=True)
                uncommitted_csv_dirs.clear()

    # Final flush
    if uncommitted_checkpoint_entries and not IS_BANNED:
        print("\n⏳ Attempting final push for uncommitted data before finishing...")
        for attempt in range(5):
            try:
                api.upload_folder(
                    folder_path=data_dir,
                    path_in_repo="data",
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    commit_message="Final sync remaining data"
                )
                with open(os.path.join(log_dir, "checkpoint.log"), "a", encoding="utf-8") as f:
                    for entry in uncommitted_checkpoint_entries:
                        f.write(entry + "\n")
                uncommitted_checkpoint_entries.clear()
                
                api.upload_folder(
                    folder_path=log_dir,
                    path_in_repo=f"log/{NOTEBOOK_ID}",
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    commit_message="Final sync remaining logs"
                )
                for d in list(uncommitted_csv_dirs):
                    shutil.rmtree(d, ignore_errors=True)
                uncommitted_csv_dirs.clear()
                print("✅ Final sync completed successfully.")
                break
            except Exception as e:
                print(f"⚠️ Final push attempt {attempt+1}/5 failed: {e}")
                time.sleep(3)
        else:
            raise RuntimeError("Failed to final push data to Hugging Face after 5 attempts.")

    print("\n==================================================================")
    status_str = "ABORTED (Rate Limit / Ban Triggered)" if IS_BANNED else "COMPLETED (All Assigned Chunks Done)"
    print(f"🏁 RUN STATUS: {status_str}")
    print(f"📊 Total Successful Downloads: {SUCCESSFUL_CONNECTIONS}")
    print("==================================================================")

if __name__ == "__main__":
    main()
