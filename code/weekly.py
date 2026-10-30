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
import signal
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

from huggingface_hub import HfApi, hf_hub_download
try:
    from huggingface_hub.utils import EntryNotFoundError
except ImportError:
    EntryNotFoundError = None

# ==============================================================================
# ⚙️ SETTINGS & CONFIGURATION
# ==============================================================================
TOTAL_NOTEBOOKS = int(os.environ.get("TOTAL_NOTEBOOKS", 20))
NOTEBOOK_INDEX = int(os.environ.get("NOTEBOOK_INDEX", 0))  # 0 to (TOTAL_NOTEBOOKS - 1)
NOTEBOOK_ID = f"NODE_{NOTEBOOK_INDEX}"

# Weekly Directory & Formula: m = week number + 2-digit year (e.g. 30 + 2026 = 3026)
now_utc = datetime.now(timezone.utc)
iso_year, iso_week, _ = now_utc.isocalendar()
DEFAULT_WEEK_M = f"{iso_week}{iso_year % 100:02d}"
WEEK_M = os.environ.get("WEEK_M", os.environ.get("WEEK_ID", DEFAULT_WEEK_M))
WEEK_TAG = f"WEEK_{WEEK_M}"

# Free Worker Sizing:
# 8 Free workers per node x 1 node = 8 connections on the node's dedicated free cookie.
FREE_WORKERS = int(os.environ.get("FREE_WORKERS", os.environ.get("NUM_WORKERS", 8)))
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
    return ""

HF_TOKEN = _resolve_hf_token()
DATA_PASSWORD = os.environ.get("DATA_PASSWORD", "")
HF_REPO_ID = os.environ.get("HF_REPO_ID", "thonguyen511/temp")
ENABLE_ENCRYPTION = True  # Mandatory: Output is ALWAYS AES-encrypted to protect market data

# Interval Assignment (100% Free Pool):
INTRADAY_INTERVALS = ["1", "5", "15", "60", "240"]
DAILY_INTERVALS = ["1D", "1W", "1M"]
ALL_INTERVALS = INTRADAY_INTERVALS + DAILY_INTERVALS

# Symbol Repo URL
SYMBOLS_GIT_REPO = "https://github.com/thonguyen511/tdv_aio.git"

# ==============================================================================
# 🔑 TRADINGVIEW AUTO-LOGIN & JWT MANAGEMENT
# ==============================================================================
try:
    from .tv_auth import TVTokenManager
except ImportError:
    from tv_auth import TVTokenManager

FREE_EMAILS = []
if os.environ.get("TV_EMAILS_JSON"):
    try:
        FREE_EMAILS = json.loads(os.environ["TV_EMAILS_JSON"])
    except Exception:
        FREE_EMAILS = []

# ==============================================================================
# 🌐 GLOBALS & QUEUES
# ==============================================================================
IS_BANNED = False
SUCCESSFUL_CONNECTIONS = 0

task_queue = queue.Queue()

# Shared In-Memory Data Store (protected by data_lock)
current_file_data = {interval: [] for interval in ALL_INTERVALS}
symbol_metadata = {}  # {symbol: {"has_intraday": bool, "type": str, ...}}
data_lock = threading.Lock()

COMPLETED_TASKS = set()
retry_list = []

api = HfApi()

base_dir = "/kaggle/working/staging" if os.path.exists("/kaggle/working") else "staging"
log_dir = os.path.join(base_dir, "log", NOTEBOOK_ID, WEEK_TAG)
data_dir = os.path.join(base_dir, "data")
log_repo_dir = f"log/{NOTEBOOK_ID}/{WEEK_TAG}"

uncommitted_checkpoint_entries = []
uncommitted_csv_dirs = set()

# Ensure Hugging Face never uses default ~/.cache; route all caching into Kaggle working directory
os.environ["HF_HOME"] = os.path.join(base_dir, "hf_home")
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

os.makedirs(log_dir, exist_ok=True)
os.makedirs(data_dir, exist_ok=True)

def sigterm_handler(signum, frame):
    global IS_BANNED
    print(f"\n🛑 [Signal Received: {signum}] Stopping workers cleanly without pushing incomplete chunk...")
    IS_BANNED = True
    sys.exit(0)

try:
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)
except Exception:
    pass

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
# 🧵 WORKER LOOP
# ==============================================================================

def worker_loop(token_mgr, pbar_list):
    """Worker pool running 100% on dedicated Free account authenticated via JWT (with 3h auto-refresh)."""
    global IS_BANNED, SUCCESSFUL_CONNECTIONS
    
    ws_url = "wss://data.tradingview.com/socket.io/websocket?from=chart&type=chart"
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    ws = None
    ws_auth_time = 0.0
    
    while not IS_BANNED:
        try:
            csv_name, symbol, interval, is_retry = task_queue.get(timeout=1.0)
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
                if IS_BANNED:
                    break
                chart_session = None
                quote_session = None
                try:
                    if ws is not None and (time.time() - ws_auth_time >= 3 * 3600):
                        try:
                            ws.close()
                        except Exception:
                            pass
                        ws = None

                    if ws is None or not ws.connected:
                        jwt_token = token_mgr.get_jwt()
                        ws = websocket.create_connection(
                            ws_url, header=headers, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=15
                        )
                        ws.settimeout(15)
                        ws.recv()
                        ws.send(format_message("set_auth_token", [jwt_token]))
                        ws_auth_time = time.time()
                        
                    chart_session = generate_session_id("cs")
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
                    loop_start = time.time()
                    
                    while not IS_BANNED:
                        if time.time() - loop_start > 30:
                            last_error_cat = "tradingview_timeout"
                            last_error_det = "30s_timeout"
                            break
                            
                        try:
                            ws.settimeout(2.0)
                            raw = ws.recv()
                        except websocket.WebSocketTimeoutException:
                            if series_completed_once:
                                break
                            continue
                            
                        messages = parse_message(raw)
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
                                        
                        # Single-batch completion check (no pagination / request_more_data loop)
                        if series_done:
                            if last_error_det:
                                break
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
                        if interval == "1D":
                            with data_lock:
                                symbol_metadata[symbol] = {"has_intraday": False, "error": "series_completed_but_empty"}
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
                    retry_list.append((csv_name, symbol, interval))
                
        finally:
            if pbar_list[0]:
                pbar_list[0].update(1)
                pbar_list[0].set_postfix(Success=SUCCESSFUL_CONNECTIONS, refresh=False)
            task_queue.task_done()
            
    if ws: ws.close()

# ==============================================================================
# 📂 GIT SYMBOLS REPOSITORY & MAP.JSON LOOKUP
# ==============================================================================
def ensure_symbols_repo():
    """Ensures the symbols repository is pulled from GitHub."""
    possible_dirs = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        "tdv_aio",
        "/kaggle/working/tdv_aio",
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

def _is_hf_404(exc):
    """Returns True ONLY if Hugging Face explicitly confirmed HTTP 404 (file does not exist in repo)."""
    if EntryNotFoundError is not None and isinstance(exc, EntryNotFoundError):
        return True
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) == 404:
        return True
    msg = str(exc)
    return "404 Client Error" in msg or "Entry Not Found" in msg

def _download_and_load_hf_chunk(repo_file, is_encrypted, ENCRYPTION_KEY, temp_download_dir, max_attempts=6):
    """Downloads and parses an existing chunk from Hugging Face with retries.
    Returns (df, True) if loaded, or (None, False) ONLY on confirmed HTTP 404.
    Raises RuntimeError on any persistent network/server/decryption failure to prevent overwriting historical data."""
    last_err = None
    for attempt in range(max_attempts):
        downloaded_path = None
        try:
            downloaded_path = hf_hub_download(
                repo_id=HF_REPO_ID,
                repo_type="dataset",
                filename=repo_file,
                local_dir=temp_download_dir,
                force_download=(attempt > 0)
            )
            if is_encrypted:
                if not ENCRYPTION_KEY:
                    raise RuntimeError("ENCRYPTION_KEY is required to decrypt existing chunk.")
                fernet = Fernet(ENCRYPTION_KEY)
                with open(downloaded_path, "rb") as f:
                    enc_data = f.read()
                dec_data = fernet.decrypt(enc_data)
                df = pd.read_parquet(io.BytesIO(dec_data))
            else:
                df = pd.read_parquet(downloaded_path)
            return df, True
        except Exception as e:
            if _is_hf_404(e):
                return None, False
            last_err = e
            wait_s = min(60, 5 * (2 ** attempt))
            print(f"⚠️ [HF Pull Retry {attempt+1}/{max_attempts}] Failed to pull/read {repo_file}: {e}. Retrying in {wait_s}s...")
            time.sleep(wait_s)
        finally:
            if downloaded_path and os.path.exists(downloaded_path):
                try:
                    os.remove(downloaded_path)
                except Exception:
                    pass

    raise RuntimeError(
        f"CRITICAL DATA PROTECTION ABORT: Failed to pull/decrypt existing HF chunk '{repo_file}' after {max_attempts} attempts. "
        f"Refusing to overwrite historical data! Last error: {last_err}"
    )

def pull_and_merge_existing_chunk(new_df, interval, csv_filename, chunk_idx, ENCRYPTION_KEY):
    """Pulls existing parquet chunk from Hugging Face directly into Kaggle working directory
    without using HF default cache, decrypts if needed, merges with newly fetched bars,
    and immediately purges the temporary download to keep disk at 0%."""
    chunk_base = f"{csv_filename}_{interval}_chunk_{chunk_idx + 1}.parquet"
    repo_file_enc = f"data/{csv_filename}/{interval}/{chunk_base}.enc"
    repo_file_plain = f"data/{csv_filename}/{interval}/{chunk_base}"
        
    temp_download_dir = os.path.join(base_dir, "temp_downloads")
    os.makedirs(temp_download_dir, exist_ok=True)
    
    # 1. Try downloading encrypted chunk first (only falls back on confirmed HTTP 404)
    old_df, found_enc = _download_and_load_hf_chunk(
        repo_file_enc, is_encrypted=True, ENCRYPTION_KEY=ENCRYPTION_KEY, temp_download_dir=temp_download_dir
    )
    if not found_enc:
        # 2. Confirmed 404 on .parquet.enc -> check if legacy unencrypted chunk exists
        old_df, found_plain = _download_and_load_hf_chunk(
            repo_file_plain, is_encrypted=False, ENCRYPTION_KEY=ENCRYPTION_KEY, temp_download_dir=temp_download_dir
        )
        if found_plain:
            print(f"🔄 Migrating legacy unencrypted chunk {repo_file_plain} to encrypted format.")

    if old_df is not None and not old_df.empty:
        try:
            old_df["time"] = old_df["time"].astype("int64")
            new_df["time"] = new_df["time"].astype("int64")
            old_df["symbol"] = old_df["symbol"].astype(str)
            new_df["symbol"] = new_df["symbol"].astype(str)
            
            # Use combine_first to retain full historical bars while updating overlapping bars with newest data
            df_old_idx = old_df.set_index(["symbol", "time"])
            df_new_idx = new_df.set_index(["symbol", "time"])
            merged_df = df_new_idx.combine_first(df_old_idx).reset_index()
            merged_df = merged_df.sort_values(by=["symbol", "time"]).reset_index(drop=True)
        except Exception as merge_err:
            print(f"⚠️ Merge fallback for {repo_file_enc}: {merge_err}")
            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["symbol", "time"], keep="last")
            merged_df = merged.sort_values(by=["symbol", "time"]).reset_index(drop=True)

        if len(merged_df) < len(old_df):
            raise RuntimeError(
                f"CRITICAL DATA PROTECTION ABORT: Merged chunk {repo_file_enc} has fewer bars ({len(merged_df)}) "
                f"than historical chunk ({len(old_df)})!"
            )
        print(f"📥 Pulled & merged {repo_file_enc}: {len(old_df)} historical + {len(new_df)} new -> {len(merged_df)} total bars.")
        return merged_df
    else:
        print(f"✨ Brand-new chunk (confirmed 404 on HF): {repo_file_enc} ({len(new_df)} bars, no prior HF data).")
        new_df["time"] = new_df["time"].astype("int64")
        new_df["symbol"] = new_df["symbol"].astype(str)
        return new_df.sort_values(by=["symbol", "time"]).reset_index(drop=True)

def save_and_sync_interval(interval, csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs):
    """Flushes dataframe list for an interval, pulls and merges with existing HF chunk,
    encrypts data with AES-128 (Fernet), saves to Parquet (.parquet.enc), and schedules HF sync."""
    if not ENCRYPTION_KEY:
        raise RuntimeError("CRITICAL ERROR: Output must always be encrypted, but ENCRYPTION_KEY is missing!")
        
    with data_lock:
        df_list = current_file_data[interval]
        if not df_list: return
        raw_new_df = pd.concat(df_list, ignore_index=True)
        current_file_data[interval] = []
        
    combined_df = pull_and_merge_existing_chunk(raw_new_df, interval, csv_filename, chunk_idx, ENCRYPTION_KEY)
    
    # Always write encrypted files (.parquet.enc) to protect data
    chunk_filename = f"{csv_filename}_{interval}_chunk_{chunk_idx + 1}.parquet.enc"
    chunk_dir = os.path.join(data_dir, csv_filename, interval)
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, chunk_filename)
    
    temp_buffer = io.BytesIO()
    combined_df.to_parquet(temp_buffer, index=False)
    fernet = Fernet(ENCRYPTION_KEY)
    encrypted_data = fernet.encrypt(temp_buffer.getvalue())
    with open(chunk_path, "wb") as f:
        f.write(encrypted_data)
    del temp_buffer
    del encrypted_data
        
    symbol_counts = combined_df["symbol"].value_counts().to_dict()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    for s, c in symbol_counts.items():
        uncommitted_checkpoint_entries.append(
            f"{timestamp}, {s}, {interval}, checkpoint, Bars: {c}, data/{csv_filename}/{interval}/{os.path.basename(chunk_path)}"
        )
    uncommitted_csv_dirs.add(os.path.join(data_dir, csv_filename))
    del combined_df
    del raw_new_df
    gc.collect()

# ==============================================================================
# 🚀 MAIN WEEKLY CONTROLLER
# ==============================================================================
def main():
    print("==================================================================")
    print(f"🚀 TRADINGVIEW WEEKLY FETCHER (NODE {NOTEBOOK_INDEX + 1}/{TOTAL_NOTEBOOKS} | {WEEK_TAG})")
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

    # 1b. Enforce Mandatory End-to-End Encryption
    print("🔒 End-to-End Encryption: MANDATORY (Always Active)")
    if Fernet is None:
        print("❌ CRITICAL ERROR: 'cryptography' library is not installed. Data cannot be encrypted!")
        sys.exit(1)
        
    password = os.environ.get("DATA_PASSWORD", DATA_PASSWORD)
    if not password:
        try:
            from kaggle_secrets import UserSecretsClient
            password = UserSecretsClient().get_secret("DATA_PASSWORD")
        except Exception:
            pass
            
    if not password:
        print("❌ CRITICAL ERROR: Failed to load DATA_PASSWORD from os.environ or Kaggle Secrets.")
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
        print(f"❌ CRITICAL ERROR: Key derivation failed: {e}")
        sys.exit(1)

    if not ENCRYPTION_KEY:
        print("❌ CRITICAL ERROR: Encryption key could not be established. Aborting to protect data.")
        sys.exit(1)

    # 2. Auto-Login & JWT Token Manager for This Node
    if os.environ.get("TV_EMAILS_JSON"):
        try:
            FREE_EMAILS[:] = json.loads(os.environ["TV_EMAILS_JSON"])
        except Exception as e:
            print(f"⚠️ Could not parse TV_EMAILS_JSON: {e}")

    assigned_email = FREE_EMAILS[NOTEBOOK_INDEX % len(FREE_EMAILS)] if FREE_EMAILS else ""
    tv_password = os.environ.get("TV_LOGIN_PASSWORD", "")

    free_token_mgr = TVTokenManager(
        email=assigned_email,
        password=tv_password,
        label=f"NODE_{NOTEBOOK_INDEX}_FREE",
        fallback_emails=FREE_EMAILS
    )
    print(f"🆓 FREE POOL: Node {NOTEBOOK_INDEX} authenticated as {free_token_mgr.email} (JWT ready, 3h auto-refresh active).")
    
    # Dynamically resolve live corporate action and study descriptors from TradingView server
    resolve_live_study_descriptors(server="data")
    
    print(f"\n📊 WORKER ALLOCATION FOR THIS NODE:")
    print(f"  • {FREE_WORKERS} Free Workers -> [{', '.join(ALL_INTERVALS)}] on data.tradingview.com (Single-batch weekly fetch)")
    print(f"  • Total Active Connections: {FREE_WORKERS}\n")

    # 3. Pull Checkpoints from Hugging Face (Scoped exclusively to current WEEK_TAG, stored in Kaggle working dir)
    print(f"⏳ Synchronizing checkpoint logs for {WEEK_TAG} from Hugging Face...")
    try:
        from huggingface_hub import hf_hub_download
        for log_file in ["checkpoint.log", "failures.log"]:
            try:
                downloaded_path = hf_hub_download(
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    filename=f"{log_repo_dir}/{log_file}",
                    local_dir=base_dir
                )
                dest_path = os.path.join(log_dir, log_file)
                if os.path.abspath(downloaded_path) != os.path.abspath(dest_path):
                    shutil.copy(downloaded_path, dest_path)
                    if os.path.exists(downloaded_path):
                        os.remove(downloaded_path)
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
    
    # 5. Launch Worker Threads (100% Free Pool)
    print(f"🚀 Launching {FREE_WORKERS} Free workers...")
    for _ in range(FREE_WORKERS):
        threading.Thread(
            target=worker_loop,
            args=(free_token_mgr, pbar_list),
            daemon=True
        ).start()
        time.sleep(0.15)

    global uncommitted_checkpoint_entries, uncommitted_csv_dirs
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
                    task_queue.put((csv_filename, sym, "1D", False))
                    phase1_tasks += 1
                    
            if phase1_tasks > 0:
                print(f"⏳ [Phase 1: 1D Scan] Queued {phase1_tasks} tasks for unadjusted OHLCV + splits, dividends, earnings, rolls...")
                pbar_list[0] = tqdm(
                    total=phase1_tasks,
                    desc=f"1D Scan {csv_filename} [{chunk_idx+1}/{len(symbol_chunks)}]",
                    mininterval=15.0,
                    file=sys.stdout
                )
                phase1_finished = wait_for_queue(task_queue, f"1D Scan {csv_filename}", phase1_tasks, pbar_list)
                if pbar_list[0]: pbar_list[0].close()
                if not phase1_finished or IS_BANNED:
                    print("🛑 Halting due to rate-limit/ban trigger.")
                    break
                    
                # Retry transient Phase 1 network errors
                retry_round = 1
                while retry_list and not IS_BANNED and retry_round <= MAX_NETWORK_RETRIES:
                    with data_lock:
                        retries = list(retry_list)
                        retry_list.clear()
                    if not retries: break
                    print(f"⚠️ [Phase 1 Retry Round {retry_round}] Retrying {len(retries)} 1D tasks...")
                    pbar_list[0] = tqdm(total=len(retries), desc=f"1D Retries R{retry_round}", mininterval=10.0, file=sys.stdout)
                    for r in retries: task_queue.put((r[0], r[1], r[2], True))
                    wait_for_queue(task_queue, f"1D Retries R{retry_round}", len(retries), pbar_list)
                    if pbar_list[0]: pbar_list[0].close()
                    retry_round += 1
                    
                # Save 1D Parquet chunk
                save_and_sync_interval("1D", csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs)
            else:
                print("ℹ️ Chunk already completed in previous session. Skipping.")
                continue

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
            phase2_tasks = 0
            
            # Remaining Macro
            for sym in symbol_chunk:
                meta = symbol_metadata.get(sym, {})
                if meta.get("error"):
                    for interval in ["1W", "1M"]:
                        task_str = f"{sym} ({interval})"
                        COMPLETED_TASKS.add(task_str)
                    continue
                    
                for interval in ["1W", "1M"]:
                    task_str = f"{sym} ({interval})"
                    if task_str not in COMPLETED_TASKS:
                        COMPLETED_TASKS.add(task_str)
                        task_queue.put((csv_filename, sym, interval, False))
                        phase2_tasks += 1
                        
            # Eligible Intraday
            for sym in intraday_eligible:
                for interval in INTRADAY_INTERVALS:
                    task_str = f"{sym} ({interval})"
                    if task_str not in COMPLETED_TASKS:
                        COMPLETED_TASKS.add(task_str)
                        task_queue.put((csv_filename, sym, interval, False))
                        phase2_tasks += 1

            if phase2_tasks > 0:
                print(f"🚀 [Phase 2] Queued {phase2_tasks} tasks (Macro + Eligible Intraday)...")
                pbar_list[0] = tqdm(
                    total=phase2_tasks,
                    desc=f"Phase 2 {csv_filename} [{chunk_idx+1}/{len(symbol_chunks)}]",
                    mininterval=20.0,
                    file=sys.stdout
                )
                
                # Wait for queue
                phase2_finished = wait_for_queue(task_queue, f"Phase 2 {csv_filename}", phase2_tasks, pbar_list)
                if pbar_list[0]: pbar_list[0].close()
                if not phase2_finished or IS_BANNED: break
                
                # End-of-Chunk Network Retry Loop
                retry_round = 1
                while retry_list and not IS_BANNED and retry_round <= MAX_NETWORK_RETRIES:
                    with data_lock:
                        current_retries = list(retry_list)
                        retry_list.clear()
                        
                    total_retries = len(current_retries)
                    if total_retries == 0: break
                    
                    cooldown = min(60, 15 * retry_round)
                    print(f"\n⚠️ [Phase 2 Network Retry Round {retry_round}/{MAX_NETWORK_RETRIES}] {total_retries} tasks pending TradingView confirmation. Pausing for {cooldown}s...")
                    time.sleep(cooldown)
                    
                    pbar_list[0] = tqdm(total=total_retries, desc=f"Phase 2 Retries R{retry_round}", mininterval=15.0, file=sys.stdout)
                    for r in current_retries: task_queue.put((r[0], r[1], r[2], True))
                    
                    wait_for_queue(task_queue, f"Phase 2 Retries R{retry_round}", total_retries, pbar_list)
                    if pbar_list[0]: pbar_list[0].close()
                    retry_round += 1
                    
                # Save Parquet for each interval
                for interval in ALL_INTERVALS:
                    if interval != "1D":  # 1D was already saved in Phase 1
                        save_and_sync_interval(interval, csv_filename, chunk_idx, ENCRYPTION_KEY, uncommitted_checkpoint_entries, uncommitted_csv_dirs)

            # ------------------------------------------------------------------
            # ☁️ BATCH SYNC TO HUGGING FACE & LOCAL CLEANUP (SINGLE ATOMIC COMMIT)
            # ------------------------------------------------------------------
            if uncommitted_checkpoint_entries:
                with open(os.path.join(log_dir, "checkpoint.log"), "a", encoding="utf-8") as f:
                    for entry in uncommitted_checkpoint_entries:
                        f.write(entry + "\n")
                uncommitted_checkpoint_entries.clear()

            has_data = any(os.path.exists(d) for d in uncommitted_csv_dirs)
            has_logs = os.path.exists(log_dir) and (
                (os.path.exists(os.path.join(log_dir, "checkpoint.log")) and os.path.getsize(os.path.join(log_dir, "checkpoint.log")) > 0) or
                (os.path.exists(os.path.join(log_dir, "failures.log")) and os.path.getsize(os.path.join(log_dir, "failures.log")) > 0)
            )
            if has_data or has_logs:
                print(f"🚀 Pushing Parquet data & logs for {csv_filename} chunk {chunk_idx+1} in 1 commit to Hugging Face...")
                for attempt in range(8):
                    try:
                        api.upload_folder(
                            folder_path=base_dir,
                            path_in_repo=".",
                            repo_id=HF_REPO_ID,
                            repo_type="dataset",
                            allow_patterns=["data/**", f"{log_repo_dir}/**"],
                            commit_message=f"Sync {NOTEBOOK_ID} {csv_filename} chunk {chunk_idx+1} {WEEK_TAG}"
                        )
                        print(f"✅ Successfully synced {csv_filename} chunk {chunk_idx+1} (data + logs) to Hugging Face.")
                        break
                    except Exception as e:
                        wait_s = 60 * (attempt + 1) if "429" in str(e) else 5 * (attempt + 1)
                        print(f"⚠️ Push attempt {attempt+1}/8 failed: {e}. Waiting {wait_s}s...")
                        time.sleep(wait_s)
                else:
                    raise RuntimeError(f"Failed to push {csv_filename} chunk {chunk_idx+1} after 8 attempts.")

                # Clean local files to keep Kaggle disk at 0% usage
                for d in list(uncommitted_csv_dirs):
                    shutil.rmtree(d, ignore_errors=True)
                uncommitted_csv_dirs.clear()

    # Final flush
    if uncommitted_checkpoint_entries and not IS_BANNED:
        print("\n⏳ Attempting final push for uncommitted data before finishing...")
        with open(os.path.join(log_dir, "checkpoint.log"), "a", encoding="utf-8") as f:
            for entry in uncommitted_checkpoint_entries:
                f.write(entry + "\n")
        uncommitted_checkpoint_entries.clear()
        for attempt in range(8):
            try:
                api.upload_folder(
                    folder_path=base_dir,
                    path_in_repo=".",
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    allow_patterns=["data/**", f"{log_repo_dir}/**"],
                    commit_message=f"Final sync {NOTEBOOK_ID} {WEEK_TAG}"
                )
                for d in list(uncommitted_csv_dirs):
                    shutil.rmtree(d, ignore_errors=True)
                uncommitted_csv_dirs.clear()
                print("✅ Final sync completed successfully.")
                break
            except Exception as e:
                wait_s = 60 * (attempt + 1) if "429" in str(e) else 5 * (attempt + 1)
                print(f"⚠️ Final push attempt {attempt+1}/8 failed: {e}. Waiting {wait_s}s...")
                time.sleep(wait_s)
        else:
            raise RuntimeError("Failed to final push data to Hugging Face after 8 attempts.")

    # 7. Node Completion Marker
    if not IS_BANNED:
        completion_marker_path = os.path.join(log_dir, "COMPLETED")
        try:
            with open(completion_marker_path, "w", encoding="utf-8") as f:
                f.write(f"Completed at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
            api.upload_file(
                path_or_fileobj=completion_marker_path,
                path_in_repo=f"{log_repo_dir}/COMPLETED",
                repo_id=HF_REPO_ID,
                repo_type="dataset",
                commit_message=f"{NOTEBOOK_ID} {WEEK_TAG} run completed"
            )
            print(f"🏁 Marked {NOTEBOOK_ID} as COMPLETED for {WEEK_TAG} on Hugging Face.")
        except Exception as e:
            print(f"⚠️ Could not upload completion marker: {e}")

    print("\n==================================================================")
    status_str = "ABORTED (Rate Limit / Ban Triggered)" if IS_BANNED else "COMPLETED (All Assigned Chunks Done)"
    print(f"🏁 RUN STATUS: {status_str}")
    print(f"📊 Total Successful Downloads: {SUCCESSFUL_CONNECTIONS}")
    print("==================================================================")
    if IS_BANNED:
        sys.exit(2)

if __name__ == "__main__":
    main()
