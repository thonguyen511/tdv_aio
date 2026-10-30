import os
import sys
import json
import time
import shutil
import argparse
import subprocess
from datetime import datetime, timezone, timedelta

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
except Exception:
    pass


DEFAULT_KAGGLE_ACCOUNTS = [
    {
        "username": "thonguyen511",
        "token_env": "KAGGLE_TOKEN_0",
        "nodes": list(range(0, 5))   # Nodes 0, 1, 2, 3, 4
    },
    {
        "username": "hero0511acc",
        "token_env": "KAGGLE_TOKEN_1",
        "nodes": list(range(5, 10))  # Nodes 5, 6, 7, 8, 9
    },
    {
        "username": "tqunacc",
        "token_env": "KAGGLE_TOKEN_2",
        "nodes": list(range(10, 15)) # Nodes 10, 11, 12, 13, 14
    },
    {
        "username": "acc3conheo",
        "token_env": "KAGGLE_TOKEN_3",
        "nodes": list(range(15, 20)) # Nodes 15, 16, 17, 18, 19
    }
]


def load_kaggle_accounts():
    """Loads Kaggle account configurations from KAGGLE_ACCOUNTS_JSON or KAGGLE_TOKEN_0..3 env vars."""
    raw_json = os.environ.get("KAGGLE_ACCOUNTS_JSON", "").strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list) and len(parsed) == 4:
                return parsed
        except Exception as e:
            print(f"[!] Warning parsing KAGGLE_ACCOUNTS_JSON: {e}")

    accounts = []
    for idx, item in enumerate(DEFAULT_KAGGLE_ACCOUNTS):
        token = os.environ.get(item["token_env"], "").strip()
        accounts.append({
            "username": item["username"],
            "token": token,
            "nodes": list(item["nodes"])
        })
    return accounts


KAGGLE_ACCOUNTS = load_kaggle_accounts()

REPO_URL = "https://github.com/thonguyen511/tdv_aio.git"
HF_REPO_ID = os.environ.get("HF_REPO_ID", "thonguyen511/temp")
STATE_FILE = "node_status.json"

ACTIVE_KERNEL_STATES = {"running", "queued", "cancelrequested"}
MAX_ERROR_RETRIES = 6
RATE_LIMIT_COOLDOWN_SECONDS = 1800  # 30 minutes cooldown on rate-limit/ban (exit code 2)
ERROR_COOLDOWN_SECONDS = 300        # 5 minutes cooldown on unexpected error


def get_current_week_m():
    """Returns WEEK_M string using formula: iso_week + 2-digit iso_year (e.g. '4426')."""
    now_utc = datetime.now(timezone.utc)
    iso_year, iso_week, _ = now_utc.isocalendar()
    return f"{iso_week}{iso_year % 100:02d}"


def get_account_for_node(node_index):
    """Finds the account configuration for a given node index (0-19)."""
    for acc in KAGGLE_ACCOUNTS:
        if node_index in acc["nodes"]:
            return acc
    raise ValueError(f"Invalid node_index: {node_index}. Must be between 0 and 19.")


def get_env_with_token(token):
    """Returns environment with KAGGLE_API_TOKEN set and also writes ~/.kaggle/access_token."""
    env = os.environ.copy()
    if token:
        env["KAGGLE_API_TOKEN"] = token
        try:
            kaggle_dir = os.path.expanduser("~/.kaggle")
            os.makedirs(kaggle_dir, exist_ok=True)
            with open(os.path.join(kaggle_dir, "access_token"), "w", encoding="utf-8") as f:
                f.write(token.strip())
        except Exception:
            pass
    return env


def get_kernel_slug(username, node_index):
    """Standardized kernel slug for a node."""
    return f"{username}/tdv-node-{node_index}"


def get_kernel_state(username, node_index, env):
    """Queries `kaggle kernels status` for a node and returns normalized state:
    'running', 'queued', 'cancelrequested', 'complete', 'error', 'cancelacknowledged', 'not_found', or 'unknown'.
    """
    slug = get_kernel_slug(username, node_index)
    try:
        res = subprocess.run(
            ["kaggle", "kernels", "status", slug],
            env=env,
            capture_output=True,
            text=True,
            timeout=30
        )
        out = ((res.stdout or "") + " " + (res.stderr or "")).strip().lower()
        if "403" in out or "404" in out or "not found" in out:
            return "not_found", out
        for st in ("running", "queued", "cancelrequested", "cancelacknowledged", "complete", "error"):
            if f'"{st}"' in out or f"status {st}" in out or f"has status \"{st}\"" in out or st in out:
                return st, out
        return "unknown", out
    except Exception as e:
        return "unknown", str(e)


def read_initial_mode(repo_dir="."):
    """Reads initial.txt to see if we are in initial phase."""
    candidates = [
        os.path.join(repo_dir, "initial.txt"),
        "initial.txt",
        os.path.join(os.path.dirname(__file__), "initial.txt")
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return f.read().strip().lower() == "true"
            except Exception:
                pass
    return False


def set_initial_mode(val: bool, repo_dir="."):
    """Writes True or False to initial.txt."""
    target_path = os.path.join(repo_dir, "initial.txt")
    with open(target_path, "w", encoding="utf-8") as f:
        f.write("True\n" if val else "False\n")
    print(f"[+] Set initial.txt to {'True' if val else 'False'}")


def load_state(repo_dir="."):
    """Loads and normalizes node_status.json for the current run."""
    state_file = os.path.join(repo_dir, STATE_FILE)
    state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    is_initial = read_initial_mode(repo_dir)
    expected_phase = "initial" if is_initial else "weekly"
    current_week_m = "" if is_initial else get_current_week_m()
    expected_run_id = "INITIAL" if is_initial else f"WEEK_{state.get('week_m') or current_week_m}"

    # If switching between initial and weekly, initialize clean state
    if state.get("phase") != expected_phase or not state.get("run_id"):
        state = {
            "run_id": "INITIAL" if is_initial else f"WEEK_{current_week_m}",
            "phase": expected_phase,
            "week_m": "" if is_initial else current_week_m,
            "status": "running",
            "completed_nodes": [int(x) for x in state.get("completed_nodes", []) if isinstance(x, int) or str(x).isdigit()] if state.get("phase") == expected_phase else [],
            "pending_nodes": [],
            "node_retries": {},
            "cooldown_until": {},
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
    else:
        state.setdefault("status", "running")
        state.setdefault("completed_nodes", [])
        state.setdefault("pending_nodes", [])
        state.setdefault("node_retries", {})
        state.setdefault("cooldown_until", {})
        state.setdefault("week_m", "" if is_initial else (state.get("week_m") or current_week_m))

    return state


def save_and_commit_state(state, commit_msg, repo_dir=".", extra_files=None):
    """Saves node_status.json and commits/pushes to git with pull --rebase serialization."""
    state["completed_nodes"] = sorted(list({int(x) for x in state.get("completed_nodes", [])}))
    state["pending_nodes"] = sorted(list({int(x) for x in state.get("pending_nodes", []) if int(x) not in state["completed_nodes"]}))
    state["last_updated"] = datetime.now(timezone.utc).isoformat()

    state_file = os.path.join(repo_dir, STATE_FILE)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    files_to_add = [STATE_FILE]
    if extra_files:
        files_to_add.extend(extra_files)

    try:
        subprocess.run(["git", "-C", repo_dir, "add"] + files_to_add, check=True)
        diff_res = subprocess.run(["git", "-C", repo_dir, "diff", "--cached", "--quiet"])
        if diff_res.returncode == 0:
            return True  # Nothing changed
        subprocess.run(["git", "-C", repo_dir, "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "-C", repo_dir, "pull", "--rebase", "origin", "main"], check=True)
        subprocess.run(["git", "-C", repo_dir, "push", "origin", "main"], check=True)
        return True
    except Exception as e:
        print(f"[!] Warning committing state to git: {e}")
        return False


def sync_hf_completed_nodes(state):
    """Checks Hugging Face dataset for COMPLETED markers so dropped DONE callbacks self-heal."""
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    phase = state.get("phase", "initial")
    week_m = state.get("week_m") or get_current_week_m()

    try:
        from huggingface_hub import HfApi
        api = HfApi(token=hf_token if hf_token else None)
        files = set(api.list_repo_files(repo_id=HF_REPO_ID, repo_type="dataset"))
    except Exception as e:
        print(f"[i] Note: Could not list HF repo files to check COMPLETED markers: {e}")
        return False

    completed = set(state.get("completed_nodes", []))
    newly_found = []
    for node_idx in range(20):
        if phase == "initial":
            marker = f"log/NODE_{node_idx}/COMPLETED"
        else:
            marker = f"log/NODE_{node_idx}/WEEK_{week_m}/COMPLETED"
        if marker in files and node_idx not in completed:
            completed.add(node_idx)
            newly_found.append(node_idx)

    if newly_found:
        print(f"[+] Discovered COMPLETED markers on Hugging Face for nodes: {newly_found}")
        state["completed_nodes"] = sorted(list(completed))
        state["pending_nodes"] = [n for n in state.get("pending_nodes", []) if n not in completed]
        return True
    return False


def squash_hf_dataset_history():
    """Executes super_squash_history on Hugging Face dataset after all 20 nodes complete."""
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    print("=" * 65)
    print(f"🧹 SQUASHING HUGGING FACE GIT HISTORY ({HF_REPO_ID})")
    print("=" * 65)
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=hf_token if hf_token else None)
        api.super_squash_history(repo_id=HF_REPO_ID, repo_type="dataset")
        print("✅ Successfully squashed Hugging Face dataset history into 1 clean commit!")
        return True
    except Exception as e:
        print(f"[!] Warning squashing Hugging Face history: {e}")
        return False


def prepare_node_kernel(staging_dir, node_index, username, week_m=""):
    """Creates kernel-metadata.json and main.py in staging directory for a worker node.
    Injects runtime secrets from environment variables into the private Kaggle script."""
    os.makedirs(staging_dir, exist_ok=True)
    kernel_slug = f"tdv-node-{node_index}"

    metadata = {
        "id": f"{username}/{kernel_slug}",
        "title": f"TDV Node {node_index}",
        "code_file": "main.py",
        "language": "python",
        "kernel_type": "script",
        "is_private": "true",
        "enable_gpu": "false",
        "enable_internet": "true",
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": []
    }

    with open(os.path.join(staging_dir, "kernel-metadata.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    hf_token = os.environ.get("HF_TOKEN", "")
    gh_pat = os.environ.get("GH_PAT", "")
    data_password = os.environ.get("DATA_PASSWORD", "")
    tv_emails = os.environ.get("TV_EMAILS_JSON", "")
    tv_login_pass = os.environ.get("TV_LOGIN_PASSWORD", "")
    tv_ult_email = os.environ.get("TV_ULTIMATE_EMAIL", "")
    tv_ult_pass = os.environ.get("TV_ULTIMATE_PASSWORD", "")

    env_injections = {
        "NOTEBOOK_INDEX": str(node_index),
        "HF_REPO_ID": HF_REPO_ID,
    }
    if week_m:
        env_injections["WEEK_M"] = str(week_m)
    if hf_token:
        env_injections["HF_TOKEN"] = hf_token
    if gh_pat:
        env_injections["GH_PAT"] = gh_pat
    if data_password:
        env_injections["DATA_PASSWORD"] = data_password
    if tv_emails:
        env_injections["TV_EMAILS_JSON"] = tv_emails
    if tv_login_pass:
        env_injections["TV_LOGIN_PASSWORD"] = tv_login_pass
    if tv_ult_email:
        env_injections["TV_ULTIMATE_EMAIL"] = tv_ult_email
    if tv_ult_pass:
        env_injections["TV_ULTIMATE_PASSWORD"] = tv_ult_pass

    env_setup_lines = "\n".join(
        f"os.environ[{json.dumps(k)}] = {json.dumps(v)}"
        for k, v in env_injections.items()
    )

    runner_code = f'''import os
import sys
import subprocess

NODE_INDEX = {node_index}
{env_setup_lines}

print("=" * 65)
print(f"🚀 KAGGLE WORKER NODE {{NODE_INDEX}} STARTING")
print("=" * 65)

REPO_URL = "{REPO_URL}"
REPO_DIR = "tdv_aio"

try:
    # Ensure required libraries and headless Chromium for TradingView JWT auto-login are installed
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "websocket-client", "huggingface_hub", "cryptography", "playwright"], check=False)
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=False)
    subprocess.run([sys.executable, "-m", "playwright", "install-deps", "chromium"], check=False)

    if not os.path.exists(REPO_DIR):
        print(f"📥 Cloning codebase from {{REPO_URL}}...")
        subprocess.run(["git", "clone", "--depth", "1", REPO_URL, REPO_DIR], check=True)
    else:
        print(f"🔄 Pulling latest updates from {{REPO_URL}}...")
        subprocess.run(["git", "-C", REPO_DIR, "pull"], check=True)

    code_dir = os.path.join(REPO_DIR, "code")
    if os.path.exists(code_dir):
        sys.path.insert(0, code_dir)

    import timer
    timer.run_supervised_node(node_index=NODE_INDEX, repo_dir=REPO_DIR)
except Exception as setup_err:
    print(f"[!] Fatal setup/supervisor error on Node {{NODE_INDEX}}: {{setup_err}}")
    try:
        import callback
        callback.send_callback(status="ERROR", node_index=NODE_INDEX, message=f"Runner exception: {{setup_err}}")
    except Exception:
        pass
    sys.exit(1)
'''
    with open(os.path.join(staging_dir, "main.py"), "w", encoding="utf-8") as f:
        f.write(runner_code)


def push_single_node(node_index, staging_root="staging", max_retries=2, retry_delay=10, week_m="", check_running=True):
    """Pushes a single node notebook kernel to Kaggle with fast session-limit awareness.
    Does not block the GitHub Actions runner with multi-minute sleeps if the slot is still busy."""
    acc = get_account_for_node(node_index)
    username = acc["username"]
    token = acc["token"]
    if not token:
        print(f"❌ Missing Kaggle token for account '{username}' (Node {node_index}). Set KAGGLE_TOKEN_{node_index // 5} secret.")
        return False

    env = get_env_with_token(token)

    if check_running:
        k_state, k_raw = get_kernel_state(username, node_index, env)
        if k_state in ACTIVE_KERNEL_STATES:
            print(f"  ⏳ Node {node_index} ({username}/tdv-node-{node_index}) is currently '{k_state}' on Kaggle. Skipping push until slot frees.")
            return False

    node_staging = os.path.join(staging_root, f"node_{node_index}")
    prepare_node_kernel(node_staging, node_index, username, week_m=week_m)

    print(f"[+] Pushing Node {node_index} to Kaggle account '{username}'...")

    for attempt in range(max_retries):
        res = subprocess.run(["kaggle", "kernels", "push", "-p", node_staging], env=env, capture_output=True, text=True)
        combined_output = ((res.stdout or "") + "\n" + (res.stderr or "")).strip()

        # Check for Kaggle 5-session limit collision
        if "Maximum batch CPU session count" in combined_output or "session count" in combined_output:
            if attempt + 1 < max_retries:
                print(f"  ⚠️ 5-session limit active on '{username}' (Attempt {attempt+1}/{max_retries}). Waiting {retry_delay}s...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"  ⏳ Account '{username}' still at 5/5 active sessions. Deferring Node {node_index} to next reconcile.")
                return False

        if res.returncode == 0 and ("Kernel version" in combined_output or "successfully pushed" in combined_output):
            print(f"✅ Node {node_index} pushed successfully! ({username}/tdv-node-{node_index})")
            print(res.stdout.strip())
            return True
        else:
            print(f"  ⚠️ Push attempt {attempt+1}/{max_retries} failed: {combined_output}")
            if attempt + 1 < max_retries:
                time.sleep(retry_delay)

    print(f"❌ Failed to push Node {node_index} after {max_retries} attempts.")
    return False


def push_fleet(staging_root="staging", delay_between_pushes=3, repo_dir="."):
    """Initializes a clean run state and pushes all 20 nodes across the 4 Kaggle accounts."""
    is_initial = read_initial_mode(repo_dir)
    week_m = "" if is_initial else get_current_week_m()
    run_id = "INITIAL" if is_initial else f"WEEK_{week_m}"

    print("=" * 65)
    print(f"🚀 LAUNCHING KAGGLE FLEET (20 NODES) - {run_id}")
    print(f"• Phase Mode: {'INITIAL HISTORICAL' if is_initial else f'WEEKLY LIVE REFRESH ({run_id})'}")
    print("=" * 65)

    state = {
        "run_id": run_id,
        "phase": "initial" if is_initial else "weekly",
        "week_m": week_m,
        "status": "running",
        "completed_nodes": [],
        "pending_nodes": [],
        "node_retries": {},
        "cooldown_until": {},
        "launched_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

    # Check if any nodes already completed this run on Hugging Face
    sync_hf_completed_nodes(state)
    completed = set(state.get("completed_nodes", []))

    success_count = 0
    deferred = []
    for node_idx in range(20):
        if node_idx in completed:
            print(f"ℹ️ Node {node_idx} already has COMPLETED marker on HF for {run_id}. Skipping push.")
            success_count += 1
            continue
        ok = push_single_node(node_idx, staging_root, max_retries=2, retry_delay=10, week_m=week_m, check_running=True)
        if ok:
            success_count += 1
        else:
            deferred.append(node_idx)
        time.sleep(delay_between_pushes)

    state["pending_nodes"] = deferred
    save_and_commit_state(state, f"Launch fleet for {run_id} ({success_count}/20 active, {len(deferred)} deferred)", repo_dir=repo_dir)

    print(f"\n[+] Fleet launch pass complete: {success_count}/20 nodes pushed/done ({len(deferred)} deferred to next reconcile).")
    return success_count == 20


def push_standby(staging_root="staging"):
    """Deploys the Standby Master Clock notebook on account 0 (thonguyen511)."""
    acc = KAGGLE_ACCOUNTS[0]
    username = acc["username"]
    token = acc["token"]
    if not token:
        print(f"❌ Missing Kaggle token for '{username}'. Cannot push Standby Clock.")
        return False

    staging_dir = os.path.join(staging_root, "standby_clock")
    os.makedirs(staging_dir, exist_ok=True)

    metadata = {
        "id": f"{username}/tdv-standby-master-clock",
        "title": "TDV Standby Master Clock",
        "code_file": "main.py",
        "language": "python",
        "kernel_type": "script",
        "is_private": "true",
        "enable_gpu": "false",
        "enable_internet": "true",
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": []
    }

    with open(os.path.join(staging_dir, "kernel-metadata.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    gh_pat = os.environ.get("GH_PAT", "")
    gh_pat_line = f"os.environ['GH_PAT'] = {json.dumps(gh_pat)}\n" if gh_pat else ""

    standby_runner = f'''import os
import sys
import subprocess

{gh_pat_line}
print("=" * 65)
print("⏱️ KAGGLE STANDBY MASTER CLOCK STARTING")
print("=" * 65)

REPO_URL = "{REPO_URL}"
REPO_DIR = "tdv_aio"

if not os.path.exists(REPO_DIR):
    subprocess.run(["git", "clone", "--depth", "1", REPO_URL, REPO_DIR], check=True)
else:
    subprocess.run(["git", "-C", REPO_DIR, "pull"], check=True)

code_dir = os.path.join(REPO_DIR, "code")
if os.path.exists(code_dir):
    sys.path.insert(0, code_dir)

import standby_clock
standby_clock.run_standby_clock()
'''
    with open(os.path.join(staging_dir, "main.py"), "w", encoding="utf-8") as f:
        f.write(standby_runner)

    env = get_env_with_token(token)
    print(f"[+] Pushing Standby Master Clock notebook to '{username}'...")
    res = subprocess.run(["kaggle", "kernels", "push", "-p", staging_dir], env=env, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"✅ Standby Master Clock pushed successfully!")
        return True
    else:
        print(f"❌ Failed to push Standby Clock: {res.stderr.strip()}")
        return False


def delete_fleet():
    """Deletes all 20 worker notebook kernels from Kaggle to clean version buildup."""
    print("=" * 65)
    print("🧹 CLEANING UP FLEET: DELETING ALL 20 KAGGLE KERNELS")
    print("=" * 65)

    for node_idx in range(20):
        acc = get_account_for_node(node_idx)
        username = acc["username"]
        token = acc["token"]
        kernel_slug = f"{username}/tdv-node-{node_idx}"

        env = get_env_with_token(token)
        print(f"Deleting {kernel_slug}...")
        res = subprocess.run(["kaggle", "kernels", "delete", "-y", kernel_slug], env=env, capture_output=True, text=True)
        if res.returncode == 0:
            print(f"  ✓ Deleted {kernel_slug}")
        else:
            print(f"  - Note: {kernel_slug} ({res.stderr.strip()})")
        time.sleep(1)

    print("\n✅ Fleet cleanup complete. All 20 worker kernels removed from Kaggle.")


def check_status(repo_dir="."):
    """Checks execution status for all 20 nodes and displays current run state."""
    state = load_state(repo_dir)
    sync_hf_completed_nodes(state)
    print("=" * 65)
    print(f"📊 KAGGLE FLEET STATUS REPORT ({state.get('run_id')} | status={state.get('status')})")
    print(f"• Completed Nodes ({len(state.get('completed_nodes', []))}/20): {state.get('completed_nodes', [])}")
    print(f"• Pending Nodes: {state.get('pending_nodes', [])}")
    print("=" * 65)

    for node_idx in range(20):
        acc = get_account_for_node(node_idx)
        username = acc["username"]
        token = acc["token"]
        env = get_env_with_token(token)
        k_state, k_raw = get_kernel_state(username, node_idx, env)
        done_tag = " [DONE]" if node_idx in state.get("completed_nodes", []) else ""
        print(f"Node {node_idx:2d} ({username}): {k_state.upper()}{done_tag} -> {k_raw}")


def finalize_completed_run(state, repo_dir="."):
    """Called once all 20/20 nodes have finished the current run."""
    run_id = state.get("run_id", "UNKNOWN")
    print(f"\n🎉 ALL 20 NODES COMPLETED FOR {run_id}!")

    # 1. Squash Hugging Face dataset git history now that all 20 nodes are finished writing
    squash_hf_dataset_history()

    extra_files = []
    is_initial = read_initial_mode(repo_dir)
    if is_initial:
        print("🏁 Phase 1 (Initial) Finished! Permanently setting initial.txt = False...")
        set_initial_mode(False, repo_dir)
        extra_files.append("initial.txt")

    state["status"] = "standby"
    state["pending_nodes"] = []
    state["node_retries"] = {}
    state["cooldown_until"] = {}
    state["completed_at"] = datetime.now(timezone.utc).isoformat()
    save_and_commit_state(state, f"Complete {run_id} (20/20 nodes done) and enter standby", repo_dir=repo_dir, extra_files=extra_files)

    # 2. Delete the 20 worker notebooks to eliminate version stacking and free slots
    print("🧹 Cleaning up worker notebooks to eliminate version stacking...")
    delete_fleet()

    # 3. Deploy Standby Master Clock to wait for Monday 00:00 UTC
    print("⏱️ Deploying Standby Master Clock...")
    push_standby()


def reconcile(repo_dir=".", staging_root="staging"):
    """
    Idempotent, account-aware reconciler.
    - Syncs COMPLETED markers from Hugging Face so dropped DONE callbacks self-heal.
    - Respects Kaggle's 5-session-per-account limit: counts active sessions ('running', 'queued')
      per account before pushing and skips nodes whose old VM is still shutting down.
    - Automatically re-pushes timed-out or stopped nodes once their slot is free.
    - Launches a new weekly run on Monday 00:00 UTC if in standby from a previous week.
    """
    state = load_state(repo_dir)
    now_ts = time.time()
    now_utc = datetime.now(timezone.utc)
    current_week_m = get_current_week_m()
    is_initial = read_initial_mode(repo_dir)

    print("=" * 65)
    print(f"🔄 RECONCILING FLEET STATE ({now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')})")
    print(f"• Run ID: {state.get('run_id')} | Status: {state.get('status')} | Initial Mode: {is_initial}")
    print("=" * 65)

    # Check if we are in standby and a new ISO week has started
    if not is_initial and state.get("status") == "standby":
        if state.get("week_m") != current_week_m:
            print(f"🚀 New ISO week detected ({current_week_m} != {state.get('week_m')}). Launching weekly fleet!")
            push_fleet(staging_root=staging_root, repo_dir=repo_dir)
            return
        else:
            print(f"ℹ️ Weekly run WEEK_{current_week_m} is already completed and in standby. Nothing to do.")
            return

    # Sync COMPLETED markers from Hugging Face
    state_changed = sync_hf_completed_nodes(state)
    completed = set(state.get("completed_nodes", []))
    pending = set(state.get("pending_nodes", []))
    retries = state.get("node_retries", {})
    cooldowns = state.get("cooldown_until", {})
    week_m = state.get("week_m", "")

    print(f"• Progress: {len(completed)}/20 nodes completed: {sorted(list(completed))}")

    if len(completed) >= 20:
        finalize_completed_run(state, repo_dir=repo_dir)
        return

    pushed_any = False

    # Reconcile account by account (5 nodes per account, max 5 concurrent sessions per account)
    for acc in KAGGLE_ACCOUNTS:
        username = acc["username"]
        token = acc["token"]
        if not token:
            print(f"⚠️ Skipping account '{username}': no Kaggle token configured.")
            continue

        env = get_env_with_token(token)
        node_states = {}
        active_count = 0

        for n in acc["nodes"]:
            k_state, _ = get_kernel_state(username, n, env)
            node_states[n] = k_state
            if k_state in ACTIVE_KERNEL_STATES:
                active_count += 1

        print(f"\n[Account: {username}] Active sessions: {active_count}/5 | States: {node_states}")

        for n in acc["nodes"]:
            if n in completed:
                continue

            k_state = node_states[n]

            # Leave already running/queued nodes alone
            if k_state in ACTIVE_KERNEL_STATES:
                if n in pending:
                    pending.discard(n)
                    state_changed = True
                continue

            # Check cooldown if rate-limited or recently errored
            cd_until = float(cooldowns.get(str(n), 0))
            if cd_until > now_ts:
                rem = int(cd_until - now_ts)
                print(f"  ⏳ Node {n} is in cooldown for another {rem}s. Skipping.")
                continue

            # Check error retry cap
            err_count = int(retries.get(str(n), 0))
            if k_state == "error" and n not in pending:
                err_count += 1
                retries[str(n)] = err_count
                state_changed = True
                if err_count > MAX_ERROR_RETRIES:
                    print(f"  ❌ Node {n} exceeded MAX_ERROR_RETRIES ({MAX_ERROR_RETRIES}). Requires manual check.")
                    continue
                print(f"  ⚠️ Node {n} is in ERROR state on Kaggle (retry {err_count}/{MAX_ERROR_RETRIES}). Will re-push.")

            # Node is not completed and not running ('complete', 'not_found', 'cancelacknowledged', or 'error')
            if active_count >= 5:
                print(f"  ⏳ Account '{username}' at 5/5 active sessions. Deferring Node {n} to next reconcile.")
                pending.add(n)
                state_changed = True
                continue

            print(f"  🔄 Re-pushing Node {n} (current Kaggle state: {k_state})...")
            ok = push_single_node(n, staging_root=staging_root, max_retries=2, retry_delay=10, week_m=week_m, check_running=False)
            if ok:
                active_count += 1
                pushed_any = True
                pending.discard(n)
                cooldowns.pop(str(n), None)
                state_changed = True
            else:
                pending.add(n)
                state_changed = True

    state["pending_nodes"] = sorted(list(pending))
    state["node_retries"] = retries
    state["cooldown_until"] = cooldowns

    if state_changed or pushed_any:
        save_and_commit_state(
            state,
            f"Reconcile {state.get('run_id')}: {len(completed)}/20 done, {len(pending)} pending",
            repo_dir=repo_dir
        )


def handle_callback(payload_str, repo_dir="."):
    """
    Processes incoming GitHub repository_dispatch webhook payloads and runs reconciliation.
    """
    try:
        data = json.loads(payload_str)
    except Exception as e:
        print(f"[!] Error parsing callback JSON: {e}")
        return

    status = data.get("status", "").upper()
    node_index = data.get("node_index")
    phase = data.get("phase")
    event = data.get("event") or status

    print(f"\n[+] Received Callback: status={status}, node={node_index}, event={event}, phase={phase}")

    # Standby Master Clock callbacks
    if event == "TRIGGER_WEEKLY" or status == "TRIGGER_WEEKLY":
        state = load_state(repo_dir)
        current_week_m = get_current_week_m()
        if not read_initial_mode(repo_dir) and state.get("status") == "running" and state.get("week_m") == current_week_m:
            print(f"ℹ️ Weekly fleet for WEEK_{current_week_m} is already running. Running reconcile instead.")
            reconcile(repo_dir=repo_dir)
        else:
            print("🚀 Standby Master Clock triggered Monday 00:00 UTC! Launching 20-node weekly fleet...")
            push_fleet(repo_dir=repo_dir)
        return

    if status == "REARM_STANDBY":
        print("⏱️ Standby session reached 11h45m. Waiting 15s then re-arming Standby Master Clock...")
        time.sleep(15)
        push_standby()
        return

    state = load_state(repo_dir)
    completed = set(state.get("completed_nodes", []))
    pending = set(state.get("pending_nodes", []))
    retries = state.get("node_retries", {})
    cooldowns = state.get("cooldown_until", {})

    if node_index is not None:
        node_index = int(node_index)

        if status == "DONE":
            print(f"✅ Node {node_index} reported DONE!")
            completed.add(node_index)
            pending.discard(node_index)
            cooldowns.pop(str(node_index), None)

        elif status == "TIMEOUT":
            print(f"🔄 Node {node_index} timed out at 11h45m mark. Queueing for continuation...")
            if node_index not in completed:
                pending.add(node_index)
            # Wait briefly (15s) to give Kaggle a chance to close the container before reconcile checks status
            time.sleep(15)

        elif status == "RATE_LIMITED":
            print(f"⚠️ Node {node_index} reported RATE_LIMITED (exit code 2). Setting {RATE_LIMIT_COOLDOWN_SECONDS}s cooldown...")
            if node_index not in completed:
                pending.add(node_index)
                cooldowns[str(node_index)] = time.time() + RATE_LIMIT_COOLDOWN_SECONDS

        elif status == "ERROR":
            err_count = int(retries.get(str(node_index), 0)) + 1
            retries[str(node_index)] = err_count
            print(f"⚠️ Node {node_index} reported ERROR (attempt {err_count}/{MAX_ERROR_RETRIES}). Setting {ERROR_COOLDOWN_SECONDS}s cooldown...")
            if node_index not in completed and err_count <= MAX_ERROR_RETRIES:
                pending.add(node_index)
                cooldowns[str(node_index)] = time.time() + ERROR_COOLDOWN_SECONDS

    state["completed_nodes"] = sorted(list(completed))
    state["pending_nodes"] = sorted(list(pending))
    state["node_retries"] = retries
    state["cooldown_until"] = cooldowns
    save_and_commit_state(state, f"Callback {status} from Node {node_index} ({len(completed)}/20 done)", repo_dir=repo_dir)

    # Always run reconcile after processing a node callback so any free slots are filled
    reconcile(repo_dir=repo_dir)


def main():
    parser = argparse.ArgumentParser(description="TradingView Distributed Kaggle Fleet Manager")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    subparsers.add_parser("push-fleet", help="Push all 20 worker nodes to Kaggle")
    subparsers.add_parser("reconcile", help="Reconcile all 20 nodes against HF markers and Kaggle session status")

    p_node = subparsers.add_parser("push-node", help="Push a single worker node")
    p_node.add_argument("node_index", type=int, help="Node index (0 to 19)")

    subparsers.add_parser("push-standby", help="Push Standby Master Clock notebook")
    subparsers.add_parser("delete-fleet", help="Delete all 20 worker kernels to clean versions")
    subparsers.add_parser("status", help="Check status of all 20 worker nodes")

    p_cb = subparsers.add_parser("handle-callback", help="Handle incoming webhook callback")
    p_cb.add_argument("--payload", required=False, default="", help="Callback JSON payload (or set CALLBACK_PAYLOAD env var)")

    args = parser.parse_args()

    if args.command == "push-fleet":
        push_fleet()
    elif args.command == "reconcile":
        reconcile()
    elif args.command == "push-node":
        state = load_state()
        push_single_node(args.node_index, week_m=state.get("week_m", ""), check_running=False)
    elif args.command == "push-standby":
        push_standby()
    elif args.command == "delete-fleet":
        delete_fleet()
    elif args.command == "status":
        check_status()
    elif args.command == "handle-callback":
        payload = args.payload or os.environ.get("CALLBACK_PAYLOAD", "{}")
        handle_callback(payload)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
