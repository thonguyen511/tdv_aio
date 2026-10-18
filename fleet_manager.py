import os
import sys
import json
import time
import shutil
import argparse
import subprocess
from datetime import datetime, timezone

# 4 Kaggle Accounts Mapping: 5 nodes per account = 20 nodes (0 to 19)
KAGGLE_ACCOUNTS = [
    {
        "username": "thonguyen511",
        "token": "KGAT_9a29974782c1c24e1e9bccf69a51e21d",
        "nodes": list(range(0, 5))   # Nodes 0, 1, 2, 3, 4
    },
    {
        "username": "hero0511acc",
        "token": "KGAT_8aad194411dbee85815ec01a682f3047",
        "nodes": list(range(5, 10))  # Nodes 5, 6, 7, 8, 9
    },
    {
        "username": "tqunacc",
        "token": "KGAT_46cc7c9f3ebb3239e81b25d0f7f6ae97",
        "nodes": list(range(10, 15)) # Nodes 10, 11, 12, 13, 14
    },
    {
        "username": "acc3conheo",
        "token": "KGAT_9b980e55d4c06e72e229bc520ae1ac98",
        "nodes": list(range(15, 20)) # Nodes 15, 16, 17, 18, 19
    }
]

REPO_URL = "https://github.com/thonguyen511/tdv_aio.git"
STATE_FILE = "node_status.json"

def get_account_for_node(node_index):
    """Finds the account configuration for a given node index (0-19)."""
    for acc in KAGGLE_ACCOUNTS:
        if node_index in acc["nodes"]:
            return acc
    raise ValueError(f"Invalid node_index: {node_index}. Must be between 0 and 19.")

def get_env_with_token(token):
    """Returns environment with KAGGLE_API_TOKEN set."""
    env = os.environ.copy()
    env["KAGGLE_API_TOKEN"] = token
    return env

def get_kernel_slug(username, node_index):
    """Standardized kernel slug for a node."""
    return f"{username}/tdv-node-{node_index}"

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
    """Writes True or False to initial.txt and commits if in git repo."""
    target_path = os.path.join(repo_dir, "initial.txt")
    with open(target_path, "w", encoding="utf-8") as f:
        f.write("True\n" if val else "False\n")
    print(f"[+] Set initial.txt to {'True' if val else 'False'}")

def prepare_node_kernel(staging_dir, node_index, username):
    """Creates kernel-metadata.json and main.py in staging directory for a worker node."""
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

    runner_code = f'''import os
import sys
import subprocess

NODE_INDEX = {node_index}
os.environ["NOTEBOOK_INDEX"] = str(NODE_INDEX)

print("=" * 65)
print(f"🚀 KAGGLE WORKER NODE {{NODE_INDEX}} STARTING")
print("=" * 65)

REPO_URL = "{REPO_URL}"
REPO_DIR = "tdv_aio"

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
'''
    with open(os.path.join(staging_dir, "main.py"), "w", encoding="utf-8") as f:
        f.write(runner_code)

def push_single_node(node_index, staging_root="staging"):
    """Pushes a single node notebook kernel to Kaggle."""
    acc = get_account_for_node(node_index)
    username = acc["username"]
    token = acc["token"]
    
    node_staging = os.path.join(staging_root, f"node_{node_index}")
    prepare_node_kernel(node_staging, node_index, username)

    env = get_env_with_token(token)
    print(f"[+] Pushing Node {node_index} to Kaggle account '{username}'...")
    
    res = subprocess.run(["kaggle", "kernels", "push", "-p", node_staging], env=env, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"✅ Node {node_index} pushed successfully! ({username}/tdv-node-{node_index})")
        print(res.stdout.strip())
        return True
    else:
        print(f"❌ Failed to push Node {node_index}: {res.stderr.strip()}")
        return False

def push_fleet(staging_root="staging", delay_between_pushes=5):
    """Pushes all 20 nodes across the 4 Kaggle accounts."""
    is_initial = read_initial_mode()
    print("=" * 65)
    print(f"🚀 LAUNCHING KAGGLE FLEET (20 NODES)")
    print(f"• Phase Mode: {'INITIAL HISTORICAL' if is_initial else 'WEEKLY LIVE REFRESH'}")
    print("=" * 65)

    success_count = 0
    for node_idx in range(20):
        ok = push_single_node(node_idx, staging_root)
        if ok:
            success_count += 1
        time.sleep(delay_between_pushes)

    print(f"\n[+] Fleet deployment complete: {success_count}/20 nodes pushed successfully.")
    return success_count == 20

def push_standby(staging_root="staging"):
    """Deploys the Standby Master Clock notebook on account 0 (thonguyen511)."""
    acc = KAGGLE_ACCOUNTS[0]
    username = acc["username"]
    token = acc["token"]

    staging_dir = os.path.join(staging_root, "standby_clock")
    os.makedirs(staging_dir, exist_ok=True)

    metadata = {
        "id": f"{username}/tdv-standby-clock",
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

    standby_runner = f'''import os
import sys
import subprocess

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
    """
    Deletes all 20 worker notebook kernels from Kaggle.
    Cleans up version buildup and resets the accounts.
    """
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

def check_status():
    """Checks execution status for all 20 nodes."""
    print("=" * 65)
    print("📊 KAGGLE FLEET STATUS REPORT")
    print("=" * 65)

    for node_idx in range(20):
        acc = get_account_for_node(node_idx)
        username = acc["username"]
        token = acc["token"]
        kernel_slug = f"{username}/tdv-node-{node_idx}"

        env = get_env_with_token(token)
        res = subprocess.run(["kaggle", "kernels", "status", kernel_slug], env=env, capture_output=True, text=True)
        status_line = res.stdout.strip() if res.returncode == 0 else f"Error: {res.stderr.strip()}"
        print(f"Node {node_idx:2d} ({username}): {status_line}")

def handle_callback(payload_str, repo_dir="."):
    """
    Processes incoming GitHub repository_dispatch webhook payloads.
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

    # Case 1: Node session hit 11h45m timeout -> re-push kernel for continuation
    if status == "TIMEOUT" and node_index is not None:
        print(f"🔄 Node {node_index} timed out at 11h45m mark. Re-triggering node...")
        push_single_node(node_index)

    # Case 2: Node finished workload completely
    elif status == "DONE" and node_index is not None:
        print(f"✅ Node {node_index} completed all assigned workload!")
        # Track node completion
        state = {}
        state_file = os.path.join(repo_dir, STATE_FILE)
        if os.path.exists(state_file):
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except Exception:
                state = {}

        completed = set(state.get("completed_nodes", []))
        completed.add(node_index)
        state["completed_nodes"] = sorted(list(completed))
        state["last_updated"] = datetime.now(timezone.utc).isoformat()

        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

        print(f"• Completed progress: {len(completed)}/20 nodes.")

        # If all 20 nodes finished
        if len(completed) >= 20:
            print("\n🎉 ALL 20 NODES COMPLETED!")
            state["completed_nodes"] = []  # Reset for next run
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)

            is_initial = read_initial_mode(repo_dir)
            if is_initial:
                print("🏁 Phase 1 (Initial) Finished! Permanently setting initial.txt = False...")
                set_initial_mode(False, repo_dir)
                # Commit change to git
                try:
                    subprocess.run(["git", "-C", repo_dir, "add", "initial.txt"], check=True)
                    subprocess.run(["git", "-C", repo_dir, "commit", "-m", "Switch initial.txt to False (Phase 1 Complete)"], check=True)
                    subprocess.run(["git", "-C", repo_dir, "push", "origin", "main"], check=True)
                except Exception as e:
                    print(f"[!] Warning committing initial.txt: {e}")

            # Delete the 20 worker notebooks to remove version accumulation
            print("🧹 Cleaning up worker notebooks to eliminate version stacking...")
            delete_fleet()

            # Deploy Standby Master Clock to wait for Monday 00:00 UTC
            print("⏱️ Deploying Standby Master Clock...")
            push_standby()

    # Case 3: Exact Monday 00:00 UTC arrived -> trigger weekly fleet
    elif event == "TRIGGER_WEEKLY" or status == "TRIGGER_WEEKLY":
        print("🚀 Standby Master Clock triggered Monday 00:00 UTC! Launching 20-node weekly fleet...")
        push_fleet()

    # Case 4: Standby session elapsed 11h45m -> re-arm standby
    elif status == "REARM_STANDBY":
        print("⏱️ Standby session reached 11h45m. Re-arming Standby Master Clock...")
        push_standby()

def main():
    parser = argparse.ArgumentParser(description="TradingView Distributed Kaggle Fleet Manager")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    subparsers.add_parser("push-fleet", help="Push all 20 worker nodes to Kaggle")
    
    p_node = subparsers.add_parser("push-node", help="Push a single worker node")
    p_node.add_argument("node_index", type=int, help="Node index (0 to 19)")

    subparsers.add_parser("push-standby", help="Push Standby Master Clock notebook")
    subparsers.add_parser("delete-fleet", help="Delete all 20 worker kernels to clean versions")
    subparsers.add_parser("status", help="Check status of all 20 worker nodes")

    p_cb = subparsers.add_parser("handle-callback", help="Handle incoming webhook callback")
    p_cb.add_argument("--payload", required=True, help="Callback JSON payload")

    args = parser.parse_args()

    if args.command == "push-fleet":
        push_fleet()
    elif args.command == "push-node":
        push_single_node(args.node_index)
    elif args.command == "push-standby":
        push_standby()
    elif args.command == "delete-fleet":
        delete_fleet()
    elif args.command == "status":
        check_status()
    elif args.command == "handle-callback":
        handle_callback(args.payload)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
