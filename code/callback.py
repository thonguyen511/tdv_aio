import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone

DEFAULT_REPO = "thonguyen511/tdv_aio"

def get_github_token():
    # 1. Environment variable (injected by fleet_manager into private Kaggle kernel runner)
    token = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
    if token and token.strip():
        return token.strip()
        
    # 2. Kaggle secrets (if attached in Kaggle environment)
    try:
        from kaggle_secrets import UserSecretsClient
        user_secrets = UserSecretsClient()
        token = user_secrets.get_secret("GH_PAT")
        if token and token.strip():
            return token.strip()
    except Exception:
        pass
        
    return None


def send_callback(status, node_index=None, phase=None, message=None, repo=DEFAULT_REPO, max_retries=5):
    """
    Sends a repository_dispatch event to GitHub API with exponential backoff retries.
    
    status: 'TIMEOUT', 'DONE', 'ERROR', 'RATE_LIMITED', 'REARM_STANDBY', 'TRIGGER_WEEKLY'
    node_index: int (0 to 19) or None
    phase: 'initial', 'weekly', 'standby' or None
    message: optional string description
    """
    token = get_github_token()
    if not token:
        print("[!] Warning: No GH_PAT token detected. Callback cannot be sent to GitHub.")
        return False

    url = f"https://api.github.com/repos/{repo}/dispatches"
    
    payload = {
        "event_type": "node_callback",
        "client_payload": {
            "status": str(status).upper(),
            "node_index": node_index,
            "phase": phase,
            "week_m": os.environ.get("WEEK_M", ""),
            "message": message or "",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

    req_data = json.dumps(payload).encode("utf-8")

    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(
            url,
            data=req_data,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "KaggleFleetSupervisor/2.0",
                "Content-Type": "application/json"
            },
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status in (200, 204):
                    print(f"[+] Successfully sent callback '{status}' (Node {node_index}) to GitHub Actions.")
                    return True
                else:
                    print(f"[!] GitHub API responded with status {resp.status} (Attempt {attempt}/{max_retries})")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            print(f"[!] HTTP Error sending callback ({e.code}, Attempt {attempt}/{max_retries}): {body}")
            if e.code in (401, 403, 404, 422):
                return False
        except Exception as e:
            print(f"[!] Error sending callback to GitHub (Attempt {attempt}/{max_retries}): {e}")

        if attempt < max_retries:
            wait_s = min(30, 3 * (2 ** (attempt - 1)))
            print(f"    Retrying callback in {wait_s}s...")
            time.sleep(wait_s)

    return False

def main():
    parser = argparse.ArgumentParser(description="Send callback event to GitHub Actions.")
    parser.add_argument("--status", required=True, help="Callback status (TIMEOUT, DONE, ERROR, RATE_LIMITED, REARM_STANDBY, TRIGGER_WEEKLY)")
    parser.add_argument("--node", type=int, default=None, help="Node index (0-19)")
    parser.add_argument("--phase", type=str, default=None, help="Phase (initial, weekly, standby)")
    parser.add_argument("--message", type=str, default="", help="Optional message")
    parser.add_argument("--repo", type=str, default=DEFAULT_REPO, help="GitHub repo owner/name")
    args = parser.parse_args()

    success = send_callback(
        status=args.status,
        node_index=args.node,
        phase=args.phase,
        message=args.message,
        repo=args.repo
    )
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
