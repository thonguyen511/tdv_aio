import os
import sys
import time
import subprocess
import threading
import argparse

# 11 hours 45 minutes in seconds: (11 * 3600) + (45 * 60) = 42,300 seconds
DEFAULT_TIMEOUT_SECONDS = 42300

try:
    from . import callback
except ImportError:
    import callback

def read_phase_from_initial_txt(repo_dir="."):
    """Reads initial.txt to decide whether we are running initial.py or weekly.py."""
    candidates = [
        os.path.join(repo_dir, "initial.txt"),
        "initial.txt",
        os.path.join(os.path.dirname(__file__), "..", "initial.txt")
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    val = f.read().strip().lower()
                    if val == "false":
                        return "weekly"
                    elif val == "true":
                        return "initial"
            except Exception as e:
                print(f"[!] Warning reading {p}: {e}")
    # Default to initial if file is missing or ambiguous
    return "initial"

def run_supervised_node(node_index=0, repo_dir=".", timeout_seconds=DEFAULT_TIMEOUT_SECONDS, forced_phase=None):
    """
    Supervises the execution of initial.py or weekly.py with an 11h45m hard stop.
    """
    code_dir = os.path.dirname(os.path.abspath(__file__))
    phase = forced_phase or read_phase_from_initial_txt(repo_dir)
    target_script_name = "initial.py" if phase == "initial" else "weekly.py"
    target_script = os.path.join(code_dir, target_script_name)

    if not os.path.exists(target_script):
        print(f"[!] Error: Target script not found at {target_script}")
        callback.send_callback(status="ERROR", node_index=node_index, phase=phase, message="Script file missing")
        return False

    print("=" * 65)
    print(f"⏱️ TIMER SUPERVISOR ACTIVATED")
    print(f"• Node Index: {node_index}")
    print(f"• Phase: {phase.upper()} ({target_script_name})")
    print(f"• Hard Stop Timeout: {timeout_seconds}s ({timeout_seconds / 3600:.2f} hours)")
    print("=" * 65)

    env = os.environ.copy()
    env["NOTEBOOK_INDEX"] = str(node_index)

    start_time = time.time()
    timed_out = [False]

    proc = subprocess.Popen(
        [sys.executable, target_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1
    )

    # Output streaming thread
    def stream_output():
        try:
            for line in iter(proc.stdout.readline, ''):
                sys.stdout.write(line)
                sys.stdout.flush()
        except Exception:
            pass

    out_thread = threading.Thread(target=stream_output, daemon=True)
    out_thread.start()

    # Wait loop with periodic status reporting
    check_interval = 30  # check every 30 seconds
    while True:
        ret = proc.poll()
        if ret is not None:
            # Process terminated on its own
            break

        elapsed = time.time() - start_time
        if elapsed >= timeout_seconds:
            timed_out[0] = True
            print(f"\n[!] 🛑 REACHED {timeout_seconds}s (11h45m) TIMEOUT LIMIT!")
            print(f"[!] Sending SIGTERM to gracefully stop {target_script_name}...")
            proc.terminate()
            try:
                proc.wait(timeout=45)
            except subprocess.TimeoutExpired:
                print("[!] Process still alive after 45s. Forcing kill...")
                proc.kill()
            break

        time.sleep(min(check_interval, max(1, timeout_seconds - elapsed)))

    out_thread.join(timeout=3)
    elapsed_total = time.time() - start_time
    print(f"\n[i] Session execution finished. Total duration: {elapsed_total:.1f}s")

    # Evaluate outcome and trigger appropriate callback
    if timed_out[0]:
        print(f"[+] Notifying GitHub Actions of TIMEOUT callback (Node {node_index} needs continuation)...")
        callback.send_callback(
            status="TIMEOUT",
            node_index=node_index,
            phase=phase,
            message=f"Session stopped at {elapsed_total:.0f}s. Requesting resume."
        )
        return True
    elif proc.returncode == 0:
        print(f"[+] Workload 100% COMPLETED successfully! Notifying GitHub Actions...")
        callback.send_callback(
            status="DONE",
            node_index=node_index,
            phase=phase,
            message="Node completed all assigned files."
        )
        return True
    else:
        print(f"[!] Process exited with non-zero exit code: {proc.returncode}")
        callback.send_callback(
            status="ERROR",
            node_index=node_index,
            phase=phase,
            message=f"Process crashed with exit code {proc.returncode}"
        )
        return False

def main():
    parser = argparse.ArgumentParser(description="Run initial.py or weekly.py with 11h45m supervisor.")
    parser.add_argument("--node", type=int, default=int(os.environ.get("NOTEBOOK_INDEX", 0)), help="Node index (0-19)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Timeout in seconds (default: 42300)")
    parser.add_argument("--repo-dir", type=str, default=".", help="Root of tdv_aio repository")
    parser.add_argument("--phase", type=str, choices=["initial", "weekly"], default=None, help="Force phase")
    args = parser.parse_args()

    success = run_supervised_node(
        node_index=args.node,
        repo_dir=args.repo_dir,
        timeout_seconds=args.timeout,
        forced_phase=args.phase
    )
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
