import os
import sys
import time
from datetime import datetime, timezone, timedelta

try:
    from . import callback
except ImportError:
    import callback

MAX_SESSION_SECONDS = 42300  # 11 hours 45 minutes

def get_seconds_to_next_monday_utc():
    """Calculates exact floating point seconds until next Monday 00:00:00 UTC."""
    now = datetime.now(timezone.utc)
    # weekday(): Monday is 0, Sunday is 6
    days_ahead = 0 - now.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    next_monday = (now + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_remaining = (next_monday - now).total_seconds()
    return max(0.0, seconds_remaining), now, next_monday

def run_standby_clock():
    print("=" * 65)
    print("⏱️ KAGGLE STANDBY MASTER CLOCK INITIALIZING")
    print("=" * 65)

    seconds_to_monday, now_dt, next_mon_dt = get_seconds_to_next_monday_utc()
    hours_left = seconds_to_monday / 3600.0

    print(f"• Current UTC Time:       {now_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"• Target Monday UTC:      {next_mon_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"• Countdown Remaining:    {seconds_to_monday:.1f}s ({hours_left:.2f} hours)")
    print(f"• Max Single Session:     {MAX_SESSION_SECONDS}s (11.75 hours)")
    print("-" * 65)

    if seconds_to_monday > MAX_SESSION_SECONDS:
        print(f"⏳ Remaining time ({hours_left:.2f}h) exceeds single session limit (11.75h).")
        print(f"⏳ Sleeping for {MAX_SESSION_SECONDS}s, then re-arming standby via callback...")
        
        # Sleep in chunks to keep process responsive and print occasional heartbeats
        start_time = time.time()
        while time.time() - start_time < MAX_SESSION_SECONDS:
            time.sleep(min(60, MAX_SESSION_SECONDS - (time.time() - start_time)))
            
        print("\n[+] 11h45m standby session completed. Requesting next standby session...")
        callback.send_callback(
            status="REARM_STANDBY",
            phase="standby",
            message=f"Standby chunk completed. Still {hours_left - 11.75:.2f}h remaining until Monday."
        )
    else:
        print(f"🎯 Final stretch! Less than 11h45m remaining.")
        print(f"🎯 Sleeping PRECISELY {seconds_to_monday:.1f} seconds until Monday 00:00:00 UTC...")
        
        # Precise countdown sleep
        start_time = time.time()
        while time.time() - start_time < seconds_to_monday:
            time.sleep(min(10, max(0.1, seconds_to_monday - (time.time() - start_time))))
            
        final_now = datetime.now(timezone.utc)
        print(f"\n🔔 DING! Target reached: {final_now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("🚀 Sending TRIGGER_WEEKLY callback to launch the 20-node weekly fleet!")
        callback.send_callback(
            status="TRIGGER_WEEKLY",
            phase="weekly",
            message=f"Exact Monday 00:00 UTC reached ({final_now.isoformat()}). Triggering weekly run."
        )

if __name__ == "__main__":
    run_standby_clock()
