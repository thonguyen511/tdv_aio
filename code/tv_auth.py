import os
import re
import time
import threading
import requests

# Refresh JWT every 3 hours (TradingView JWT TTL is 4 hours)
JWT_REFRESH_INTERVAL_SECONDS = 3 * 3600


def fetch_jwt_from_session(sessionid, sessionid_sign=""):
    """Exchanges the live in-memory session from Playwright login for a fresh 4-hour JWT (auth_token)."""
    if not sessionid:
        return "unauthorized_user_token"
    cookies = {"sessionid": sessionid}
    if sessionid_sign:
        cookies["sessionid_sign"] = sessionid_sign
    try:
        resp = requests.get(
            "https://www.tradingview.com/",
            cookies=cookies,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"},
            timeout=15
        )
        match = re.search(r'"auth_token":"([^"]+)"', resp.text)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"  [!] Warning refreshing JWT from in-memory session: {e}")
    return "unauthorized_user_token"


def playwright_auto_login(email, password, max_attempts=3):
    """Logs into TradingView inside the notebook using headless Playwright and extracts live JWT (auth_token)."""
    from playwright.sync_api import sync_playwright

    for attempt in range(1, max_attempts + 1):
        print(f"🔐 [Auto-Login Attempt {attempt}/{max_attempts}] Logging into TradingView as {email}...")
        browser = None
        try:
            with sync_playwright() as p:
                launch_args = ["--headless=new", "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
                try:
                    # Standard Playwright Chromium on Linux / Kaggle
                    browser = p.chromium.launch(headless=True, args=launch_args)
                except Exception:
                    # Fallback on Windows if only msedge is installed
                    browser = p.chromium.launch(channel="msedge", headless=False, args=launch_args)

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                page.goto("https://www.tradingview.com/accounts/signin/", timeout=30000)
                time.sleep(2)

                # Click "Email" option if username input is not yet visible
                if not page.locator('input[name="id_username"]').is_visible():
                    for sel in ["button[name='Email']", "span:has-text('Email')", "button:has-text('Email')"]:
                        try:
                            loc = page.locator(sel).first
                            if loc.is_visible(timeout=2000):
                                loc.click()
                                time.sleep(1)
                                break
                        except Exception:
                            pass

                page.wait_for_selector('input[name="id_username"]', timeout=10000)
                page.fill('input[name="id_username"]', email)
                page.fill('input[name="id_password"]', password)
                time.sleep(0.5)

                clicked = False
                for sub in ['button[type="submit"]', 'button:has-text("Log in")', 'button:has-text("Sign in")']:
                    try:
                        loc = page.locator(sub).first
                        if loc.is_visible(timeout=2000):
                            loc.click()
                            clicked = True
                            break
                    except Exception:
                        pass
                if not clicked:
                    page.keyboard.press("Enter")

                sessionid = None
                sessionid_sign = None
                for _ in range(20):
                    cookies = {c["name"]: c["value"] for c in context.cookies()}
                    if "sessionid" in cookies:
                        sessionid = cookies.get("sessionid", "")
                        sessionid_sign = cookies.get("sessionid_sign", "")
                        break
                    time.sleep(1)

                if sessionid:
                    jwt_token = fetch_jwt_from_session(sessionid, sessionid_sign)
                    if jwt_token == "unauthorized_user_token":
                        page.goto("https://www.tradingview.com/", timeout=20000)
                        html = page.content()
                        m = re.search(r'"auth_token":"([^"]+)"', html)
                        if m:
                            jwt_token = m.group(1)

                    browser.close()
                    if jwt_token and jwt_token != "unauthorized_user_token":
                        print(f"✅ [Auto-Login] Extracted live JWT for {email} ({jwt_token[:24]}...)")
                        return jwt_token, sessionid, sessionid_sign
                    else:
                        print(f"⚠️ [Auto-Login] Logged in as {email}, but auth_token was not found.")
                else:
                    body_snippet = ""
                    try:
                        body_snippet = page.inner_text("body")[:200].replace("\n", " ")
                    except Exception:
                        pass
                    print(f"⚠️ [Auto-Login] Attempt {attempt} did not complete login. Page: {body_snippet}")
                    browser.close()
        except Exception as e:
            print(f"⚠️ [Auto-Login] Error on attempt {attempt} for {email}: {e}")
            try:
                if browser:
                    browser.close()
            except Exception:
                pass

        if attempt < max_attempts:
            time.sleep(3 * attempt)

    return "unauthorized_user_token", "", ""


class TVTokenManager:
    """Thread-safe per-notebook JWT manager.
    Logs in with email + password via headless Playwright at startup to extract the exact JWT token,
    and automatically refreshes the 4-hour JWT token every 3 hours across the 11h45m Kaggle session."""

    def __init__(self, email="", password="", label="FREE", fallback_emails=None):
        self.email = (email or "").strip()
        self.password = (password or "").strip()
        self.fallback_emails = [e.strip() for e in (fallback_emails or []) if e and e.strip() and e.strip() != self.email]
        self.sessionid = ""
        self.sessionid_sign = ""
        self.label = label
        self.jwt_token = "unauthorized_user_token"
        self.last_refresh = 0.0
        self._lock = threading.Lock()
        self.refresh(force_login=True)

    def refresh(self, force_login=False):
        with self._lock:
            now = time.time()
            if not force_login and self.jwt_token != "unauthorized_user_token" and (now - self.last_refresh < JWT_REFRESH_INTERVAL_SECONDS):
                return self.jwt_token

            # Fast in-memory session refresh if already logged in during this VM run
            if not force_login and self.sessionid:
                jwt = fetch_jwt_from_session(self.sessionid, self.sessionid_sign)
                if jwt and jwt != "unauthorized_user_token":
                    self.jwt_token = jwt
                    self.last_refresh = time.time()
                    print(f"🔄 [{self.label}] Auto-refreshed 4h JWT token for {self.email}.")
                    return self.jwt_token

            # Primary path: Headless Playwright Auto-Login with email + password (plus fallback emails if primary fails)
            candidates = ([self.email] if self.email else []) + self.fallback_emails
            if candidates and self.password:
                for candidate_email in candidates[:4]:
                    jwt, sess, sign = playwright_auto_login(candidate_email, self.password)
                    if jwt and jwt != "unauthorized_user_token":
                        self.email = candidate_email
                        self.jwt_token = jwt
                        self.sessionid = sess
                        self.sessionid_sign = sign
                        self.last_refresh = time.time()
                        return self.jwt_token

            print(f"❌ [{self.label}] Warning: Could not obtain valid JWT token for {self.email}; falling back to unauthorized_user_token.")
            self.jwt_token = "unauthorized_user_token"
            self.last_refresh = time.time()
            return self.jwt_token

    def get_jwt(self):
        if time.time() - self.last_refresh >= JWT_REFRESH_INTERVAL_SECONDS:
            return self.refresh(force_login=False)
        return self.jwt_token
