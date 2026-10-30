import os
import re
import time
import threading
import requests

# Refresh JWT every 3 hours (TradingView JWT TTL is 4 hours)
JWT_REFRESH_INTERVAL_SECONDS = 3 * 3600

# Pre-authenticated session fallback map (used if TradingView /accounts/signin/ challenges headless login with reCAPTCHA)
FALLBACK_SESSIONS = {
    "teddy6908ba@6n.moonvf.com": ("nyd2xhhge1eqe1nz7vlt56bjq7gii7rv", "v3:HN1KRBXurQgBil6iUvZ/VFBTmRGD/X+2PZ9HHLn1sso="),
    "maureen832c0a@as.firegameplay.com": ("6oju8gg72zx3inpzdam4hg1owv9b7b4v", "v3:IlDG6eb3hNM1MeKfrhkD1SC59Yb3pyqf2DE9LVJ4QtY="),
    "alexina832b5b@m.firegameplay.com": ("r68mmxpvvg6neoc5azk0zubo3q9aj058", "v3:0kmDxH+xdl8DKWy+vhLcSUcw4fAqC6L47rtfJ3K9DNs="),
    "hartman82dcb8@3z.firegameplay.com": ("2xx7a6j9cmfpuq5w5aff9eoum6kccui8", "v3:dojiB43SFWBHWYQgBG7EtC44iLXOMqpfOcx5hKfPrE8="),
    "thi842901@5zj.firegameplay.com": ("8zdncz5k1bdxrkx4fc5i4aeng99xypgg", "v3:gji0jmijT6/fvTy2QK7vWX+wJ9xMYtjGMKHi4Vk+370="),
    "thi842992@yc.foodhz.com": ("m56urgnz31kkubfu4zyrqi0wfhkwzyxh", "v3:76wkQm2BMYNOD9ru/isHX2ZuqEOWaXibVwC7U+G3XBw="),
    "jos84347a@1p.firegameplay.com": ("36906qz62lf46hm5e88gm44qm7i3ooqu", "v3:pFwesXZ8uWB4hh5asBEOCyfIp77OQ9CWCMn6yS6ZGy8="),
    "heidie8434e4@dnl.firegameplay.com": ("mutvuw4vczaqmpvgaih4gw5350lns4kb", "v3:2xeMpVLXD2lkDXKHl3eT2JYQzUQGTLVDoENt1+s+Nh4="),
    "antonia843551@wn.foodhz.com": ("jn8enyc1lt2r656lh15u1qb4340al4r2", "v3:2P5aycMG9Sf3VrvYdDtbMssQLJsrb9EzaOUGvoo0qns="),
    "bronny843591@75.firegameplay.com": ("r2lauc0jxqri7c9kezdoy6t6e71opkay", "v3:2b/0b4MomCx+76tmfF3uQItmh8MQK2GKymuXsKqh9dQ="),
    "yablon8c0a71@pr.prominentghost.com": ("od5mt2i8lo5mg4k4jie4cxneu89kl3kr", "v3:uhpG+CL/Z8eBuOVWlqqFYu3Yupq0j+h4wnjeQe6NlaE="),
    "sydelle8c0e8b@ut.prominentghost.com": ("on8jljxtijcz08nxvtdsafnvpgpca6wu", "v3:zdrSEQxqSJdqOcRxWD8xLajJcgXsyh1s9CgDXQsnZtc="),
    "rosalee8c0fbd@hy.26ai.art": ("5m1xyo8gi0omogyef3roflrteexrt3zv", "v3:pfgppWB0cO4xDYcI1Y6y/mY6rhFOklANzNSEtnUQXdI="),
    "poland8c11e8@pp.26ai.art": ("vxvdynua8osiyf0fwfn2x2yvy5q8n8cs", "v3:eI4Mg4fXQGTAJ1SJ9rlxiskSr+ngc3on66Nd0IbmTr4="),
    "damita8c1277@d3.prominentghost.com": ("08sqlp86hd3bduscbjv159tq0ds9adzt", "v3:OqBdhTW3DSjVzJRjH0mSHUYdStCyrvZ3MoXJmtmD3I0="),
    "harlie8c12dd@vb.imagesthere.com": ("qatorul4xe9ah6rzb1vkk5xtuwt7uygq", "v3:FOhRQDa0O2Oo6IrnHcpizxSSlw4CgFdOtYs4kf/25wE="),
    "nerita8c1384@ouk.imagesthere.com": ("c6e83lfw9hop1hld0qsft4634zah8ucp", "v3:mE0TcSuxquhCXT7ysQaGtiq+vz2IGLwSj7NyKiWPk5k="),
    "melamie8c1414@ie.26ai.art": ("rdu9xyuda4c2sawsigzowj8xyynkh1oj", "v3:1fhMVrppO2SEAihS7O/BB7UOE5Q+WUBwgwVa1AJP9Tk="),
    "amersham3305ed@e2.paxeluni.com": ("6b51nxhkk9lvl54o7lrtnz86yudctaej", "v3:ZXq8jO1JJr3/m3IDN/8XizhFNhe+ytm6nBCLouDXagg="),
    "thi8c155f@xx.26ai.art": ("xa5a7083pvwu57qrsioayf3mw4ofnevq", "v3:gg1koNMDyACH4F4EDuNRQtwYNnNe8Ss6NrFkYObVpM0="),
}


def fetch_jwt_from_session(sessionid, sessionid_sign=""):
    """Exchanges the live in-memory session for a fresh 4-hour JWT (auth_token)."""
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
        print(f"  [!] Warning refreshing JWT from session: {e}")
    return "unauthorized_user_token"


def playwright_auto_login(email, password, max_attempts=2):
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
                captcha_detected = False
                for _ in range(15):
                    cookies = {c["name"]: c["value"] for c in context.cookies()}
                    if "sessionid" in cookies:
                        sessionid = cookies.get("sessionid", "")
                        sessionid_sign = cookies.get("sessionid_sign", "")
                        break
                    try:
                        btxt = page.inner_text("body").lower()
                        if "captcha" in btxt or "not a robot" in btxt:
                            captcha_detected = True
                            break
                    except Exception:
                        pass
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
                    if captcha_detected:
                        print(f"ℹ️ [Auto-Login] CAPTCHA challenge detected for {email}; skipping further browser attempts.")
                        break
        except Exception as e:
            print(f"⚠️ [Auto-Login] Error on attempt {attempt} for {email}: {e}")
            try:
                if browser:
                    browser.close()
            except Exception:
                pass

        if attempt < max_attempts:
            time.sleep(2 * attempt)

    return "unauthorized_user_token", "", ""


class TVTokenManager:
    """Thread-safe per-notebook JWT manager.
    Logs in with email + password via headless Playwright at startup to extract the exact JWT token,
    falls back to verified session JWT extraction if TradingView triggers reCAPTCHA,
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

            # 1. Fast in-memory session refresh if already logged in during this VM run
            if not force_login and self.sessionid:
                jwt = fetch_jwt_from_session(self.sessionid, self.sessionid_sign)
                if jwt and jwt != "unauthorized_user_token":
                    self.jwt_token = jwt
                    self.last_refresh = time.time()
                    print(f"🔄 [{self.label}] Auto-refreshed 4h JWT token for {self.email}.")
                    return self.jwt_token

            # 2. Primary path: Headless Playwright Auto-Login with email + password
            if self.email and self.password:
                jwt, sess, sign = playwright_auto_login(self.email, self.password)
                if jwt and jwt != "unauthorized_user_token":
                    self.jwt_token = jwt
                    self.sessionid = sess
                    self.sessionid_sign = sign
                    self.last_refresh = time.time()
                    return self.jwt_token

            # 3. CAPTCHA-proof session exchange for the assigned email
            if self.email in FALLBACK_SESSIONS:
                fb_sess, fb_sign = FALLBACK_SESSIONS[self.email]
                jwt = fetch_jwt_from_session(fb_sess, fb_sign)
                if jwt and jwt != "unauthorized_user_token":
                    self.jwt_token = jwt
                    self.sessionid = fb_sess
                    self.sessionid_sign = fb_sign
                    self.last_refresh = time.time()
                    print(f"✅ [{self.label}] Extracted live 4h JWT for {self.email} via verified session ({jwt[:24]}...)")
                    return self.jwt_token

            # 4. Fallback to other pool emails (Playwright or verified session)
            for candidate_email in self.fallback_emails[:4]:
                if candidate_email in FALLBACK_SESSIONS:
                    fb_sess, fb_sign = FALLBACK_SESSIONS[candidate_email]
                    jwt = fetch_jwt_from_session(fb_sess, fb_sign)
                    if jwt and jwt != "unauthorized_user_token":
                        self.email = candidate_email
                        self.jwt_token = jwt
                        self.sessionid = fb_sess
                        self.sessionid_sign = fb_sign
                        self.last_refresh = time.time()
                        print(f"✅ [{self.label}] Extracted live 4h JWT via fallback account {candidate_email} ({jwt[:24]}...)")
                        return self.jwt_token

            print(f"❌ [{self.label}] Warning: Could not obtain valid JWT token for {self.email}; falling back to unauthorized_user_token.")
            self.jwt_token = "unauthorized_user_token"
            self.last_refresh = time.time()
            return self.jwt_token

    def get_jwt(self):
        if time.time() - self.last_refresh >= JWT_REFRESH_INTERVAL_SECONDS:
            return self.refresh(force_login=False)
        return self.jwt_token
