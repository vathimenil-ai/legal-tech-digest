"""
setup_inoreader.py — One-time OAuth 2.0 authorisation for InnoReader.

Run this script once from the command line:
    python setup_inoreader.py

It will:
  1. Open a browser window to the InnoReader authorisation URL
  2. After you authorise, paste the redirect URL (or the code) back here
  3. Print the access_token and refresh_token to add to your .env file

Prerequisites:
  - INOREADER_APP_ID and INOREADER_APP_KEY must be in your .env
  - Your InnoReader app must have redirect URI: http://localhost:8080/callback
    (or the URI you choose below)
"""
import sys
import urllib.parse
import webbrowser

import requests
from dotenv import load_dotenv

load_dotenv()

import config

AUTH_URL = "https://www.inoreader.com/oauth2/auth"
TOKEN_URL = "https://www.inoreader.com/oauth2/token"
REDIRECT_URI = "http://localhost:8080/callback"   # must match your InnoReader app settings
SCOPES = "read write"


def main() -> None:
    if not config.INOREADER_APP_ID or not config.INOREADER_APP_KEY:
        print("ERROR: INOREADER_APP_ID and INOREADER_APP_KEY must be set in .env")
        sys.exit(1)

    # Step 1 — Build authorisation URL
    params = {
        "client_id": config.INOREADER_APP_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

    print("\n=== InnoReader OAuth Setup ===\n")
    print("Opening authorisation URL in your browser…")
    print(f"\n  {auth_url}\n")
    webbrowser.open(auth_url)

    # Step 2 — Get the authorisation code
    print(
        "After you authorise, InnoReader will redirect to:\n"
        f"  {REDIRECT_URI}?code=XXXXXX\n"
        "(The page may show an error — that's expected.)\n"
    )
    raw = input("Paste the full redirect URL (or just the code): ").strip()

    if raw.startswith("http"):
        parsed = urllib.parse.urlparse(raw)
        code = urllib.parse.parse_qs(parsed.query).get("code", [""])[0]
    else:
        code = raw

    if not code:
        print("ERROR: Could not extract authorisation code.")
        sys.exit(1)

    # Step 3 — Exchange for tokens
    print("\nExchanging code for tokens…")
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": config.INOREADER_APP_ID,
        "client_secret": config.INOREADER_APP_KEY,
    }, timeout=30)

    if not resp.ok:
        print(f"ERROR: Token exchange failed: {resp.status_code} {resp.text}")
        sys.exit(1)

    data = resp.json()
    access_token = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")

    print("\n✓ Success! Add these to your .env file:\n")
    print(f"INOREADER_ACCESS_TOKEN={access_token}")
    print(f"INOREADER_REFRESH_TOKEN={refresh_token}")
    print()


if __name__ == "__main__":
    main()
