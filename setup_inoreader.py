"""
setup_inoreader.py — Authenticate with InnoReader via ClientLogin.

Run this script once:
    python setup_inoreader.py

It exchanges your InnoReader email + password for an auth token and
prints the value to add to your .env as INOREADER_TOKEN.

Prerequisites:
  - INOREADER_APP_ID, INOREADER_APP_KEY, INOREADER_USERNAME,
    INOREADER_PASSWORD must be set in legaltech_weeklydigest.env
"""
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "legaltech_weeklydigest.env", override=True)

import requests
import config

CLIENT_LOGIN_URL = "https://www.inoreader.com/accounts/ClientLogin"


def main() -> None:
    username = config._optional("INOREADER_USERNAME")
    password = config._optional("INOREADER_PASSWORD")

    if not username or not password:
        print("ERROR: INOREADER_USERNAME and INOREADER_PASSWORD must be set in your .env file.")
        sys.exit(1)

    print("\n=== InnoReader ClientLogin ===\n")
    print(f"Authenticating as: {username}")

    resp = requests.post(
        CLIENT_LOGIN_URL,
        data={
            "Email": username,
            "Passwd": password,
        },
        headers={
            "AppId": config.INOREADER_APP_ID,
            "AppKey": config.INOREADER_APP_KEY,
        },
        timeout=30,
    )

    if not resp.ok:
        print(f"ERROR: Authentication failed ({resp.status_code}):\n{resp.text}")
        sys.exit(1)

    # Response is plain text: "SID=...\nLSID=...\nAuth=..."
    token = None
    for line in resp.text.splitlines():
        if line.startswith("Auth="):
            token = line.split("=", 1)[1].strip()
            break

    if not token:
        print(f"ERROR: Could not find Auth token in response:\n{resp.text}")
        sys.exit(1)

    print("\n✓ Success! Add this to your legaltech_weeklydigest.env:\n")
    print(f"INOREADER_TOKEN={token}")
    print()


if __name__ == "__main__":
    main()
