"""
setup_gmail.py — One-time OAuth 2.0 authorisation for the Gmail API.

Run this script once:
    python setup_gmail.py

It will:
  1. Open a browser window for Google OAuth consent
  2. After you approve, exchange the code for tokens
  3. Print the values to add to your .env file

Prerequisites (one-time Google Cloud setup):
  1. Go to https://console.cloud.google.com/
  2. Create a project (or select existing)
  3. Enable the Gmail API
  4. Create OAuth 2.0 credentials → Desktop app
  5. Download the client_secret JSON — you only need client_id and client_secret
  6. Add GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET to your .env

Authorised redirect URI to add in the Cloud Console:
    http://localhost:8080/
"""
import sys
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import requests
from dotenv import load_dotenv

load_dotenv()

import config

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = "http://localhost:8080/"
SCOPES = "https://www.googleapis.com/auth/gmail.compose"

_received_code: list[str] = []


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        code = urllib.parse.parse_qs(parsed.query).get("code", [""])[0]
        if code:
            _received_code.append(code)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Authorised! You can close this tab.</h2>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h2>Error: no code in URL.</h2>")

    def log_message(self, *args: object) -> None:
        pass  # suppress server logs


def _run_server() -> None:
    server = HTTPServer(("localhost", 8080), _Handler)
    server.handle_request()  # handle exactly one request, then stop


def main() -> None:
    if not config.GMAIL_CLIENT_ID or not config.GMAIL_CLIENT_SECRET:
        print(
            "ERROR: GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET must be set in .env\n"
            "See the module docstring for Google Cloud Console setup instructions."
        )
        sys.exit(1)

    # Step 1 — Build authorisation URL
    params = {
        "client_id": config.GMAIL_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "access_type": "offline",
        "prompt": "consent",   # force refresh_token to be returned
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

    print("\n=== Gmail OAuth Setup ===\n")
    print("Starting local callback server on port 8080…")
    server_thread = Thread(target=_run_server, daemon=True)
    server_thread.start()

    print("Opening authorisation URL in your browser…")
    print(f"\n  {auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for Google to redirect back… (approve in the browser window)")
    server_thread.join(timeout=120)

    if not _received_code:
        print("\nERROR: Did not receive a code within 120s. Run the script again.")
        sys.exit(1)

    code = _received_code[0]

    # Step 2 — Exchange for tokens
    print("\nExchanging code for tokens…")
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": config.GMAIL_CLIENT_ID,
        "client_secret": config.GMAIL_CLIENT_SECRET,
    }, timeout=30)

    if not resp.ok:
        print(f"ERROR: Token exchange failed: {resp.status_code} {resp.text}")
        sys.exit(1)

    data = resp.json()
    refresh_token = data.get("refresh_token", "")

    if not refresh_token:
        print(
            "ERROR: No refresh_token in response. "
            "Make sure 'access_type=offline' and 'prompt=consent' are set.\n"
            f"Full response: {data}"
        )
        sys.exit(1)

    print("\n✓ Success! Add this to your .env file:\n")
    print(f"GMAIL_REFRESH_TOKEN={refresh_token}")
    print()


if __name__ == "__main__":
    main()
