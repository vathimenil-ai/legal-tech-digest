"""
gmail_client.py — Create Gmail drafts via the Gmail API (OAuth 2.0).

One-time setup: run `python setup_gmail.py` to authorise and save credentials.
After setup, GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, and GMAIL_REFRESH_TOKEN
must be set in your .env file.
"""
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]


def _get_credentials() -> Credentials:
    """Build OAuth credentials from env vars, refreshing if necessary."""
    if not config.GMAIL_CLIENT_ID or not config.GMAIL_CLIENT_SECRET or not config.GMAIL_REFRESH_TOKEN:
        raise EnvironmentError(
            "Gmail OAuth credentials not configured. "
            "Run `python setup_gmail.py` and add the output to your .env file."
        )

    creds = Credentials(
        token=None,
        refresh_token=config.GMAIL_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=config.GMAIL_CLIENT_ID,
        client_secret=config.GMAIL_CLIENT_SECRET,
        scopes=SCOPES,
    )
    # Force a refresh to obtain a valid access token
    creds.refresh(Request())
    return creds


def _build_mime(
    to: str,
    subject: str,
    html_body: str,
    plain_body: str | None = None,
) -> str:
    """Build a base64url-encoded MIME message."""
    msg = MIMEMultipart("alternative")
    msg["To"] = to
    msg["Subject"] = subject

    if plain_body:
        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    return raw


def create_draft(
    subject: str,
    html_body: str,
    plain_body: str | None = None,
    to: str | None = None,
) -> str:
    """
    Create a Gmail draft.
    Returns the draft ID.

    Args:
        subject:    Email subject line.
        html_body:  HTML version of the email body.
        plain_body: Optional plain-text fallback.
        to:         Recipient address (defaults to GMAIL_USER from config).
    """
    recipient = to or config.GMAIL_USER
    logger.info("Creating Gmail draft to '%s' with subject: %s", recipient, subject)

    try:
        creds = _get_credentials()
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)

        raw = _build_mime(recipient, subject, html_body, plain_body)
        draft_body = {"message": {"raw": raw}}

        draft = service.users().drafts().create(userId="me", body=draft_body).execute()
        draft_id = draft["id"]
        logger.info("Draft created: id=%s", draft_id)
        return draft_id

    except HttpError as exc:
        logger.error("Gmail API error creating draft: %s", exc)
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    draft_id = create_draft(
        subject="Test draft from pipeline",
        html_body="<h1>Hello</h1><p>This is a test draft.</p>",
        plain_body="Hello\n\nThis is a test draft.",
    )
    print(f"Draft created: {draft_id}")
