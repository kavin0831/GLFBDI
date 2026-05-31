"""
Gmail API service — OAuth2 authentication + send/receive operations.
"""

from __future__ import annotations

import base64
import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from database import get_settings, get_secure_file

logger = logging.getLogger(__name__)


def _scopes() -> list[str]:
    return ["https://www.googleapis.com/auth/gmail.modify"]


def get_gmail_service():
    """Build authenticated Gmail API service."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    cfg = get_settings()
    creds = None
    token_path = Path(cfg.gmail_token_file)
    creds_path = Path(cfg.gmail_credentials_file)

    if not creds_path.exists():
        stored = get_secure_file("gmail_credentials")
        if stored:
            creds_path.parent.mkdir(parents=True, exist_ok=True)
            creds_path.write_bytes(stored)
            logger.info("Restored gmail_credentials from MongoDB")
        else:
            raise FileNotFoundError(
                f"Gmail credentials file not found: {creds_path}\n"
                "Please complete Gmail Setup in the Settings page."
            )

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), _scopes())

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), _scopes())
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def gmail_available() -> bool:
    """Check if Gmail credentials exist on disk or in MongoDB."""
    try:
        cfg = get_settings()
        if Path(cfg.gmail_credentials_file).exists():
            return True
        return get_secure_file("gmail_credentials") is not None
    except Exception:
        return False


def _get_or_create_label(service, name: str) -> str:
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    for lbl in labels:
        if lbl["name"] == name:
            return lbl["id"]
    return service.users().labels().create(userId="me", body={"name": name}).execute()["id"]


def fetch_unprocessed_emails(service) -> list[dict]:
    cfg = get_settings()
    q = f'subject:"{cfg.gmail_subject_filter}" has:attachment -label:FBDI_PROCESSED'
    result = service.users().messages().list(userId="me", q=q).execute()
    return result.get("messages", [])


# Only .zip, .csv, .txt are accepted by the pipeline (matches manual upload restriction)
SUPPORTED_EXT = {".csv", ".txt", ".zip"}


def download_attachments(service, message_id: str, dest_dir: str = "") -> list[dict]:
    """
    Fetch attachments from a Gmail message.
    Returns a list of dicts with file_name, file_bytes, file_type, file_size_bytes, sender, subject.
    Nothing is written to local disk — bytes flow straight to MongoDB in the caller.
    """
    msg = service.users().messages().get(userId="me", id=message_id, format="full").execute()
    payload = msg["payload"]
    sender, subject = "", ""
    for h in payload.get("headers", []):
        if h["name"].lower() == "from":
            sender = h["value"]
        elif h["name"].lower() == "subject":
            subject = h["value"]

    parts = payload.get("parts", [payload])
    saved = []
    for part in parts:
        fname = part.get("filename", "")
        if not fname or Path(fname).suffix.lower() not in SUPPORTED_EXT:
            continue
        att_id = part["body"].get("attachmentId")
        if not att_id:
            continue
        data = service.users().messages().attachments().get(
            userId="me", messageId=message_id, id=att_id).execute()
        file_bytes = base64.urlsafe_b64decode(data["data"])
        saved.append({"file_name":       fname,
                      "file_bytes":      file_bytes,
                      "file_path":       fname,  # virtual name — file lives in MongoDB
                      "file_type":       Path(fname).suffix.lower().lstrip("."),
                      "file_size_bytes": len(file_bytes),
                      "sender":          sender,
                      "subject":         subject})
    return saved


def mark_processed(service, message_id: str):
    label_id = _get_or_create_label(service, "FBDI_PROCESSED")
    service.users().messages().modify(userId="me", id=message_id,
        body={"addLabelIds": [label_id], "removeLabelIds": ["INBOX"]}).execute()


def mark_failed(service, message_id: str):
    label_id = _get_or_create_label(service, "FBDI_FAILED")
    service.users().messages().modify(userId="me", id=message_id,
        body={"addLabelIds": [label_id]}).execute()


def send_email(to: str, subject: str, html: str, attachments: list[str] | None = None) -> bool:
    try:
        service = get_gmail_service()
        msg = MIMEMultipart("mixed")
        msg["to"] = to
        msg["subject"] = subject
        msg.attach(MIMEText(html, "html"))
        for att in (attachments or []):
            p = Path(att)
            if p.exists():
                with open(p, "rb") as f:
                    part = MIMEApplication(f.read(), Name=p.name)
                part["Content-Disposition"] = f'attachment; filename="{p.name}"'
                msg.attach(part)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.info("Email sent to %s: %s", to, subject[:60])
        return True
    except Exception as e:
        logger.error("Email send failed: %s", e)
        return False
