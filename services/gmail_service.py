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
    # Fall back to defaults if MongoDB settings doc has these blank/missing —
    # otherwise Path("") becomes Path('.') and write_text() crashes with IsADirectoryError.
    _ct = (cfg.gmail_token_file or "config/gmail_token.json").strip() or "config/gmail_token.json"
    _cc = (cfg.gmail_credentials_file or "config/gmail_credentials.json").strip() or "config/gmail_credentials.json"
    token_path = Path(_ct)
    creds_path = Path(_cc)
    # Don't let a path that points to an existing directory through
    if token_path.is_dir(): token_path = Path("config/gmail_token.json")
    if creds_path.is_dir(): creds_path = Path("config/gmail_credentials.json")

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

    # Token: try disk first, fall back to MongoDB (HF Spaces rebuilds wipe the disk)
    if not token_path.exists():
        stored_token = get_secure_file("gmail_token")
        if stored_token:
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_bytes(stored_token)
            logger.info("Restored gmail_token from MongoDB")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), _scopes())

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json())
            # Also persist refreshed token to MongoDB so next container restart sees it
            try:
                from database import store_secure_file as _store
                _store("gmail_token", token_path.read_bytes())
            except Exception:
                pass
        else:
            # No valid token and refresh impossible — fall back to interactive flow.
            # This needs a browser on the server, which doesn't exist in headless
            # containers (HF Spaces, Docker). Tell the user clearly what to do.
            import os as _os
            if _os.environ.get("SPACE_ID") or (_os.name == "posix" and not _os.environ.get("DISPLAY")):
                raise RuntimeError(
                    "Cannot launch OAuth browser on this headless server. "
                    "Run the app locally once, finish 'Authorize Gmail' there, "
                    "then upload the generated config/gmail_token.json file "
                    "via /gmail-setup → 'Upload OAuth Token'."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), _scopes())
            creds = flow.run_local_server(port=0)
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def gmail_available() -> bool:
    """Check if any Gmail integration is configured — App Password OR OAuth."""
    try:
        # App Password mode (preferred for HF Spaces / headless deploys)
        from services.gmail_imap import app_password_available
        if app_password_available():
            return True
    except Exception:
        pass
    try:
        cfg = get_settings()
        if Path(cfg.gmail_credentials_file).exists():
            return True
        return get_secure_file("gmail_credentials") is not None
    except Exception:
        return False


def send_email(to: str, subject: str, html: str,
               attachments: list[str] | None = None) -> bool:
    """
    Send a notification email. Routes through Gmail App Password (SMTP) if
    configured, otherwise falls back to OAuth Gmail API.
    """
    try:
        from services.gmail_imap import app_password_available, send_email as _smtp_send
        if app_password_available():
            return _smtp_send(to, subject, html, attachments)
    except Exception as e:
        logger.warning("App-password send failed, will try OAuth: %s", e)
    # Fall through to original OAuth path defined below
    return _send_email_oauth(to, subject, html, attachments)


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


def _send_email_oauth(to: str, subject: str, html: str,
                      attachments: list[str] | None = None) -> bool:
    """OAuth-mode send (original implementation). Called from the top-level
    send_email() above only when App Password isn't configured."""
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
        logger.info("Email sent to %s via OAuth: %s", to, subject[:60])
        return True
    except Exception as e:
        logger.error("OAuth email send failed: %s", e)
        return False
