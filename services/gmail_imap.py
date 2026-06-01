"""
Gmail via IMAP + SMTP using a Gmail App Password (16-char).

No OAuth, no consent screen, no redirect URIs. Works on any server (HF Spaces,
Docker, Render, …). Users need:
  1. 2-Step Verification enabled on the Gmail account
  2. An App Password generated at https://myaccount.google.com/apppasswords
  3. Paste the 16-char password into Settings -> Gmail App Password
"""

from __future__ import annotations

import base64
import email
import imaplib
import logging
import os
import smtplib
import ssl
import uuid
from email import policy
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import BytesParser
from pathlib import Path

from database import get_settings

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SUPPORTED_EXT = {".csv", ".txt", ".zip"}
PROCESSED_LABEL = "FBDI_PROCESSED"


def _creds() -> tuple[str, str] | None:
    cfg = get_settings()
    user = (cfg.gmail_user or "").strip()
    pwd  = (cfg.gmail_app_password or "").strip().replace(" ", "")  # Google copies with spaces
    if not user or not pwd:
        return None
    return user, pwd


def app_password_available() -> bool:
    """
    True only when App Password is configured AND the host can actually reach
    IMAP/SMTP. Hugging Face Spaces blocks outbound ports 993 and 587, so we
    skip App Password there and let polling/sending fall through to OAuth.
    """
    # On HF, Google Cloud Run, and similar locked-down hosts, IMAP is blocked.
    # Detect HF specifically since it's the documented case.
    if os.environ.get("SPACE_ID") or os.environ.get("SPACE_HOST"):
        return False
    # Manual opt-out for other hosts that block these ports
    if os.environ.get("DISABLE_APP_PASSWORD", "").strip().lower() in ("1", "true", "yes"):
        return False
    return _creds() is not None


def test_app_password() -> tuple[bool, str]:
    """Try logging in to IMAP and report success / failure with a clear message."""
    c = _creds()
    if not c:
        return False, "Gmail address or App Password not configured."
    user, pwd = c
    try:
        with imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT,
                               ssl_context=ssl.create_default_context(),
                               timeout=15) as imap:
            imap.login(user, pwd)
            imap.select("INBOX")
            imap.logout()
        return True, f"IMAP login OK as {user}"
    except imaplib.IMAP4.error as e:
        msg = str(e)
        if "Invalid credentials" in msg or "AUTHENTICATIONFAILED" in msg:
            return False, ("Login rejected. Check that 2-Step Verification is enabled "
                           "on the account and that the App Password is correct "
                           "(remove any spaces).")
        return False, f"IMAP error: {msg[:200]}"
    except Exception as e:
        return False, f"Connection error: {e}"


# ── Inbox polling ─────────────────────────────────────────────────────────────

def fetch_unprocessed_messages() -> list[dict]:
    """
    Fetch new emails matching the subject filter that haven't been processed yet.
    Returns list of {message_id, sender, subject, attachments: [{file_name,
    file_bytes, file_type, file_size_bytes}]} dicts.
    """
    c = _creds()
    if not c:
        return []
    user, pwd = c
    cfg = get_settings()
    subject_kw = (cfg.gmail_subject_filter or "journal upload").strip()

    out = []
    try:
        with imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT,
                               ssl_context=ssl.create_default_context(),
                               timeout=15) as imap:
            imap.login(user, pwd)
            imap.select("INBOX")

            # SEARCH: case-insensitive subject + no FBDI_PROCESSED Gmail label
            # Use Gmail's X-GM-RAW for native search query support
            search_q = f'subject:"{subject_kw}" has:attachment -label:{PROCESSED_LABEL}'
            typ, data = imap.search(None, "X-GM-RAW", f'"{search_q}"')
            if typ != "OK":
                return []
            ids = data[0].split()
            if not ids:
                return []
            logger.info("IMAP: found %d unprocessed email(s) matching '%s'",
                        len(ids), subject_kw)

            for mid in ids:
                typ, msg_data = imap.fetch(mid, "(RFC822)")
                if typ != "OK" or not msg_data:
                    continue
                raw = msg_data[0][1]
                msg = BytesParser(policy=policy.default).parsebytes(raw)

                sender  = str(msg.get("From", ""))
                subject = str(msg.get("Subject", ""))
                message_id_hdr = str(msg.get("Message-ID", "")) or mid.decode()

                attachments = []
                for part in msg.iter_attachments():
                    fname = part.get_filename()
                    if not fname:
                        continue
                    ext = Path(fname).suffix.lower()
                    if ext not in SUPPORTED_EXT:
                        continue
                    file_bytes = part.get_payload(decode=True)
                    if not file_bytes:
                        continue
                    attachments.append({
                        "file_name":       fname,
                        "file_bytes":      file_bytes,
                        "file_path":       fname,  # virtual — lives in MongoDB
                        "file_type":       ext.lstrip("."),
                        "file_size_bytes": len(file_bytes),
                    })

                if attachments:
                    out.append({
                        "imap_uid":   mid.decode() if isinstance(mid, bytes) else str(mid),
                        "message_id": message_id_hdr,
                        "sender":     sender,
                        "subject":    subject,
                        "attachments": attachments,
                    })

            imap.logout()
    except Exception as e:
        logger.warning("IMAP poll failed: %s", e)
    return out


def mark_processed(imap_uid: str):
    """Apply the FBDI_PROCESSED Gmail label and remove from INBOX."""
    c = _creds()
    if not c: return
    user, pwd = c
    try:
        with imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT,
                               ssl_context=ssl.create_default_context(),
                               timeout=15) as imap:
            imap.login(user, pwd)
            imap.select("INBOX")
            # Add Gmail label via STORE on the X-GM-LABELS attribute
            imap.uid("STORE", imap_uid, "+X-GM-LABELS", PROCESSED_LABEL)
            # Mark as read
            imap.uid("STORE", imap_uid, "+FLAGS", "(\\Seen)")
            imap.logout()
    except Exception as e:
        logger.warning("IMAP mark_processed failed for %s: %s", imap_uid, e)


# ── Outbound SMTP ─────────────────────────────────────────────────────────────

def send_email(to: str, subject: str, html: str,
               attachments: list[str] | None = None) -> bool:
    """Send via Gmail SMTP relay using the App Password."""
    c = _creds()
    if not c:
        logger.warning("send_email skipped — no Gmail App Password configured")
        return False
    user, pwd = c
    try:
        msg = MIMEMultipart("mixed")
        msg["From"]    = user
        msg["To"]      = to
        msg["Subject"] = subject
        msg.attach(MIMEText(html, "html"))
        for att in (attachments or []):
            p = Path(att)
            if not p.exists():
                continue
            with open(p, "rb") as f:
                part = MIMEApplication(f.read(), Name=p.name)
            part["Content-Disposition"] = f'attachment; filename="{p.name}"'
            msg.attach(part)

        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ctx)
            smtp.ehlo()
            smtp.login(user, pwd)
            smtp.sendmail(user, [to], msg.as_string())
        logger.info("SMTP email sent to %s: %s", to, subject[:60])
        return True
    except Exception as e:
        logger.error("SMTP send failed: %s", e)
        return False
