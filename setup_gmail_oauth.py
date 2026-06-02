"""
One-shot CLI to generate an OAuth token for the Gmail account that will be
polled on Hugging Face Spaces.

  python setup_gmail_oauth.py

What it does:
  1. Reads credentials.json (must be uploaded already via /gmail-setup or in
     config/gmail_credentials.json locally)
  2. Opens your browser to Google's consent screen
  3. Lets you sign in as the polling account (project2025aigov@gmail.com or
     any other Gmail) and grant access
  4. Captures the OAuth token
  5. Saves it to BOTH:
       - config/gmail_token.json on disk (used locally)
       - MongoDB collection 'secure_files' under id 'gmail_token'
         (used by Hugging Face — same Fernet key decrypts it)

After this, Hugging Face will start polling Gmail via the API on port 443.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from google_auth_oauthlib.flow import InstalledAppFlow

from database import get_settings, store_secure_file

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


def main():
    cfg = get_settings()
    creds_path = Path(cfg.gmail_credentials_file)
    if not creds_path.exists():
        print(f"ERROR: {creds_path} not found.")
        print("Upload credentials.json via http://localhost:8000/gmail-setup first.")
        sys.exit(1)

    token_path = Path(cfg.gmail_token_file)

    print("=" * 60)
    print(" OAuth setup — your browser will open in a moment")
    print("=" * 60)
    print()
    print("  When the Google sign-in screen appears, IMPORTANT:")
    print("  - Pick the Gmail account you want to POLL (e.g. project2025aigov@gmail.com)")
    print("  - NOT your everyday account if different")
    print("  - On the 'Google hasn't verified this app' page:")
    print("        Advanced -> Go to TESTFBDI (unsafe) -> Allow")
    print()
    input("Press ENTER when ready to open the browser… ")

    flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent")

    token_json = creds.to_json()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(token_json)
    print(f"\n[1] Token saved locally: {token_path} ({len(token_json)} bytes)")

    store_secure_file("gmail_token", token_json.encode())
    print("[2] Token also stored encrypted in MongoDB.")
    print()
    print("Hugging Face will pick it up on the next poll cycle (~60 s)")
    print("— no upload, no UI clicks needed.")


if __name__ == "__main__":
    main()
