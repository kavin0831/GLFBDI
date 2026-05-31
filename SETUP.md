# Oracle Fusion GL FBDI Automation — Setup Guide

## Quick Start

```
cd oracle_fbdi_app
pip install -r requirements.txt
cp .env.example .env       # then edit .env and set MONGO_URI
python app.py
```

Open http://localhost:8000

---

## Requirements

- Python 3.11+
- A MongoDB Atlas cluster (or local MongoDB)
- Internet access for the first run only (downloads ~22 MB sentence-transformers model from HuggingFace)
- No Redis/Celery, no API keys other than what you configure in Settings

---

## First-Run Configuration

After starting the app, visit **http://localhost:8000/settings** and fill in:

| Setting | Where it goes |
|---|---|
| Fusion URL | Your Oracle Fusion ERP Cloud base URL |
| Username | Your Oracle Fusion user with ERP Integration access |
| Password | Encrypted with Fernet before being written to MongoDB |
| Ledger | Primary ledger name from your COA |
| Document Account | Oracle UCM document account (typically `fin$/generalLedger$/import$`) |
| Notification Email | Address that receives success / failure / approval emails |
| Gmail Subject Filter | Keyword the auto-poller looks for in inbound emails |

Sensitive values (password) are encrypted at rest. Nothing is ever written to local disk; the entire state lives in MongoDB.

---

## Environment Variables

Set in `.env` (gitignored). See `.env.example` for the template.

| Var | Required | Default | Purpose |
|-----|----------|---------|---------|
| `MONGO_URI` | yes | `mongodb://localhost:27017/` | Atlas connection string |
| `MONGO_DB` | no | `oracle_fbdi` | Database name |
| `FERNET_KEY` | no | auto-generated into `config/secret.key` | Override symmetric encryption key |

---

## Gmail Setup (Optional — for auto-processing emailed journal files)

Gmail is **not required** — you can always upload files manually via the Upload page.

### Step 1 — Create Google Cloud credentials

1. Go to https://console.cloud.google.com
2. Create a project → Enable the **Gmail API**
3. OAuth Consent Screen → External → add your Gmail as Test User
4. Credentials → Create → **OAuth 2.0 Client ID** → **Desktop App**
5. Download the JSON

### Step 2 — Upload credentials

- Go to http://localhost:8000/gmail-setup
- Upload the downloaded JSON file (also stored encrypted in MongoDB)

### Step 3 — Authorize

- Click **Authorize Gmail** — a browser window opens for Google login
- Grant access
- The token is saved to `config/gmail_token.json` and to MongoDB

Gmail polling then starts automatically. Any email with the configured subject keyword (default `"journal upload"`) and an attachment is processed.

---

## How It Works

1. **Parse** — Reads any file format (CSV, TXT, ZIP)
2. **AI Mapping** — Local ML model maps your columns to Oracle GL FBDI fields (no API key — runs offline)
3. **Validate** — Checks debit/credit balance per row; marks bad rows
4. **Generate** — Creates `GlInterface.csv` (exact 150-column Oracle format ending with `END`) + `GlInterface.zip`
5. **Approval** — If some rows are bad and some good, sends approval email with Continue / Reject buttons
6. **Period Check** — Verifies the accounting period is open in Oracle Fusion
7. **Submit** — POSTs `GlInterface.zip` to Oracle ERP Integration REST API
8. **Monitor** — Polls ESS job status until complete; correlates `Import Journals` + `Import Journals: Child` via `submit.argument4` (group_id)
9. **Notify** — Sends success or failure email with the ESS log zip attached

---

## File Structure

```
oracle_fbdi_app/
├── app.py              # FastAPI application
├── database.py         # MongoDB layer (no local DB file — all state in Atlas)
├── workflow.py         # Processing pipeline + Oracle ESS correlation
├── requirements.txt
├── .env.example
├── .gitignore
├── sample_data/        # CSV/TXT/ZIP samples for testing the UI
├── services/
│   ├── fusion_service.py   # Oracle Fusion REST API
│   ├── gmail_service.py    # Gmail OAuth2
│   └── ml_mapper.py        # Local sentence-transformers ML mapper
├── utils/
│   ├── file_parser.py      # Universal file parser
│   └── fbdi_generator.py   # GlInterface.csv/zip generator + verifier
├── templates/          # Jinja2 HTML
├── static/             # CSS
├── storage/
│   └── logs/           # Application log (app.log only — gitignored)
└── config/             # Fernet key + Gmail OAuth files (all gitignored)
```

---

## Sample Journal Format

Upload any CSV / TXT / ZIP. Business-friendly column names are auto-mapped:

| date | company | cost_center | account | description | debit | credit | currency |
|---|---|---|---|---|---|---|---|
| 2026-05-16 | 120 | 10 | 60230 | Rent | 250.00 | | USD |
| 2026-05-16 | 120 | 10 | 11000 | AP accrual | | 250.00 | USD |

If the column header already matches an Oracle FBDI field name (e.g. `Segment1`, `Entered Debit Amount`), it's matched at 1.0 confidence and the ML model is skipped. See `sample_data/README.txt` for ten ready-to-upload test files.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | / | Dashboard |
| GET | /upload | Manual upload form |
| POST | /upload | Submit one or more files |
| GET | /request/{id} | Request detail page |
| GET | /api/request/{id} | Full request payload as JSON |
| GET | /api/request/{id}/status | Lightweight status poll |
| GET | /settings | Settings page |
| POST | /settings/save | Save settings |
| POST | /settings/test-fusion | Test Oracle connection |
| GET | /gmail-setup | Gmail setup page |
| GET | /approve/{token}?action=continue | Approve import |
| GET | /approve/{token}?action=reject | Reject import |
| GET | /download/{id}/{zip\|csv\|bad\|log} | Download files (from MongoDB) |
| POST | /api/poll-now | Trigger immediate Gmail check |
| POST | /api/retry/{id} | Retry a failed request |
| GET | /health | Health check |
| GET | /api/docs | Swagger UI |
