# GLFBDI — Oracle Fusion GL FBDI Automation

A FastAPI app that automates Oracle Fusion General Ledger journal imports via the
File-Based Data Import (FBDI) format.  Drop a CSV/TXT/ZIP into the upload page
(or have it land in a watched Gmail inbox) and the app:

1. Parses any column layout (Excel, CSV, TXT, JSON, XML, PDF, ZIP)
2. Uses a local sentence-transformer model to map source columns → Oracle FBDI fields
3. Validates rows (debit/credit balance, required fields)
4. Generates a 150-column `GlInterface.csv` ending with `END` per row
5. Packages `GlInterface.zip` and submits via Oracle's `importBulkData` REST API
6. Polls ESS status, correlates the spawned **Import Journals** + **Import Journals: Child**
   via `submit.argument4` (group_id), downloads the full execution log, and emails
   a finance approver with a summary

All state — uploaded files, generated FBDI, ESS logs, settings, secrets — lives in
MongoDB.  Nothing important is written to local disk.

## Tech stack

| Layer | Tool |
|-------|------|
| Web | FastAPI + Jinja2 + Uvicorn |
| Database | MongoDB Atlas (pymongo) |
| ML mapping | `sentence-transformers/all-MiniLM-L6-v2` (offline after first download) |
| Encryption | Fernet (cryptography) |
| Oracle integration | `httpx` against ERP Integrations + Scheduler REST APIs |
| Email | Gmail API (OAuth2) |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env          # then fill in MONGO_URI
python app.py                  # → http://localhost:8000
```

First-run housekeeping inside the UI:

1. **Settings** — paste your Oracle Fusion URL, username, password, ledger name, document account
2. **Gmail Setup** — upload `credentials.json` from your Google Cloud Console OAuth client
3. **Upload** — drop a CSV/TXT/ZIP from `sample_data/` to test end-to-end

## Environment variables

| Var | Required | Default | Purpose |
|-----|----------|---------|---------|
| `MONGO_URI` | yes | `mongodb://localhost:27017/` | Atlas connection string |
| `MONGO_DB` | no | `oracle_fbdi` | Database name |
| `FERNET_KEY` | no | auto-generated | Override symmetric encryption key |

## Sample data

`sample_data/` contains 10 ready-to-upload test files using the real Oracle COA
values (Company `120`, Cost Center `10`, Account `60230`, etc.):

| File | Behavior |
|------|----------|
| `good_journal.csv/.txt/.zip` | Balanced 6-row journal, USD, business column names |
| `bad_journal.csv/.txt/.zip` | 1 row missing amounts, unbalanced — exercises bad-data path |
| `oracle_headers_journal.csv` | Already uses Oracle FBDI field names → 1.0 exact mapping |
| `inr_journal.csv` | Foreign currency (INR) with conversion rate from data |
| `GlInterface.csv` | Headerless pre-built FBDI — detected and submitted directly |
| `prebuilt_fbdi.zip` | Pre-built GlInterface.zip — validates + submits |

## Key API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Dashboard |
| GET | `/upload` | Upload form |
| POST | `/upload` | Submit one or more files |
| GET | `/request/{req_id}` | Per-request detail page |
| GET | `/api/request/{req_id}` | Per-request JSON |
| GET | `/api/request/{req_id}/status` | Lightweight status poll |
| POST | `/api/poll-now` | Trigger Gmail check immediately |
| POST | `/api/retry/{req_id}` | Retry a failed request |
| GET | `/download/{req_id}/{zip\|csv\|bad\|log}` | Download artifacts from MongoDB |
| GET | `/settings` | Configuration |
| GET | `/gmail-setup` | Gmail OAuth wizard |
| GET | `/health` | Health probe |

## Concurrency correlation

When many submissions run at the same time, Oracle spawns separate
`Import Journals` ESS requests whose only correlation key is the
`submit.argument4` parameter (our numeric `Interface Group Identifier`).
The app reads that field via the Scheduler REST API for every JI candidate
to attach the right pair of logs to the right submission — verified against
live Oracle Fusion data, see commit history for the diagnostic scripts that
established this approach.

## License

Internal use — not for public distribution.
