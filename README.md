---
title: GLFBDI
emoji: 🌐
colorFrom: red
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
---

# GLFBDI — Oracle Fusion GL FBDI Automation

A FastAPI app that automates Oracle Fusion General Ledger journal imports via
the File-Based Data Import (FBDI) format.  Drop a CSV/TXT/ZIP into the upload
page (or have it land in a watched Gmail inbox) and the app:

1. Parses any column layout (CSV, TXT, ZIP — with Excel, JSON, XML, PDF also supported)
2. Uses a local sentence-transformer model to map source columns → Oracle FBDI fields
3. Validates rows (debit/credit balance, required fields, currency conversion rate)
4. Generates a 150-column `GlInterface.csv` ending with `END` per row
5. Packages `GlInterface.zip` and submits via Oracle's `importBulkData` REST API
6. Polls ESS status, correlates the spawned **Import Journals** + **Import Journals: Child**
   via `submit.argument4` (group_id), downloads the full execution log
7. Emails a finance approver with success / failure / approval-needed summaries

All state — uploaded files, generated FBDI, ESS logs, app settings, secrets —
lives in **MongoDB Atlas**. Nothing important is written to local disk, so the
app runs cleanly on ephemeral hosts (Hugging Face Spaces, Docker, Render, …).

## Tech stack

| Layer | Tool |
|-------|------|
| Web | FastAPI + Jinja2 + Uvicorn |
| Database | MongoDB Atlas (pymongo) |
| ML mapping | `sentence-transformers/all-MiniLM-L6-v2` — offline after first download (~22 MB) |
| Encryption at rest | Fernet (cryptography) — passwords + OAuth tokens encrypted in MongoDB |
| Oracle integration | `httpx` against ERP Integrations + Scheduler REST APIs |
| Email | Two modes — Gmail App Password (IMAP+SMTP) **OR** Gmail OAuth2 |

## Quick start (local)

```bash
git clone https://github.com/<your-user>/GLFBDI.git
cd GLFBDI
pip install -r requirements.txt
cp .env.example .env             # then fill in MONGO_URI
python app.py                    # → http://localhost:8000
```

Open `/settings` and fill in your Oracle Fusion and notification settings,
then go to `/upload` and drop a file from `sample_data/`.

## Deploy on Hugging Face Spaces

This repo is HF-ready: it ships a `Dockerfile`, `app_port: 7860` in the README
frontmatter, and the app honours the `PORT`, `SPACE_HOST`, `MONGO_URI`,
`MONGO_DB`, and `FERNET_KEY` environment variables HF provides / supports.

1. Create a new Space at https://huggingface.co/new-space → choose **Docker** SDK
2. Add HF as a remote and push:
   ```bash
   git remote add hf https://huggingface.co/spaces/<your-hf-username>/<space-name>
   git push hf main
   ```
3. In **Space → Settings → Variables and secrets**, add:

   | Name | Type | Value |
   |------|------|-------|
   | `MONGO_URI` | Secret | `mongodb+srv://<user>:<password>@<cluster>.mongodb.net/` |
   | `MONGO_DB` | Secret | `oracle_fbdi` |
   | `FERNET_KEY` | Secret | a 32-byte url-safe base64 key (see below) |

4. In MongoDB Atlas → **Network Access** → **+ ADD IP ADDRESS** → **Allow Access
   from Anywhere** (`0.0.0.0/0`).  HF Spaces use rotating egress IPs; you can't
   pin a specific one.
5. Wait ~5 min for the build to finish.  Open the Space's `*.hf.space` URL —
   it serves only the app (no HF chrome), suitable for sharing with finance users.

### Generating a stable Fernet key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Pin this key in HF Secrets so the encrypted data in MongoDB stays decryptable
across container rebuilds.  If you change the key, previously-encrypted passwords
become unreadable and must be re-entered.

## Environment variables

| Var | Required | Default | Purpose |
|-----|----------|---------|---------|
| `MONGO_URI` | yes | `mongodb://localhost:27017/` | Atlas connection string |
| `MONGO_DB` | no | `oracle_fbdi` | Database name |
| `FERNET_KEY` | recommended | auto-generated `config/secret.key` | Override the symmetric encryption key (set on stateless hosts so it persists) |
| `APP_BASE_URL` | no | derived from `SPACE_HOST` | Public host used in approval-email links; overrides auto-detection |
| `PORT` | no | `8000` | Listen port (HF Spaces sets this to `7860`) |
| `SPACE_HOST` | (HF auto) | unset | If set, the app builds `https://$SPACE_HOST` as the public base URL |

## First-run configuration (inside the UI)

Open `/settings` and fill in:

| Setting | Notes |
|---|---|
| Fusion URL | Your Oracle Fusion ERP Cloud base (e.g. `https://fa-<env>.<provider>.com`) |
| Username | Oracle Fusion user with ERP Integration access |
| Password | Encrypted with Fernet before storage |
| Ledger | Primary ledger name from your chart of accounts |
| Document Account | UCM document account, typically `fin$/generalLedger$/import$` |
| Notification Email | Address that receives success / failure / approval emails |
| Public App URL | Auto-filled on HF via `SPACE_HOST`; for other hosts paste your URL |

## Gmail integration (optional)

Two modes — pick one.  Gmail is entirely optional; the app works fine with
manual uploads if you skip it.

### Option A — App Password (recommended for any deployed host)

Simplest, no Google Cloud Console needed:

1. Enable 2-Step Verification on the Gmail account:
   https://myaccount.google.com/signinoptions/twosv
2. Generate an App Password at https://myaccount.google.com/apppasswords
3. In the app, open `/gmail-setup` → top "App Password" card → paste the Gmail
   address + 16-char App Password → click **Save** then **Test Connection**

The app uses IMAP (port 993, SSL) to poll the inbox and SMTP (port 587, STARTTLS)
to send notifications.  Polling labels processed messages `FBDI_PROCESSED` and
marks them read so they aren't re-processed.

### Option B — OAuth (more setup, works on local dev)

1. In Google Cloud Console, create an OAuth Client ID
   - **Application type:** Web application (or Desktop for local-only)
   - For deployed hosts, add the redirect URI shown on `/gmail-setup`
2. Enable the Gmail API for the project
3. Add your Gmail to the OAuth consent screen's **Test users** list
4. Download `credentials.json` and upload it via `/gmail-setup` → Step 2
5. Click **Sign in with Google** → consent → token stored encrypted in MongoDB

For headless deploys (HF Spaces, Docker) without a browser, the page also
accepts a `gmail_token.json` generated by running the app locally once.

## Sample data

`sample_data/` ships ready-to-upload test files using valid Oracle COA structure:

| File | Behavior |
|------|----------|
| `good_journal.csv` / `.txt` / `.zip` | Balanced 6-row journal — exercises the happy path |
| `bad_journal.csv` / `.txt` / `.zip` | 1 row missing amounts, unbalanced totals — exercises the bad-data + approval-email path |
| `oracle_headers_journal.csv` | Source columns already named like Oracle FBDI fields → 1.0 exact-match mapping |
| `inr_journal.csv` | Foreign currency (INR) with conversion rate column |
| `GlInterface.csv` | Headerless pre-built FBDI — detected and submitted directly |
| `prebuilt_fbdi.zip` | Pre-built `GlInterface.zip` — validates the CSV inside then submits |

Edit `sample_data/good_journal.csv` to match your own ledger's Company /
Cost Center / Account values before uploading.

## API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Dashboard |
| GET | `/upload` | Upload form |
| POST | `/upload` | Submit one or more files (multipart, accepts `.csv/.txt/.zip`) |
| GET | `/request/{id}` | Per-request detail page |
| GET | `/api/request/{id}` | Full request payload as JSON |
| GET | `/api/request/{id}/status` | Lightweight status poll |
| POST | `/api/poll-now` | Trigger Gmail check immediately |
| POST | `/api/retry/{id}` | Retry a failed request |
| GET | `/download/{id}/{zip\|csv\|bad\|log}` | Stream artifact bytes from MongoDB |
| GET | `/approve/{token}` | Finance reviewer Continue / Reject endpoint |
| GET | `/settings`, POST `/settings/save` | Configuration |
| POST | `/settings/test-fusion` | Test Oracle Fusion connectivity |
| POST | `/settings/test-gmail` | Test Gmail App Password connectivity |
| GET | `/gmail-setup` | Gmail integration wizard |
| GET | `/health` | Health probe |

## How concurrent submissions are isolated

When several files are submitted around the same time, Oracle spawns separate
`Import Journals` ESS requests whose request IDs interleave.  The app reads
each candidate's `submit.argument4` parameter (our numeric `Interface Group
Identifier`) via the Scheduler REST API and only attaches logs whose group_id
matches this submission's.  Result: each request page in the UI shows exactly
its own `Load Interface File for Import`, `Transfer File`, `Load File to
Interface`, `Import Journals`, and `Import Journals: Child` jobs — no
cross-contamination, even at high concurrency.

## What's in MongoDB

| Collection | Contents |
|------------|----------|
| `app_settings` | Single doc with all config (passwords + tokens Fernet-encrypted) |
| `journal_requests` | One doc per submission: status, file hashes, mappings, Oracle ess_request_id |
| `mapping_history` | Learned source→target mappings, boost confidence over time |
| `uploaded_files` | Encrypted source-file bytes per request (so workflow can re-read after restart) |
| `generated_files` | FBDI CSV / ZIP / bad_data / ESS log per request, base64 + SHA-256 |
| `process_logs` | Structured per-stage log entries + downloaded ESS log files |
| `secure_files` | Gmail credentials, OAuth token, Fernet-encrypted |
| `ji_claims` | Atomic claim records for Import Journals correlation under concurrency |

Nothing important is on the host filesystem.  `storage/logs/app.log` is the
only file the app writes locally and it's gitignored.

## License

Internal use — not for public distribution.
