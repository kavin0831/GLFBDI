# Setup Guide — Oracle Fusion GL FBDI Automation

This walks through every configuration knob.  For a quick overview see [README.md](README.md).

---

## 1. Prerequisites

- Python 3.11+
- A MongoDB Atlas cluster (free tier is enough) — or a local MongoDB
- An Oracle Fusion ERP Cloud tenant with the **ERP Integrations** REST API enabled
  and a user account with ERP Integration access
- *(optional)* A Gmail account with 2-Step Verification enabled, if you want
  email polling or notifications
- Outbound internet access (the sentence-transformers model is downloaded once,
  ~22 MB)

---

## 2. Local development

```bash
git clone https://github.com/<your-user>/GLFBDI.git
cd GLFBDI
pip install -r requirements.txt
cp .env.example .env       # then edit .env and set MONGO_URI
python app.py
```

Open http://localhost:8000 and you'll land on the dashboard.

### Local-only `.env` template

```ini
MONGO_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/
MONGO_DB=oracle_fbdi
# Optional — pin the encryption key so it doesn't auto-regenerate
# FERNET_KEY=<32-byte url-safe base64>
```

The `.env` file is gitignored.  Never commit real credentials.

---

## 3. First-run configuration

After the app starts, visit `/settings` and fill in the form:

| Field | Where it ends up |
|-------|-------------------|
| Fusion URL | Your Oracle Fusion ERP Cloud base URL |
| Username | Oracle user with ERP Integration access |
| Password | **Encrypted with Fernet** before being written to MongoDB |
| Ledger | Primary ledger name from your COA |
| Document Account | UCM document account (typically `fin$/generalLedger$/import$`) |
| Notification Email | Where success / failure / approval emails go |
| Gmail Subject Filter | Keyword the auto-poller matches in inbound subjects |
| Public App URL | Auto-detected from `SPACE_HOST` on HF; paste manually for other hosts |

Sensitive fields (`fusion_password`, `gmail_app_password`) are encrypted at
rest.  Nothing is written to local disk; all state lives in MongoDB.

---

## 4. Environment variables

| Var | Required | Default | Purpose |
|-----|----------|---------|---------|
| `MONGO_URI` | yes | `mongodb://localhost:27017/` | Atlas connection string |
| `MONGO_DB` | no | `oracle_fbdi` | Database name |
| `FERNET_KEY` | recommended for stateless hosts | auto-generated → `config/secret.key` | Symmetric encryption key — pin this so encrypted passwords/tokens survive rebuilds |
| `APP_BASE_URL` | no | derived from `SPACE_HOST` or settings | Public host used in approval-email links |
| `PORT` | no | `8000` | Listen port; HF Spaces sets this to `7860` |
| `SPACE_HOST` | (HF sets it automatically) | unset | If present, becomes `https://$SPACE_HOST` as the public base URL |

### Generating a Fernet key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Save that 44-character string somewhere safe.  If you ever need to redeploy or
recover, you'll set it as `FERNET_KEY` in env vars / secrets.

---

## 5. Gmail integration

Gmail is **optional**.  The app works fine with only manual uploads via `/upload`.
If you want auto-polling and notification emails, pick one of these modes.

### Mode A — App Password (recommended, works on every host)

No Google Cloud Console.  No OAuth.  No consent screens.

1. Enable 2-Step Verification on the Gmail account you'll use for polling:
   https://myaccount.google.com/signinoptions/twosv

2. Generate an App Password (16 characters):
   https://myaccount.google.com/apppasswords

3. In the app, open `/gmail-setup` → top **App Password** card → enter:
   - Gmail address (e.g. `your-polling-mailbox@gmail.com`)
   - App Password (16 chars — spaces are stripped automatically)

4. Click **Save App Password** → then **Test Connection**.  Within 2 seconds
   you'll see either *"IMAP login OK"* or a clear error.

What happens behind the scenes:

- **Inbox polling** uses `imap.gmail.com:993` (SSL) with Gmail's `X-GM-RAW`
  search to look for emails matching the subject filter and lacking the
  `FBDI_PROCESSED` Gmail label.  Each match's `.csv/.txt/.zip` attachments are
  pulled, queued, then labeled `FBDI_PROCESSED + Read` so they're never
  picked up twice.
- **Notification emails** are sent via `smtp.gmail.com:587` with STARTTLS.

### Mode B — OAuth (more setup; good for local dev, awkward on HF)

1. In Google Cloud Console (https://console.cloud.google.com/apis/credentials):
   - Create an **OAuth client ID**, type **Web application** (use Desktop only
     for local dev — Desktop clients can't redirect to HTTPS URLs)
   - Under **Authorized redirect URIs**, add the value displayed on the
     `/gmail-setup` page (copy button there).  For HF Spaces it looks like
     `https://<user>-<space>.hf.space/gmail-setup/oauth-callback`
2. Enable the **Gmail API** for the project (APIs & Services → Library →
   search "Gmail API" → Enable)
3. Configure the **OAuth consent screen**:
   - User type **External** (Internal blocks personal Gmails)
   - Under **Test users**, add every Gmail you want to authorize (the consent
     screen blocks anyone not on this list while the app is in Testing mode)
4. Download `credentials.json` from the client and upload it via
   `/gmail-setup` → Step 2
5. Click **▶ Sign in with Google**.  Your browser is redirected to Google's
   consent page; after Allow, Google redirects back and the app stores the
   OAuth token encrypted in MongoDB.

For purely headless deploys (no browser at all), the page also accepts a
`gmail_token.json` generated by running the app locally once and clicking
through OAuth there — upload via the "Alternative: upload a token" section.

---

## 6. Deploy on Hugging Face Spaces

This repo includes a `Dockerfile` and the README has the HF frontmatter
(`sdk: docker, app_port: 7860`) so the Space SDK is auto-set on first push.

1. Create a new Space at https://huggingface.co/new-space
   - **Owner:** your HF username
   - **Space name:** anything
   - **License:** any (e.g. `mit`)
   - **Space SDK:** **Docker** → Blank template
   - **Visibility:** Private if you want only signed-in HF users to see it,
     otherwise Public (URL is unguessable enough for finance-team sharing)

2. Generate a Hugging Face access token (write scope) at
   https://huggingface.co/settings/tokens

3. Push the repo to the Space:

   ```bash
   git remote add hf https://huggingface.co/spaces/<your-user>/<space-name>
   git push hf main
   ```

   When prompted: username = your HF username, password = the access token.

4. In **Space → Settings → Variables and secrets**, add (mark each as Secret):

   | Name | Value |
   |------|-------|
   | `MONGO_URI` | Your Atlas connection string |
   | `MONGO_DB` | `oracle_fbdi` |
   | `FERNET_KEY` | The 44-char Fernet key you generated earlier |

5. In **MongoDB Atlas → Network Access**, click **Add IP Address** → **Allow
   Access from Anywhere** (`0.0.0.0/0`).  HF egress IPs rotate, so a specific
   whitelist isn't workable on the free tier.

6. Watch **Logs** tab on the Space.  First build is 5–8 minutes (Docker image,
   Python deps, ML model bake).  When you see `Application startup complete`,
   the app is live at `https://<user>-<space-name>.hf.space`.

### The two URLs your Space has

| URL | Shows |
|-----|-------|
| `https://huggingface.co/spaces/<user>/<space>` | HF wrapper (Files, Logs, Settings, Community) |
| `https://<user>-<space>.hf.space` | **Just the app** — no HF chrome, suitable for sharing with finance |

Approval emails link to the `*.hf.space` URL (auto-detected from `SPACE_HOST`).

### HF free tier caveats

- Container sleeps after ~48 h of inactivity — Gmail polling pauses until someone
  visits the URL.  Upgrade hardware or use an external uptime ping to keep it warm.
- No persistent disk — that's why we pin `FERNET_KEY` and store everything in
  MongoDB.  A rebuild wipes `config/` but the app restores `gmail_token` from
  MongoDB on next start.

---

## 7. How it works (pipeline)

For each upload (manual or from Gmail):

1. **Parse** — auto-detects CSV / TXT / ZIP layout, handles tab/comma/pipe
   delimiters, headerless FBDI files, business-friendly headers, and Oracle
   FBDI column-name headers
2. **AI mapping** — local sentence-transformers model maps source columns to
   Oracle GL FBDI fields.  Exact matches → 1.0 confidence (no ML).  Alias
   matches → 0.95.  Semantic matches → embedding cosine similarity
3. **Validate** — checks debit/credit balance, flags rows missing amounts,
   tracks currency conversion rate (defaults to `1.00` only for USD; foreign
   currency must come from the data or Oracle rejects it)
4. **Generate** — builds the 150-column `GlInterface.csv` (positional,
   headerless, every row ending with `END`) and verifies the output before
   packaging into `GlInterface.zip`
5. **Approval gate** — if some rows are good and some are bad, an approval
   email goes out with Continue / Reject buttons that link to `/approve/{token}`
6. **Period check** — confirms the accounting period is open in Oracle Fusion
7. **Submit** — POSTs `GlInterface.zip` to Oracle's `importBulkData` endpoint
8. **Monitor + correlate** — polls ESS status (adaptive 2 s → configurable
   interval); uses Oracle's Scheduler REST API to read each spawned
   `Import Journals` job's `submit.argument4` parameter and only attaches logs
   whose group_id matches this submission
9. **Notify** — success or failure email with the ESS log ZIP attached

---

## 8. File structure

```
.
├── app.py                  # FastAPI application + routes
├── database.py             # MongoDB layer (no local DB file)
├── workflow.py             # Processing pipeline + Oracle correlation
├── requirements.txt
├── Dockerfile              # HF Spaces / generic Docker
├── .dockerignore
├── .env.example
├── .gitignore
├── README.md               # Quick reference (also has HF frontmatter)
├── SETUP.md                # This file
├── sample_data/            # Test files for manual upload
│   ├── good_journal.{csv,txt,zip}
│   ├── bad_journal.{csv,txt,zip}
│   ├── oracle_headers_journal.csv
│   ├── inr_journal.csv
│   ├── GlInterface.csv
│   └── prebuilt_fbdi.zip
├── services/
│   ├── fusion_service.py   # Oracle Fusion REST API
│   ├── gmail_service.py    # Gmail OAuth2 (send_email dispatcher)
│   ├── gmail_imap.py       # Gmail IMAP + SMTP (App Password mode)
│   └── ml_mapper.py        # sentence-transformers column mapper
├── utils/
│   ├── file_parser.py      # Universal CSV/TXT/ZIP parser
│   └── fbdi_generator.py   # GlInterface.csv builder + verifier
├── templates/              # Jinja2 HTML
├── static/                 # CSS + logo + favicon
├── storage/
│   └── logs/app.log        # Only file ever written locally (gitignored)
└── config/                 # Fernet key, OAuth files (gitignored)
```

---

## 9. Sample journal format

Upload any CSV / TXT / ZIP.  Business-friendly column names are auto-mapped:

| date | company | cost_center | account | description | debit | credit | currency |
|------|---------|-------------|---------|-------------|-------|--------|----------|
| 2026-05-16 | 01 | 100 | 10110 | Rent | 250.00 | | USD |
| 2026-05-16 | 01 | 100 | 11000 | AP accrual | | 250.00 | USD |

If a column header already matches an Oracle FBDI field name exactly (e.g.
`Segment1`, `Entered Debit Amount`, `REFERENCE4 (Journal Entry Name)`), the ML
model is skipped and that column maps at 1.0 confidence.

See `sample_data/README.txt` for ten ready-to-upload variants covering happy
path, bad data, foreign currency, Oracle-headered data, and pre-built FBDI ZIPs.

---

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| App starts but `/settings` is blank | `FERNET_KEY` mismatch — the key that encrypted the data isn't the one running now | Set `FERNET_KEY` env var to the original key |
| Build fails with Mongo timeout | Atlas IP whitelist | Add `0.0.0.0/0` in Network Access |
| `redirect_uri_mismatch` from Google | OAuth client's registered redirect URI doesn't match what the app sends | Copy the exact URL shown on `/gmail-setup` into your OAuth client's allowed list |
| `403 / access blocked` from Google | OAuth consent screen blocks your Gmail | Add the Gmail to **Test users** in OAuth consent screen, or switch to App Password mode |
| Approval emails link to `localhost` | `SPACE_HOST` not set and `app_base_url` blank | Set `APP_BASE_URL` env var or paste the URL into `/settings` |
| Cross-contamination of Import Journals logs | Old code — fixed in current version using `submit.argument4` | Pull latest |

---

## 11. License & confidentiality

- **Internal use only** — do not publish credentials, real OAuth client JSONs,
  Fernet keys, or actual Oracle URLs to a public Git host
- The `.gitignore` already excludes `.env`, `config/secret.key`,
  `config/gmail_credentials.json`, `config/gmail_token.json`, and runtime logs
- Real values live only in:
  - Local `.env` (never committed)
  - HF Spaces Secrets (never visible to anyone but the Space owner)
  - MongoDB Atlas (Fernet-encrypted)
