Oracle GL FBDI — Sample test files
===================================
Upload any of these via http://localhost:8000/upload

Chart of Accounts values used (matches your US Primary Ledger):
  Segment1 (Company)       = 120
  Segment2 (Cost Center)   = 10
  Segment3 (Account)       = 60230
  Segment4 (Product)       = 121
  Segment5 (Future)        = 000
  Segment6 (Intercompany)  = 000
  Date / Period            = 2025-12-16 / Dec-25
  Currency                 = USD (and INR for inr_journal.csv)

GOOD DATA (balanced, valid COA)
-------------------------------
good_journal.csv             6 rows, DR 1050 / CR 1050, USD — full business column names
good_journal.txt             Same data, tab-delimited
good_journal.zip             Same data, CSV inside a ZIP
oracle_headers_journal.csv   2 rows, uses Oracle FBDI column names directly
                             (Segment1, Entered Debit Amount, REFERENCE4 ...)
                             → triggers 1.00 exact-match mapping, no ML needed
inr_journal.csv              Foreign-currency (INR) 2 rows with conversion rate 75.50
                             → rate flows from the data column, not hard-coded

BAD DATA (will produce bad_data.csv, unbalanced totals)
-------------------------------------------------------
bad_journal.csv              5 rows: 1 row has no debit/credit, totals unbalanced
                             → 1 bad row goes to bad_data.csv, approval email sent
bad_journal.txt              Same data, tab-delimited
bad_journal.zip              Same data, inside a ZIP

PRE-BUILT FBDI (already Oracle-formatted, submit-only path)
-----------------------------------------------------------
GlInterface.csv              Headerless 150-column Oracle FBDI CSV (DR=CR=250)
                             → detected as fbdi_headerless, packaged to ZIP, submitted
prebuilt_fbdi.zip            GlInterface.zip containing GlInterface.csv
                             → detected as pre-built FBDI ZIP, validated, submitted directly

Expected outcomes
-----------------
- Good files     → Status SUCCEEDED when Oracle accepts; FBDI ZIP downloadable from request page
- Bad files      → Approval email sent; bad_data.csv generated; user can approve good rows
- INR file       → Rate 75.50 carried into Currency Conversion Rate column
- Oracle-headered file → Mappings show 1.0 confidence, method='exact'

Notes
-----
- The app stores NOTHING on local disk. These sample files exist purely as
  upload sources for manual testing.
- All uploaded files + FBDI outputs + bad_data.csv + ESS logs live in MongoDB.
- View results at http://localhost:8000/request/{req_id}
