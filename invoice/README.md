# Excel invoice generator (agent commission)

Generates an `.xlsx` invoice in the same layout as the original agent-commission invoice
(Tax Invoice → Invoice to / Invoice details → Remittance advice & bank → student table → Total).

The total is a live `=SUM()` formula, and "Total Payment" links to it. A row given
`tuition` + `rate` gets a `=ROUND(tuition*rate,2)` formula, so editing the sheet later still recalculates.

## Folder rule (create-only)
In the Received invoice folder you may only **create new files**. Never modify, overwrite, rename, move or delete existing ones. See `/CLAUDE.md`. The script refuses to overwrite an existing output file.

## Files
| File | What it is |
|---|---|
| `make_invoice.py` | the generator |
| `company.json` | standard bill-to details (company name, ABN, ACN, current address), used on every invoice |
| `profile.example.json` | the fixed info: agent and bank. Copy to `profile.json` |
| `examples/details.example.json` | per-invoice input (students / professional services) |

`profile.json` and generated invoices are **gitignored**. This repo is public, so ABN, bank
and student details must never be committed.

## Data checks built in
- The bill-to block always comes from `company.json`, never from an old invoice.
- BSB and account number written in each other's fields (seen on several old invoices) are swapped
  back automatically, using the BSB length and the bank's BSB prefix (06 CBA, 11 St.George, 01 ANZ,
  03/73 Westpac, 08 NAB). A note is printed, and a warning if the BSB still doesn't match the bank.

## Setup (once per environment)
Rebuild the profile from any previous invoice:
```bash
python3 invoice/make_invoice.py extract-profile OLD_INVOICE.xlsx -o invoice/profile.json
```
Or copy `profile.example.json` to `profile.json` and fill it in.

## Each invoice
```bash
python3 invoice/make_invoice.py build details.json -o out/
```
Output name: `Inv_<invoice_no>_<Agent_Name>_<DD_Mon_YYYY>.xlsx`.

### GST (per agent, in profile.json)
- `"gst_registered": false`: title **Invoice**, remittance shows `GST: N/A`. Every agent whose past invoices say `GST: N/A` is set to this automatically by `extract-profile`.
- `"gst_registered": true`: title **Tax Invoice**, with Subtotal, GST 10% and Total (incl. GST) rows. Line amounts are treated as GST-exclusive, unless the details file sets `"amounts_include_gst": true`.
- Missing / `null`: the build is refused. A **new agent** must have their GST status confirmed with the user first.

### details.json fields
- `invoice_no`: **required**. Numbers are sequential (`20261109` → `20261110`), so use the largest existing number in the Received folder + 1
- `invoice_date`: `YYYY-MM-DD`, defaults to today
- `currency`: optional, defaults to the profile currency (AUD). One currency per invoice
- `students[]`: one object per line
  - `name`, `institution`, `course`, `intake` (e.g. `S1 2027`)
  - `tuition`: commissionable tuition, or `"n/a"`
  - `rate`: shown in the 佣金比例 column exactly as given. Use a staged rate such as `"10%*60%"` when the
    details give the stages, or a final rate such as `"8.24%"` when they don't. Numbers (`15`, `0.15`) also work
  - `amount`: amount payable. If omitted, it is calculated as `tuition × rate` (formula kept in the cell).
    If both are given and they disagree, the build prints a warning
- `notes`: optional free text printed under the signature
