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
| `profile.example.json` | the fixed info: agent, bill-to company, bank. Copy to `profile.json` |
| `examples/details.example.json` | per-invoice input (students / professional services) |

`profile.json` and generated invoices are **gitignored**. This repo is public, so ABN, bank
and student details must never be committed.

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

### details.json fields
- `invoice_no`: **required**. Numbers are sequential (`20261109` → `20261110`), so use the largest existing number in the Received folder + 1
- `invoice_date`: `YYYY-MM-DD`, defaults to today
- `currency`: optional, defaults to the profile currency (AUD). One currency per invoice
- `students[]`: one object per line
  - `name`, `institution`, `course`, `intake` (e.g. `S1 2027`)
  - `tuition`: commissionable tuition, or `"n/a"`
  - `amount`: amount payable, **or** `rate` (`15` or `0.15`), which is applied to `tuition`
- `notes`: optional free text printed under the signature
