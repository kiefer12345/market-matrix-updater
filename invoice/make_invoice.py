#!/usr/bin/env python3
"""Generate an agent-commission Excel invoice from a profile + per-invoice details.

Profile (fixed info: agent, bill-to company, bank)  -> invoice/profile.json (gitignored)
Invoice details (students / professional services) -> any JSON file

Usage:
  # one-off: rebuild profile.json from an existing invoice
  python3 invoice/make_invoice.py extract-profile OLD_INVOICE.xlsx -o invoice/profile.json

  # each time: generate a new invoice
  python3 invoice/make_invoice.py build details.json [-p invoice/profile.json] [-o out_dir]
"""
import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.worksheet.properties import PageSetupProperties

HERE = Path(__file__).resolve().parent
FONT = "Aptos Narrow"  # same font as the original invoice
MONEY = '"$"#,##0.00'
DATE_FMT = "dd mmm yyyy"
COLS = ["学生姓名", "申请院校", "课程名称", "开学日期", "计佣学费", "应付金额", "币种"]
WIDTHS = {"A": 3, "B": 18.8, "C": 15.5, "D": 26, "E": 14, "F": 19, "G": 13, "H": 10, "I": 12, "J": 12}

thin = Side(style="thin")
BOX = Border(top=thin, bottom=thin, left=thin, right=thin)


def f(bold=False, size=11):
    return Font(name=FONT, size=size, bold=bold)


def parse_date(v):
    if v in (None, ""):
        return dt.date.today()
    if isinstance(v, (dt.date, dt.datetime)):
        return v if isinstance(v, dt.date) else v.date()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b %Y", "%d %B %Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            pass
    raise SystemExit(f"Cannot parse date: {v!r} (use YYYY-MM-DD)")


def num(v):
    if v in (None, "", "n/a", "N/A"):
        return None
    if isinstance(v, (int, float)):
        return v
    return float(re.sub(r"[^\d.\-]", "", str(v)))


def build(details_path, profile_path, out_dir):
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))
    d = json.loads(Path(details_path).read_text(encoding="utf-8"))
    agent, bill, bank = profile["agent"], profile["bill_to"], profile["bank"]
    inv_date = parse_date(d.get("invoice_date"))
    if not d.get("invoice_no"):
        raise SystemExit("invoice_no is required: invoice numbers are sequential (e.g. 20261109 -> 20261110), "
                         "take the largest existing number in the Received folder + 1")
    inv_no = str(d["invoice_no"])
    students = d["students"]
    if not students:
        raise SystemExit("No students / line items given")
    default_ccy = d.get("currency") or profile.get("currency", "AUD")
    currencies = {s.get("currency") or default_ccy for s in students}
    if len(currencies) > 1:
        raise SystemExit(f"Mixed currencies {currencies}: make one invoice per currency")
    ccy = currencies.pop()

    wb = Workbook()
    ws = wb.active
    ws.title = "Invoice"
    ws.sheet_view.showGridLines = False
    for col, w in WIDTHS.items():
        ws.column_dimensions[col].width = w

    def put(ref, value, bold=False, fmt=None, align=None, size=11):
        c = ws[ref]
        c.value = value
        c.font = f(bold, size)
        if fmt:
            c.number_format = fmt
        if align:
            c.alignment = Alignment(horizontal=align, vertical="center")
        return c

    # ---- header -------------------------------------------------------------
    put("B1", profile.get("title", "Tax Invoice"), bold=True, size=14)
    put("B3", "Invoice to", bold=True)
    put("F3", "Invoice details", bold=True)
    put("B4", bill["name"])
    put("B5", bill.get("id_label", "Company ACN:")); put("C5", bill.get("id", ""), align="left")
    put("B6", "Address:"); put("C6", bill.get("address", ""))

    agent_line = agent["name"] + (f" (ABN {agent['abn']})" if agent.get("abn") else "")
    put("F4", "Name of the Agent:"); put("G4", agent_line)
    put("F5", "Address:"); put("G5", agent.get("address", ""))
    put("F6", "Invoice date:"); put("G6", inv_date, fmt=DATE_FMT, align="left")
    put("F7", "Invoice No:"); put("G7", inv_no, align="left")

    ws.merge_cells("B8:J8")
    put("B8", d.get("heading", profile.get("heading", "Agent Commission Invoice")), bold=True, align="center")
    for col in "BCDEFGHIJ":
        ws[f"{col}8"].border = Border(bottom=Side(style="medium"))

    # ---- line items (placed first so remittance can reference the total) ----
    hdr = 25
    first = hdr + 1
    last = hdr + len(students)
    total_row = last + 1

    # ---- remittance / bank --------------------------------------------------
    put("B10", "REMITTANCE ADVICE", bold=True)
    put("B11", "Total Payment"); put("C11", f"=G{total_row}", fmt=MONEY, align="left")
    put("D11", ccy)
    put("B12", profile.get("gst_note", "GST: N/A"))
    rows = [
        ("Bank:", bank.get("bank", "")),
        ("Bank Swift Code:", bank.get("swift", "")),
        ("A/C Name:", bank.get("account_name", "")),
        ("A/C NO:", bank.get("account_no", "")),
        ("BSB NO:", bank.get("bsb", "")),
        ("A/C Address:", bank.get("account_address", "")),
    ]
    for i, (label, val) in enumerate(rows):
        r = 14 + i
        put(f"B{r}", label, bold=True)
        c = put(f"C{r}", str(val), align="left")
        c.number_format = "@"  # keep BSB / account numbers as text
    put("B21", "Payment Method:", bold=True)
    put("C21", profile.get("payment_method", "Bank Transfer"), bold=True)

    put("B23", d.get("services_heading", "For Professional Services For Below Recommended Students"), bold=True)

    for i, name in enumerate(COLS):
        c = put(f"{'BCDEFGH'[i]}{hdr}", name, bold=True, align="center")
        c.border = BOX

    for r, s in enumerate(students, start=first):
        tuition = num(s.get("tuition"))
        rate = num(s.get("rate"))
        amount = num(s.get("amount"))
        if amount is None and tuition is not None and rate is not None:
            rate = rate / 100 if rate > 1 else rate
            amount_val = f"=ROUND(F{r}*{rate},2)"
        elif amount is not None:
            amount_val = amount
        else:
            raise SystemExit(f"Student {s.get('name')!r}: give 'amount', or 'tuition' + 'rate'")
        intake = s.get("intake", "")
        vals = [s.get("name", ""), s.get("institution", ""), s.get("course", ""), intake,
                tuition if tuition is not None else "n/a", amount_val, ccy]
        for i, v in enumerate(vals):
            col = "BCDEFGH"[i]
            c = put(f"{col}{r}", v, align="center")
            c.border = BOX
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if col == "F":
                c.number_format = '"$"#,##0'
            if col == "G":
                c.number_format = MONEY

    put(f"F{total_row}", "Total", bold=True, align="center").border = BOX
    c = put(f"G{total_row}", f"=SUM(G{first}:G{last})", bold=True, fmt=MONEY, align="center")
    c.border = BOX
    put(f"H{total_row}", ccy, align="center").border = BOX

    sig = total_row + 3
    put(f"B{sig}", "Name of the Agent", bold=True)
    put(f"C{sig}", agent["name"])
    if d.get("notes"):
        put(f"B{sig + 2}", "Notes:", bold=True)
        put(f"C{sig + 2}", d["notes"])

    # ---- print setup: A4 portrait, fit to one page wide ---------------------
    ws.page_setup.paperSize = 9
    ws.page_setup.orientation = "portrait"
    ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.print_area = f"A1:J{sig + 3}"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_agent = re.sub(r"\W+", "_", agent["name"]).strip("_")
    out = out_dir / f"Inv_{inv_no}_{safe_agent}_{inv_date.strftime('%d_%b_%Y')}.xlsx"
    if out.exists():
        # Create-only rule: never overwrite an existing invoice (see CLAUDE.md)
        raise SystemExit(f"Refusing to overwrite existing file: {out}")
    wb.save(out)
    print(out)
    return out


def extract_profile(xlsx, out):
    """Pull agent / bill-to / bank details out of an invoice in the original layout."""
    ws = load_workbook(xlsx, data_only=True).active
    cells = {}
    for row in ws.iter_rows():
        for c in row:
            if c.value is not None:
                cells[c.coordinate] = c.value

    def right_of(label_regex, cols="CDEFGH"):
        for ref, v in cells.items():
            if isinstance(v, str) and re.match(label_regex, v.strip(), re.I):
                col, row = re.match(r"([A-Z]+)(\d+)", ref).groups()
                if col not in cols[:-1]:
                    continue  # label belongs to another block (e.g. agent vs bill-to address)
                for nxt in cols[cols.index(col) + 1:]:
                    if f"{nxt}{row}" in cells:
                        return str(cells[f"{nxt}{row}"]).strip()
        return ""

    agent_raw = right_of(r"name of the agent:", cols="FGHI")
    m = re.match(r"(.*?)\s*\(ABN\s*([\d ]+)\)", agent_raw)
    agent_name, abn = (m.group(1).strip(), m.group(2).strip()) if m else (agent_raw, "")
    bill_name = str(cells.get("B4", "")).strip()
    profile = {
        "title": str(cells.get("B1", "Tax Invoice")).strip(),
        "heading": "Agent Commission Invoice",
        "currency": "AUD",
        "gst_note": "GST: N/A",
        "payment_method": "Bank Transfer",
        "agent": {"name": agent_name or right_of(r"name of the agent$", cols="BC"), "abn": abn,
                  "address": right_of(r"address:", cols="FGH")},
        "bill_to": {"name": bill_name, "id_label": "Company ACN:", "id": right_of(r"company acn:", cols="BC"),
                    "address": right_of(r"address:", cols="BCD")},
        "bank": {"bank": right_of(r"bank:", cols="BC"), "swift": right_of(r"bank swift code:", cols="BC"),
                 "account_name": right_of(r"a/c name:?", cols="BC"), "account_no": right_of(r"a/c no:?", cols="BC"),
                 "bsb": right_of(r"bsb no:?", cols="BC"), "account_address": right_of(r"a/c address:?", cols="BC")},
    }
    Path(out).write_text(json.dumps(profile, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="generate an invoice")
    b.add_argument("details")
    b.add_argument("-p", "--profile", default=str(HERE / "profile.json"))
    b.add_argument("-o", "--out-dir", default=".")
    e = sub.add_parser("extract-profile", help="create profile.json from an existing invoice")
    e.add_argument("xlsx")
    e.add_argument("-o", "--out", default=str(HERE / "profile.json"))
    a = ap.parse_args()
    if a.cmd == "build":
        if not Path(a.profile).exists():
            sys.exit(f"Profile not found: {a.profile}. Run extract-profile on an old invoice, "
                     "or copy invoice/profile.example.json to invoice/profile.json and fill it in.")
        build(a.details, a.profile, a.out_dir)
    else:
        extract_profile(a.xlsx, a.out)


if __name__ == "__main__":
    main()
