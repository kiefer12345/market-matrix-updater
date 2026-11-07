#!/usr/bin/env python3
"""Generate an agent-commission Excel invoice from a profile + per-invoice details.

Company (bill-to: Ausway, standard details)          -> invoice/company.json
Profile (fixed info: agent, bank)                    -> invoice/profile.json (gitignored)
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
COLS = ["学生姓名", "申请院校", "课程名称", "开学日期", "计佣学费", "佣金比例", "应付金额", "币种"]
TCOLS = "BCDEFGHI"  # table columns; F tuition, G rate, H amount, I currency
WIDTHS = {"A": 3, "B": 24, "C": 15.5, "D": 26, "E": 14, "F": 19, "G": 14, "H": 14, "I": 8, "J": 18}
GST_RATE = 0.10
ROUNDING_TOLERANCE = 1.00  # amount vs tuition x rate: gaps below this are rounding, use the given amount

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


# BSB prefix (first 2 digits) -> bank, used to catch BSB / account number written in swapped fields
BANK_PREFIX = {"commonwealth": ("06",), "cba": ("06",), "westpac": ("03", "73"), "st george": ("11",),
               "st.george": ("11",), "anz": ("01",), "australia and new zealand": ("01",), "nab": ("08",),
               "national australia": ("08",)}


def digits(v):
    return re.sub(r"\D", "", str(v or ""))


def normalize_bank(bank):
    """Return a copy of bank with BSB formatted 'XXX-XXX' and BSB/account swapped back if needed."""
    bank = dict(bank)
    bsb, acct = digits(bank.get("bsb")), digits(bank.get("account_no"))
    name = str(bank.get("bank", "")).lower()
    prefixes = next((v for k, v in BANK_PREFIX.items() if k in name), ())
    swap = False
    if len(bsb) != 6 and len(acct) == 6:
        swap = True
    elif len(bsb) == 6 and len(acct) == 6 and prefixes and acct[:2] in prefixes and bsb[:2] not in prefixes:
        swap = True
    if swap:
        print(f"note: BSB and account number were in swapped fields - corrected "
              f"(BSB {acct}, A/C {bank.get('bsb')})", file=sys.stderr)
        bank["bsb"], bank["account_no"] = bank.get("account_no"), bank.get("bsb")
        bsb = digits(bank["bsb"])
    if len(bsb) == 6:
        bank["bsb"] = f"{bsb[:3]}-{bsb[3:]}"
        if prefixes and bsb[:2] not in prefixes:
            print(f"WARNING: BSB {bank['bsb']} does not match bank '{bank.get('bank')}' - check before sending",
                  file=sys.stderr)
    else:
        print(f"WARNING: BSB '{bank.get('bsb')}' is not 6 digits - check before sending", file=sys.stderr)
    return bank


def load_company():
    """Standard bill-to details (Ausway). Public company-register info, kept in invoice/company.json."""
    return json.loads((HERE / "company.json").read_text(encoding="utf-8"))


def parse_rate(v):
    """'10%*60%' -> (0.06, [0.1, 0.6]); '8.24%' -> (0.0824, [0.0824]); 15 -> 0.15; 0.12 -> 0.12."""
    if v in (None, "", "n/a", "N/A", "NA"):
        return None, []
    parts = re.split(r"\s*[*×xX]\s*", str(v).strip()) if isinstance(v, str) else [v]
    factors = []
    for p in parts:
        has_pct = isinstance(p, str) and "%" in p
        x = float(re.sub(r"[^\d.\-]", "", str(p)))
        factors.append(x / 100 if has_pct or x > 1 else x)
    prod = 1.0
    for x in factors:
        prod *= x
    return prod, factors


def build(details_path, profile_path, out_dir):
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))
    d = json.loads(Path(details_path).read_text(encoding="utf-8"))
    agent = profile["agent"]
    gst = profile.get("gst_registered")
    if gst is None:
        raise SystemExit(f"GST status of agent {agent.get('name')!r} is unknown. New agent: ask the user whether "
                         "the agent is registered for GST, then set \"gst_registered\": true/false in the profile.")
    bill = load_company()  # always the standard company details, never what an old invoice said
    bank = normalize_bank(profile["bank"])
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
    # Not registered for GST ("GST: N/A") -> plain "Invoice"; registered -> "Tax Invoice" with GST shown
    put("B1", "Tax Invoice" if gst else "Invoice", bold=True, size=14)
    put("B3", "Invoice to", bold=True)
    put("F3", "Invoice details", bold=True)
    put("B4", bill["name"])
    put("B5", "Company ABN:"); put("C5", bill.get("abn", ""), align="left")
    put("B6", "Company ACN:"); put("C6", bill.get("acn", ""), align="left")
    put("B7", "Address:"); put("C7", bill.get("address", ""))

    agent_line = agent["name"] + (f" (ABN {agent['abn']})" if agent.get("abn") else "")
    put("F4", "Name of the Agent:"); put("G4", agent_line)
    put("F5", "Address:"); put("G5", agent.get("address", ""))
    if len(agent.get("address", "")) > 50:  # long (e.g. overseas) address: wrap inside G5:J5
        ws.merge_cells("G5:J5")
        ws["G5"].alignment = Alignment(wrap_text=True, vertical="top")
        for ref in ("B5", "C5", "F5"):  # keep the rest of the row lined up with the wrapped text
            ws[ref].alignment = Alignment(horizontal=ws[ref].alignment.horizontal, vertical="top")
        ws.row_dimensions[5].height = 15 * -(-len(agent["address"]) // 58)  # ~58 characters per line
    put("F6", "Invoice date:"); put("G6", inv_date, fmt=DATE_FMT, align="left")
    put("F7", "Invoice No:"); put("G7", inv_no, align="left")

    ws.merge_cells("B8:J8")
    put("B8", d.get("heading", profile.get("heading", "Agent Commission Invoice")), bold=True, align="center")
    for col in "BCDEFGHIJ":
        ws[f"{col}8"].border = Border(bottom=Side(style="medium"))

    # ---- bank rows: optional ones (Swift code, A/C address) only when filled in ----
    bank_rows = [
        ("Bank:", bank.get("bank", ""), True),
        ("Bank Swift Code:", bank.get("swift", ""), False),
        ("A/C Name:", bank.get("account_name", ""), True),
        ("A/C NO:", bank.get("account_no", ""), True),
        ("BSB NO:", bank.get("bsb", ""), True),
        ("A/C Address:", bank.get("account_address", ""), False),
    ]
    bank_rows = [(label, val) for label, val, required in bank_rows if required or str(val or "").strip()]
    pay_row = 14 + len(bank_rows) + 1
    heading_row = pay_row + 2

    # ---- line items (placed first so remittance can reference the total) ----
    hdr = heading_row + 2
    first = hdr + 1
    last = hdr + len(students)
    incl = bool(d.get("amounts_include_gst"))
    if gst:
        # Subtotal / GST / Total rows; the payable total is the last one
        sub_row, gst_row, total_row = last + 1, last + 2, last + 3
    else:
        sub_row = gst_row = None
        total_row = last + 1

    # ---- remittance / bank --------------------------------------------------
    put("B10", "REMITTANCE ADVICE", bold=True)
    put("B11", "Total Payment"); put("C11", f"=H{total_row}", fmt=MONEY, align="left")
    put("D11", ccy)
    if gst:
        put("B12", "GST:"); put("C12", f"=H{gst_row}", fmt=MONEY, align="left")
        put("D12", "(included in Total Payment)")
    else:
        put("B12", "GST: N/A")
    for i, (label, val) in enumerate(bank_rows):
        r = 14 + i
        put(f"B{r}", label, bold=True)
        c = put(f"C{r}", str(val), align="left")
        c.number_format = "@"  # keep BSB / account numbers as text
    put(f"B{pay_row}", "Payment Method:", bold=True)
    put(f"C{pay_row}", profile.get("payment_method", "Bank Transfer"), bold=True)

    put(f"B{heading_row}", d.get("services_heading", "For Professional Services For Below Recommended Students"), bold=True)

    for i, name in enumerate(COLS):
        c = put(f"{TCOLS[i]}{hdr}", name, bold=True, align="center")
        c.border = BOX

    for r, s in enumerate(students, start=first):
        tuition = num(s.get("tuition"))
        rate, factors = parse_rate(s.get("rate"))
        amount = num(s.get("amount"))
        if amount is None and tuition is not None and rate is not None:
            # keep the stages visible in the formula, e.g. =ROUND(F26*0.1*0.6,2)
            amount_val = f"=ROUND(F{r}*" + "*".join(f"{x:g}" for x in factors) + ",2)"
        elif amount is not None:
            amount_val = amount
            # user rule (2026-11-08): rounding gaps under $1 follow the user's sheet without asking
            if tuition is not None and rate is not None and abs(round(tuition * rate, 2) - amount) >= ROUNDING_TOLERANCE:
                print(f"WARNING: {s.get('name')}: amount {amount} != tuition {tuition} x rate {s.get('rate')} "
                      f"= {round(tuition * rate, 2)} - check before sending", file=sys.stderr)
        else:
            raise SystemExit(f"Student {s.get('name')!r}: give 'amount', or 'tuition' + 'rate'")
        raw_rate = s.get("rate")
        # rate shown exactly as given ("10%*60%" stays a staged rate, "8.24%" a final rate)
        rate_val = raw_rate if isinstance(raw_rate, str) and raw_rate.strip() else (rate if rate is not None else "n/a")
        vals = [s.get("name", ""), s.get("institution", ""), s.get("course", ""), s.get("intake", ""),
                tuition if tuition is not None else "n/a", rate_val, amount_val, ccy]
        for col, v in zip(TCOLS, vals):
            c = put(f"{col}{r}", v, align="center")
            c.border = BOX
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if col == "F":
                c.number_format = '"$"#,##0'
            if col == "G" and isinstance(v, float):
                c.number_format = "0.00%"
            if col == "H":
                c.number_format = MONEY

    def total_line(row, label, formula, bold=False):
        ws.merge_cells(f"F{row}:G{row}")  # room for "Subtotal (excl. GST)"
        put(f"F{row}", label, bold=bold, align="right")
        ws[f"F{row}"].border = BOX
        ws[f"G{row}"].border = BOX
        put(f"H{row}", formula, bold=bold, fmt=MONEY, align="center").border = BOX
        put(f"I{row}", ccy, align="center").border = BOX

    line_sum = f"SUM(H{first}:H{last})"
    if gst and incl:
        total_line(sub_row, "Subtotal (excl. GST)", f"=H{total_row}-H{gst_row}")
        total_line(gst_row, "GST", f"=ROUND({line_sum}/11,2)")
        total_line(total_row, "Total (incl. GST)", f"={line_sum}", bold=True)
    elif gst:
        total_line(sub_row, "Subtotal (excl. GST)", f"={line_sum}")
        total_line(gst_row, f"GST {GST_RATE:.0%}", f"=ROUND(H{sub_row}*{GST_RATE},2)")
        total_line(total_row, "Total (incl. GST)", f"=H{sub_row}+H{gst_row}", bold=True)
    else:
        total_line(total_row, "Total", f"={line_sum}", bold=True)

    sig = total_row + 3
    put(f"B{sig}", "Name of the Agent:", bold=True)
    put(f"C{sig}", agent["name"])
    put(f"B{sig + 2}", "Signatory:", bold=True)
    ws[f"C{sig + 2}"].border = Border(bottom=thin)
    ws[f"D{sig + 2}"].border = Border(bottom=thin)
    put(f"F{sig + 2}", "Date:", bold=True)
    ws[f"G{sig + 2}"].border = Border(bottom=thin)
    if d.get("notes"):
        put(f"B{sig + 4}", "Notes:", bold=True)
        put(f"C{sig + 4}", d["notes"])
        sig += 2

    # ---- print setup: A4 portrait, fit to one page wide ---------------------
    ws.page_setup.paperSize = 9
    ws.page_setup.orientation = "portrait"
    ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.print_area = f"A1:J{sig + 4}"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_agent = re.sub(r"\W+", "_", agent["name"]).strip("_")
    # file_name lets the output follow the Received folder's naming, e.g. "Inv 20261110 Edu_Haoer Yu 8 Nov 2026.xlsx"
    out = out_dir / (d.get("file_name") or f"Inv_{inv_no}_{safe_agent}_{inv_date.strftime('%d_%b_%Y')}.xlsx")
    if out.exists():
        # Create-only rule: never overwrite an existing invoice (see CLAUDE.md)
        raise SystemExit(f"Refusing to overwrite existing file: {out}")
    wb.save(out)
    print(out)
    return out


def extract_profile(xlsx, out):
    """Pull agent / bank details out of an invoice in the original layout (bill-to comes from company.json)."""
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
    m = re.match(r"(.*?)\s*[(（]\s*ABN\s*[:：]?\s*([\d ]+)[)）]", agent_raw)
    agent_name, abn = (m.group(1).strip(), m.group(2).strip()) if m else (agent_raw.strip(), "")
    if not abn:
        abn = right_of(r"abn:", cols="FGH")  # some invoices put the agent ABN on its own row
    abn_d = digits(abn)
    if len(abn_d) == 11:
        abn = f"{abn_d[:2]} {abn_d[2:5]} {abn_d[5:8]} {abn_d[8:]}"
    gst_text = next((str(v) for v in cells.values() if isinstance(v, str) and re.match(r"\s*GST", v, re.I)), "")
    # "GST: N/A" on a past invoice -> not registered. Anything else stays unknown and must be confirmed.
    gst_registered = False if re.search(r"n/?a", gst_text, re.I) else None
    profile = {
        "gst_registered": gst_registered,
        "gst_source": f"past invoice says {gst_text.strip()!r}" if gst_text else "unknown - ask the user",
        "heading": "Agent Commission Invoice",
        "currency": "AUD",
        "payment_method": "Bank Transfer",
        "agent": {"name": agent_name or right_of(r"name of the agent$", cols="BC"), "abn": abn,
                  "address": right_of(r"address:", cols="FGH")},
        "bank": {"bank": right_of(r"bank:", cols="BC"), "swift": right_of(r"bank swift code:", cols="BC"),
                 "account_name": right_of(r"a/c name:?", cols="BC"), "account_no": right_of(r"a/c no:?", cols="BC"),
                 "bsb": right_of(r"bsb no:?", cols="BC"), "account_address": right_of(r"a/c address:?", cols="BC")},
    }
    profile["bank"] = normalize_bank(profile["bank"])
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
