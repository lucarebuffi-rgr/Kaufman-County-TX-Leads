#!/usr/bin/env python3
"""
Kaufman County TX – Motivated Seller Lead Scraper
Clerk  : kaufmancountytx-web.tylerhost.net (Tyler Technologies)
CAD    : kaufman-cad.org (fixed-width ZIP)
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import traceback
import zipfile
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL      = "https://kaufmancountytx-web.tylerhost.net/web/search/DOCSEARCH1008S7"
CAD_ZIP_URL   = "https://kaufman-cad.org/wp-content/uploads/2026/04/2026-Preliminary-Real-Roll-w-Improvement-Export.zip"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))

DOC_TYPES = {
    "LIS PENDENS"                              : ("pre_foreclosure", "Lis Pendens"),
    "FEDERAL TAX LIEN"                         : ("lien",            "Federal Tax Lien"),
    "STATE TAX LIEN"                           : ("lien",            "State Tax Lien"),
    "ABSTRACT OF JUDGMENT"                     : ("judgment",        "Abstract of Judgment"),
    "JUDGMENT"                                 : ("judgment",        "Judgment"),
    "PROBATE PROCEEDINGS, CERTIFIED COPY"      : ("probate",         "Probate"),
    "AFFIDAVIT OF HEIRSHIP"                    : ("probate",         "Affidavit of Heirship"),
    "LIEN AFFIDAVIT/CLAIM/NOTICE"              : ("lien",            "Lien"),
    "HOSPITAL LIEN"                            : ("lien",            "Hospital Lien"),
    "CHILD SUPPORT LIEN"                       : ("lien",            "Child Support Lien"),
    "ASSESSMENT LIEN BY HOMEOWNERS ASSOCIATION": ("lien",            "HOA Lien"),
    "DIVORCE PROCEEDINGS, CERTIFIED COPY"      : ("other",           "Divorce Decree"),
    "MEDICAL LIEN"                             : ("lien",            "Medical Lien"),
}

GRANTEE_IS_OWNER = {
    "LIEN AFFIDAVIT/CLAIM/NOTICE", "FEDERAL TAX LIEN", "STATE TAX LIEN",
    "JUDGMENT", "ABSTRACT OF JUDGMENT", "HOSPITAL LIEN", "MEDICAL LIEN",
    "CHILD SUPPORT LIEN", "ASSESSMENT LIEN BY HOMEOWNERS ASSOCIATION"
}

NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "TRUSTEE", "TR",
                 "ETAL", "ET", "AL", "ET AL", "ETUX", "ET UX", "ESTATE"}

ENTITY_FILTERS = (
    "LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "TRUST", "ASSOC", "HOMEOWNERS",
    "STATE OF", "CITY OF", "COUNTY OF", "DISTRICT", "MUNICIPALITY", "DEPT ",
    "ISD", "UTILITY", "AUTHORITY", "COMMISSION", "FEDERAL", "NATIONAL BANK",
    "MORTGAGE", "FINANCIAL", "INVESTMENT", "PROPERTIES", "REALTY", "HOLDINGS",
    "PARTNERS", "GROUP", "SERVICES", "MANAGEMENT", "SOLUTIONS", "ENTERPRISES",
    "N/A", "UNKNOWN", "PUBLIC", "ATTY GEN", "ATTY/GEN", "KAUFMAN COUNTY",
    "CITY OF TERRELL", "CITY OF KAUFMAN", "CITY OF FORNEY"
)

ACCT_S,  ACCT_E  = 596,  608
NAME_S,  NAME_E  = 608,  658
ADDR_S,  ADDR_E  = 753,  803
CITY_S,  CITY_E  = 873,  923
STAT_S,  STAT_E  = 923,  925
ZIP_S,   ZIP_E   = 978,  987
SNUM_S,  SNUM_E  = 4443, 4463
SITUS_S, SITUS_E = 1049, 1099
SCITY_S, SCITY_E = 1109, 1139
SZIP_S,  SZIP_E  = 1139, 1149
PCLS_S,  PCLS_E  = 2731, 2741


def parse_date(raw: str) -> Optional[str]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def strip_suffixes(tokens: list) -> list:
    return [t for t in tokens if t not in NAME_SUFFIXES]


def name_variants(full: str) -> list:
    full = re.sub(r"[^\w\s]", "", full.strip().upper())
    tokens = strip_suffixes(full.split())
    if not tokens:
        return [full]
    variants = set()
    variants.add(" ".join(tokens))
    if len(tokens) < 2:
        return list(variants)
    last  = tokens[0]
    first = tokens[1] if len(tokens) > 1 else ""
    mid   = tokens[2] if len(tokens) > 2 else ""
    variants.add(f"{last} {first} {mid}".strip())
    variants.add(f"{last}, {first} {mid}".strip())
    variants.add(f"{last} {first}")
    variants.add(f"{last}, {first}")
    variants.add(f"{first} {last}")
    if mid:
        variants.add(f"{first} {mid} {last}")
        variants.add(f"{first} {last}")
        if len(mid) == 1:
            variants.add(f"{last} {first}")
    return [v for v in variants if v]


def normalize_for_fuzzy(name: str) -> tuple:
    name = re.sub(r"[^\w\s]", "", name.strip().upper())
    tokens = strip_suffixes(name.split())
    filtered = [t for t in tokens if len(t) > 1]
    if len(filtered) >= 2:
        tokens = filtered
    if not tokens:
        return ("", set())
    return tokens[0], set(tokens[1:])


def is_entity(name: str) -> bool:
    n = name.strip().upper()
    if not n or n in ("N/A", "NA", "UNKNOWN", "PUBLIC", ""):
        return True
    tokens = [t for t in re.sub(r"[^\w\s]", "", n).split() if len(t) > 1]
    if len(tokens) < 2:
        return True
    return any(x in n for x in ENTITY_FILTERS)


def build_parcel_lookup() -> dict:
    lookup = {}
    log.info("Downloading Kaufman CAD data ...")
    try:
        resp = httpx.get(CAD_ZIP_URL, timeout=120, follow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        log.info(f"  Downloaded {len(resp.content)/1_048_576:.1f} MB")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        fname = next(
            (n for n in zf.namelist()
             if "APPRAISAL_INFO" in n.upper() and not n.upper().endswith(".PDF")),
            None
        )
        if not fname:
            fname = next(
                (n for n in zf.namelist()
                 if n.upper().endswith(".TXT") and "INFO" in n.upper()),
                None
            )
        if not fname:
            log.error(f"Could not find data file. Files: {zf.namelist()}")
            return lookup
        log.info(f"  Parsing {fname} ...")
        raw   = zf.read(fname).decode("latin-1")
        total = 0
        for line in raw.splitlines():
            if len(line) < PCLS_E:
                continue
            prop_class = line[PCLS_S:PCLS_E].strip()
            if not (prop_class.startswith("A") or prop_class.startswith("E")):
                continue
            owner_name = line[NAME_S:NAME_E].strip().upper()
            if not owner_name or is_entity(owner_name):
                continue
            mail_addr  = line[ADDR_S:ADDR_E].strip()
            mail_city  = line[CITY_S:CITY_E].strip()
            mail_state = line[STAT_S:STAT_E].strip() or "TX"
            mail_zip   = line[ZIP_S:ZIP_E].strip()[:5]
            situs_num  = line[SNUM_S:SNUM_E].strip() if len(line) > SNUM_E else ""
            situs_st   = f"{situs_num} {line[SITUS_S:SITUS_E].strip()}".strip()
            situs_city = line[SCITY_S:SCITY_E].strip()
            situs_zip  = line[SZIP_S:SZIP_E].strip()[:5]
            parcel = {
                "prop_address": situs_st,
                "prop_city":    situs_city or "Kaufman",
                "prop_state":   "TX",
                "prop_zip":     situs_zip,
                "mail_address": mail_addr,
                "mail_city":    mail_city,
                "mail_state":   mail_state,
                "mail_zip":     mail_zip,
            }
            for variant in name_variants(owner_name):
                lookup[variant] = parcel
            total += 1
            if total % 10000 == 0:
                log.info(f"  Processed {total:,} parcels ...")
        log.info(f"Kaufman CAD lookup: {len(lookup):,} name variants from {total:,} parcels")
    except Exception:
        log.error(f"CAD lookup error:\n{traceback.format_exc()}")
    return lookup


def parse_results_html(html: str, doc_type: str, cat: str, cat_label: str) -> list:
    """Parse search results from HTML response."""
    records = []
    try:
        # Find all instrument numbers (format: 2026-XXXXXXX)
        instruments = re.findall(r'(\d{4}-\d{5,})', html)
        # Find all dates
        dates = re.findall(r'(\d{2}/\d{2}/\d{4})', html)
        # Find grantor/grantee patterns
        grantor_matches = re.findall(
            r'(?:Grantor|GRANTOR)[:\s]+([A-Z][A-Z\s,\.]+?)(?:\n|<|Grantee)',
            html, re.IGNORECASE
        )
        grantee_matches = re.findall(
            r'(?:Grantee|GRANTEE)[:\s]+([A-Z][A-Z\s,\.]+?)(?:\n|<|Grantor|Recording)',
            html, re.IGNORECASE
        )

        log.info(f"  Found {len(instruments)} instruments, {len(dates)} dates, "
                 f"{len(grantor_matches)} grantors, {len(grantee_matches)} grantees")

        seen = set()
        for i, instr in enumerate(instruments):
            if instr in seen:
                continue
            seen.add(instr)
            filed   = dates[i] if i < len(dates) else ""
            grantor = grantor_matches[i].strip() if i < len(grantor_matches) else ""
            grantee = grantee_matches[i].strip() if i < len(grantee_matches) else ""
            records.append({
                "doc_num"  : instr,
                "doc_type" : doc_type,
                "cat"      : cat,
                "cat_label": cat_label,
                "filed"    : parse_date(filed) or filed,
                "grantor"  : grantor,
                "grantee"  : grantee,
                "legal"    : "",
                "amount"   : None,
                "clerk_url": BASE_URL,
                "_demo"    : False,
            })
    except Exception:
        log.error(f"Parse error:\n{traceback.format_exc()}")
    return records


async def scrape_all(date_from: str, date_to: str) -> list:
    all_records = []
    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=60
    ) as client:
        # Step 1 — load disclaimer page
        r = await client.get(BASE_URL)
        log.info(f"  Disclaimer page: {r.status_code} url={r.url}")

        # Step 2 — accept disclaimer via POST to /web/user/disclaimer
        disclaimer_url = "https://kaufmancountytx-web.tylerhost.net/web/user/disclaimer"
        # Find hidden inputs on disclaimer page
        hidden = dict(re.findall(
            r'<input[^>]+type=["\']hidden["\'][^>]+name=["\']([^"\']+)["\'][^>]+value=["\']([^"\']*)["\']',
            r.text, re.I
        ))
        log.info(f"  Hidden inputs: {hidden}")
        hidden["disclaimer"] = "accept"
        hidden["submit"]     = "Accept"

        r2 = await client.post(disclaimer_url, data=hidden)
        log.info(f"  Disclaimer POST: {r2.status_code} url={r2.url}")
        log.info(f"  Cookies after disclaimer: {list(client.cookies.keys())}")

        # Step 3 — load search page to confirm we're in
        r3 = await client.get(BASE_URL)
        log.info(f"  Search page: {r3.status_code} url={r3.url} len={len(r3.text)}")

        # If still on disclaimer, try different approach
        if "disclaimer" in str(r3.url).lower():
            log.warning("  Still on disclaimer — trying GET with param")
            r3 = await client.get(
                "https://kaufmancountytx-web.tylerhost.net/web/user/disclaimer",
                params={"disclaimer": "accept"}
            )
            log.info(f"  GET accept: {r3.status_code} url={r3.url}")
            r3 = await client.get(BASE_URL)
            log.info(f"  Search after GET: {r3.status_code} url={r3.url} len={len(r3.text)}")

        # Log the search page content to find form fields
        log.info(f"  Search page snippet 3000-4000: {r3.text[3000:4000]}")

        # Step 4 — find all hidden inputs on search page
        search_hidden = dict(re.findall(
            r'<input[^>]+type=["\']hidden["\'][^>]+name=["\']([^"\']+)["\'][^>]+value=["\']([^"\']*)["\']',
            r3.text, re.I
        ))
        log.info(f"  Search hidden inputs: {search_hidden}")

        # Step 5 — POST search for each doc type
        for doc_type, (cat, cat_label) in list(DOC_TYPES.items())[:2]:  # test first 2 only
            try:
                form_data = {**search_hidden}
                form_data["field_RecDateID_DOT_StartDate"] = date_from
                form_data["field_RecDateID_DOT_EndDate"]   = date_to
                form_data["field_DocTypeID"]               = doc_type
                form_data["submit"]                        = "Search"

                resp = await client.post(BASE_URL, data=form_data)
                log.info(f"  {doc_type}: {resp.status_code} len={len(resp.text)}")
                log.info(f"  Snippet 3000-4000: {resp.text[3000:4000]}")

            except Exception as e:
                log.warning(f"  {doc_type} failed: {e}")

    return all_records


def generate_demo_records(date_from: str, date_to: str) -> list:
    samples = [
        ("LIS PENDENS",                               "pre_foreclosure", "Lis Pendens",
         "SMITH ROBERT",    "ROCKET MORTGAGE",   0),
        ("ABSTRACT OF JUDGMENT",                      "judgment",        "Abstract of Judgment",
         "JONES MARY B",    "CAPITAL ONE",    87500),
        ("FEDERAL TAX LIEN",                          "lien",            "Federal Tax Lien",
         "WILLIAMS DAVID",  "IRS",            45200),
        ("JUDGMENT",                                  "judgment",        "Judgment",
         "JOHNSON PAT",     "CITIBANK",       18700),
        ("LIEN AFFIDAVIT/CLAIM/NOTICE",               "lien",            "Lien",
         "BROWN MICHAEL",   "ACME CONTR",     22000),
        ("PROBATE PROCEEDINGS, CERTIFIED COPY",       "probate",         "Probate",
         "DAVIS JAMES EST", "KAUFMAN PROB",       0),
        ("STATE TAX LIEN",                            "lien",            "State Tax Lien",
         "HENDERSON BOB",   "STATE OF TX",     9800),
        ("ASSESSMENT LIEN BY HOMEOWNERS ASSOCIATION", "lien",            "HOA Lien",
         "RODRIGUEZ JUAN",  "FORNEY HOA",      5000),
        ("AFFIDAVIT OF HEIRSHIP",                     "probate",         "Affidavit of Heirship",
         "GARCIA CARLOS",   "GARCIA MARIA",       0),
    ]
    base = datetime.strptime(date_from, "%m/%d/%Y")
    recs = []
    for i, (code, cat, cat_label, grantor, grantee, amt) in enumerate(samples):
        filed_dt = base + timedelta(days=i % LOOKBACK_DAYS)
        recs.append({
            "doc_num":   f"2026-DEMO-{i+1:04d}",
            "doc_type":  code,
            "cat":       cat,
            "cat_label": cat_label,
            "filed":     filed_dt.strftime("%Y-%m-%d"),
            "grantor":   grantor,
            "grantee":   grantee,
            "legal":     "DEMO RECORD",
            "amount":    float(amt) if amt else None,
            "clerk_url": BASE_URL,
            "_demo":     True,
        })
    return recs


def enrich_with_parcel(records: list, lookup: dict) -> list:
    fuzzy_index = []
    seen = set()
    for variant, parcel in lookup.items():
        last, firsts = normalize_for_fuzzy(variant)
        key = (last, frozenset(firsts))
        if last and key not in seen:
            seen.add(key)
            fuzzy_index.append((last, firsts, parcel))
    matched = 0
    for rec in records:
        dtype  = rec.get("doc_type", "")
        owner  = (rec.get("grantee") if dtype in GRANTEE_IS_OWNER
                  else rec.get("grantor") or "").upper().strip()
        parcel = None
        if is_entity(owner):
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")
            continue
        for variant in name_variants(owner):
            parcel = lookup.get(variant)
            if parcel:
                break
        if not parcel and owner:
            o_last, o_firsts = normalize_for_fuzzy(owner)
            if o_last and o_firsts:
                for c_last, c_firsts, candidate in fuzzy_index:
                    if c_last != o_last:
                        continue
                    if not c_firsts:
                        continue
                    if o_firsts & c_firsts:
                        parcel = candidate
                        break
                    o_str = " ".join(sorted(o_firsts))
                    c_str = " ".join(sorted(c_firsts))
                    if o_str and c_str and SequenceMatcher(
                            None, o_str, c_str).ratio() >= 0.85:
                        parcel = candidate
                        break
        if parcel:
            rec.update(parcel)
            matched += 1
        else:
            rec.setdefault("prop_address", "")
            rec.setdefault("prop_city",    "")
            rec.setdefault("prop_state",   "TX")
            rec.setdefault("prop_zip",     "")
            rec.setdefault("mail_address", "")
            rec.setdefault("mail_city",    "")
            rec.setdefault("mail_state",   "TX")
            rec.setdefault("mail_zip",     "")
    log.info(f"Parcel enrichment: {matched}/{len(records)} records matched")
    return records


def score_record(rec: dict) -> tuple:
    score = 30
    flags = []
    dtype  = rec.get("doc_type", "")
    amount = rec.get("amount") or 0
    if dtype == "LIS PENDENS":                               flags.append("Lis pendens")
    if dtype in ("FEDERAL TAX LIEN", "STATE TAX LIEN"):      flags.append("Tax lien")
    if dtype in ("JUDGMENT", "ABSTRACT OF JUDGMENT"):        flags.append("Judgment lien")
    if dtype in ("PROBATE PROCEEDINGS, CERTIFIED COPY",
                 "AFFIDAVIT OF HEIRSHIP"):                   flags.append("Probate / estate")
    if dtype == "LIEN AFFIDAVIT/CLAIM/NOTICE":               flags.append("Lien")
    if dtype in ("HOSPITAL LIEN", "MEDICAL LIEN"):           flags.append("Hospital / Medical lien")
    if dtype == "CHILD SUPPORT LIEN":                        flags.append("Child support lien")
    if dtype == "ASSESSMENT LIEN BY HOMEOWNERS ASSOCIATION": flags.append("HOA lien")
    if dtype == "DIVORCE PROCEEDINGS, CERTIFIED COPY":       flags.append("Divorce")
    try:
        filed = datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")
        if (datetime.today() - filed).days <= 14:
            flags.append("New this week")
    except Exception:
        pass
    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    score += 10 * len(flags)
    if "Lis pendens" in flags:      score += 20
    if "Probate / estate" in flags: score += 10
    if "Tax lien" in flags:         score += 10
    if amount and amount > 100_000: score += 15
    elif amount and amount > 50_000: score += 10
    if "New this week" in flags:    score += 5
    if has_addr:                    score += 5
    return min(score, 100), flags


def build_output(raw_records: list, date_from: str, date_to: str) -> dict:
    seen_docs   = set()
    out_records = []
    for raw in raw_records:
        try:
            doc_num = raw.get("doc_num", "")
            if doc_num and doc_num in seen_docs:
                continue
            if doc_num:
                seen_docs.add(doc_num)
            dtype = raw.get("doc_type", "")
            if dtype in GRANTEE_IS_OWNER:
                owner   = raw.get("grantee", "")
                grantee = raw.get("grantor", "")
            else:
                owner   = raw.get("grantor", "")
                grantee = raw.get("grantee", "")
            if not owner:
                continue
            score, flags = score_record({**raw, "owner": owner})
            out_records.append({
                "doc_num":      doc_num,
                "doc_type":     dtype,
                "filed":        raw.get("filed", ""),
                "cat":          raw.get("cat", "other"),
                "cat_label":    raw.get("cat_label", ""),
                "owner":        owner,
                "grantee":      grantee,
                "amount":       raw.get("amount"),
                "legal":        raw.get("legal", ""),
                "prop_address": raw.get("prop_address", ""),
                "prop_city":    raw.get("prop_city", ""),
                "prop_state":   raw.get("prop_state", "TX"),
                "prop_zip":     raw.get("prop_zip", ""),
                "mail_address": raw.get("mail_address", ""),
                "mail_city":    raw.get("mail_city", ""),
                "mail_state":   raw.get("mail_state", "TX"),
                "mail_zip":     raw.get("mail_zip", ""),
                "clerk_url":    raw.get("clerk_url", ""),
                "flags":        flags,
                "score":        score,
                "_demo":        raw.get("_demo", False),
            })
        except Exception:
            log.warning(f"Skipping: {traceback.format_exc()}")
    out_records = [r for r in out_records if not is_entity(r.get("owner", ""))]
    out_records = [r for r in out_records if not any(
        x in (r.get("owner", "")).upper() for x in ENTITY_FILTERS
    )]
    out_records.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""))
    with_address = sum(1 for r in out_records if r["prop_address"] or r["mail_address"])
    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Kaufman County TX – Tyler Technologies",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(out_records),
        "with_address": with_address,
        "records":      out_records,
    }


def save_output(data: dict):
    for path in ["dashboard/records.json", "data/records.json"]:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2))
        log.info(f"Saved {data['total']} records → {path}")


def export_ghl_csv(data: dict):
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in data["records"]:
        parts = (r.get("owner", "")).split()
        writer.writerow({
            "First Name":             parts[0] if parts else "",
            "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
            "Mailing Address":        r.get("mail_address", ""),
            "Mailing City":           r.get("mail_city", ""),
            "Mailing State":          r.get("mail_state", "TX"),
            "Mailing Zip":            r.get("mail_zip", ""),
            "Property Address":       r.get("prop_address", ""),
            "Property City":          r.get("prop_city", ""),
            "Property State":         r.get("prop_state", "TX"),
            "Property Zip":           r.get("prop_zip", ""),
            "Lead Type":              r.get("cat_label", ""),
            "Document Type":          r.get("doc_type", ""),
            "Date Filed":             r.get("filed", ""),
            "Document Number":        r.get("doc_num", ""),
            "Amount/Debt Owed":       str(r.get("amount", "") or ""),
            "Seller Score":           str(r.get("score", "")),
            "Motivated Seller Flags": "|".join(r.get("flags", [])),
            "Source":                 "Kaufman County TX",
            "Public Records URL":     r.get("clerk_url", ""),
        })
    Path("data/ghl_export.csv").write_text(buf.getvalue())
    log.info("GHL CSV saved")


async def main():
    today     = datetime.today()
    start     = today - timedelta(days=LOOKBACK_DAYS)
    date_from = start.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")

    log.info("=== Kaufman County TX Lead Scraper ===")
    log.info(f"Date range: {date_from} → {date_to}")

    log.info("Building parcel lookup ...")
    parcel_lookup = build_parcel_lookup()
    log.info(f"  {len(parcel_lookup):,} name variants indexed")

    log.info("Scraping clerk records ...")
    raw_records = await scrape_all(date_from, date_to)
    log.info(f"Total raw records: {len(raw_records)}")

    if not raw_records:
        log.warning("No live records – using demo data")
        raw_records = generate_demo_records(date_from, date_to)

    raw_records = enrich_with_parcel(raw_records, parcel_lookup)
    data = build_output(raw_records, date_from, date_to)
    save_output(data)
    export_ghl_csv(data)
    log.info(f"Done. {data['total']} leads | {data['with_address']} with address")


if __name__ == "__main__":
    asyncio.run(main())
