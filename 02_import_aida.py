"""
Import AIDA data from Google Sheets CSV → Supabase
====================================================
Uso:
    python 02_import_aida.py                    # scarica da Google Sheets
    python 02_import_aida.py spurghi.csv        # usa file locale

Variabili d'ambiente richieste (in .env o Railway):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
    OPENAI_API_KEY
"""

from __future__ import annotations
import os, re, time, unicodedata, logging, difflib, json
from datetime import datetime

import requests
import csv
import io
from dotenv import load_dotenv
from supabase import create_client, Client
from openai import OpenAI

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_ID  = "1BzcKrG1JhuiKhbivMyFBXXmqdVuRui1Qerk4WGNZw48"
GID       = "1243837551"
CSV_URL   = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
OPENAI_KEY   = os.environ["OPENAI_API_KEY"]

BATCH_SIZE        = 20   # record per batch (embedding + upsert)
EMBEDDING_MODEL   = "text-embedding-3-small"
EMBED_DELAY_S     = 0.5  # pausa tra batch per rate-limit

# ── Clients ───────────────────────────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client    = OpenAI(api_key=OPENAI_KEY)

# ── Helpers ───────────────────────────────────────────────────────────────────
def slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")[:120]

def to_bigint(val: str) -> int | None:
    """
    Converte una stringa numerica in intero gestendo sia il formato italiano
    (punto = migliaia, virgola = decimale: 1.234.567,89) che il formato US/AIDA
    (virgola = migliaia, punto = decimale: 1,234,567.89).
    """
    if not val or val.strip() == "":
        return None
    val = val.strip().lstrip("-+")  # ignora segno per ora
    negative = val != val.lstrip("-+") and val.startswith("-")  # ricostruisce segno
    val = val.strip().replace(" ", "")
    try:
        has_comma  = "," in val
        has_period = "." in val
        if has_comma and has_period:
            # Entrambi: il separatore che viene DOPO è il decimale
            if val.rfind(".") > val.rfind(","):
                # US format: 1,234,567.89 → rimuovi virgole
                clean = val.replace(",", "")
            else:
                # IT format: 1.234.567,89 → rimuovi punti, virgola→punto
                clean = val.replace(".", "").replace(",", ".")
        elif has_comma:
            # Solo virgola: se tutti i blocchi dopo la prima virgola hanno 3 cifre → migliaia
            parts = val.split(",")
            if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
                clean = val.replace(",", "")   # US thousands separator
            else:
                clean = val.replace(",", ".")  # decimale
        else:
            # Solo punti o nessuno
            parts = val.split(".")
            if len(parts) > 2 or (len(parts) == 2 and len(parts[-1]) == 3):
                clean = val.replace(".", "")   # IT thousands separator
            else:
                clean = val                    # decimale o intero
        result = int(float(clean))
        return -result if negative else result
    except Exception:
        return None

def to_numeric(val: str) -> float | None:
    if not val or val.strip() == "":
        return None
    try:
        return float(val.replace(",", "."))
    except Exception:
        return None

def to_date(val: str) -> str | None:
    if not val or val.strip() == "":
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def build_embedding_text(row: dict) -> str:
    parts = [
        row.get("ragione_sociale", ""),
        f"ATECO {row.get('ateco_codice', '')}",
        f"Regione: {row.get('regione', '')}",
        f"Provincia: {row.get('provincia', '')}",
        f"Comune: {row.get('comune', '')}",
    ]
    if row.get("ricavi_0"):
        parts.append(f"Ricavi: {row['ricavi_0']:,} EUR")
    if row.get("ebitda_0"):
        parts.append(f"EBITDA: {row['ebitda_0']:,} EUR")
    if row.get("ebitda_margin_0"):
        parts.append(f"EBITDA margin: {row['ebitda_margin_0']}%")
    # Includi descrizione_web se disponibile in altro
    desc = (row.get("altro") or {}).get("descrizione_web", "")
    if desc:
        parts.append(desc)
    return " | ".join(p for p in parts if p.strip() and p.strip() not in ["ATECO ", "Regione: ", "Provincia: ", "Comune: "])

def get_embeddings(texts: list[str]) -> list[list[float]]:
    resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in resp.data]


# =============================================================================
# Column detection — fuzzy + year-based
# =============================================================================

# Canonical column name → list of accepted variants (lowercase)
KNOWN_COLS: dict[str, list[str]] = {
    "ragione_sociale":    ["ragione sociale", "ragione soc", "nome azienda", "company"],
    "interesse":          ["interesse a vendere", "interesse vendere", "interesse"],
    "note":               ["note", "notes", "annotazioni"],
    "contatti":           ["contatti", "contatto", "contacts"],
    "next_steps":         ["next steps", "next step", "prossimi passi", "azioni"],
    "partita_iva":        ["partita iva", "p.iva", "piva", "vat"],
    "ateco_codice":       ["ateco 2007 codice", "ateco codice", "ateco 2007", "ateco"],
    "regione":            ["sede operativa - regione - regione", "sede operativa - regione", "regione", "region"],
    "provincia":          ["sede operativa - provincia", "provincia", "province"],
    "comune":             ["sede operativa - comune", "comune", "city", "citta"],
    "telefono":           ["numero di telefono", "telefono", "tel", "phone"],
    "website":            ["website", "sito web", "sito", "web"],
    "data_bilancio":      ["data di chiusura", "data chiusura", "data bilancio", "data di chiusura ultimo bilancio"],
    "azionisti":          ["azionisti nome", "azionisti", "shareholders"],
    "csh_nome":           ["csh nome", "csh"],
    "dm_nome":            ["dm nome completo", "dm nome", "decision maker", "dm"],
    "dm_codice_fiscale":  ["dm codice fiscale", "codice fiscale dm"],
    "esclusiva":          ["esclusiva", "exclusive"],
}

# Year regex
YEAR_RE = re.compile(r"\b(20\d{2})\b")

# Financial column keywords
def _is_ebitda(h: str) -> bool:
    hl = h.lower()
    return "ebitda" in hl and "%" not in hl and "vendite" not in hl and "margin" not in hl

def _is_margin(h: str) -> bool:
    hl = h.lower()
    return ("ebitda" in hl and ("%" in hl or "vendite" in hl)) or "margine" in hl

def _is_ricavi(h: str) -> bool:
    hl = h.lower()
    return "ricavi" in hl and "ebitda" not in hl


def detect_columns(header: list[str]) -> tuple[dict[str, int], dict[int, str], dict[str, dict[int, int]]]:
    """
    Returns:
        field_map:   field_name → column_index   (for scalar fields)
        unmatched:   column_index → header_name  (for 'altro' JSONB)
        fin_map:     'ebitda'/'ricavi'/'margin' → {year: col_idx}
    """
    header_lower = [h.lower().strip() for h in header]
    field_map: dict[str, int] = {}
    claimed: set[int] = set()

    # ── 1. Financial columns (by year keyword) ────────────────────────────────
    fin_map: dict[str, dict[int, int]] = {"ebitda": {}, "margin": {}, "ricavi": {}}
    for i, h in enumerate(header):
        ym = YEAR_RE.search(h)
        if not ym:
            continue
        year = int(ym.group(1))
        if _is_ebitda(h):
            fin_map["ebitda"][year] = i
            claimed.add(i)
        elif _is_margin(h):
            fin_map["margin"][year] = i
            claimed.add(i)
        elif _is_ricavi(h):
            fin_map["ricavi"][year] = i
            claimed.add(i)

    # ── 2. Scalar fields (exact then fuzzy) ───────────────────────────────────
    for field, variants in KNOWN_COLS.items():
        # exact match first, then substring containment
        found = False
        variants_lower = [v.lower() for v in variants]
        for i, hl in enumerate(header_lower):
            if i in claimed:
                continue
            if hl in variants_lower:
                field_map[field] = i
                claimed.add(i)
                found = True
                break
        if not found:
            # substring containment: any variant appears in the header
            for i, hl in enumerate(header_lower):
                if i in claimed:
                    continue
                if any(v in hl for v in variants_lower):
                    field_map[field] = i
                    claimed.add(i)
                    found = True
                    break
        if found:
            continue
        # fuzzy match
        best_score = 0.0
        best_idx   = None
        for i, hl in enumerate(header_lower):
            if i in claimed:
                continue
            for v in variants:
                ratio = difflib.SequenceMatcher(None, hl, v.lower()).ratio()
                if ratio > best_score:
                    best_score = ratio
                    best_idx   = i
        if best_idx is not None and best_score >= 0.72:
            log.info(f"  Fuzzy match: '{header[best_idx]}' → '{field}' (score={best_score:.2f})")
            field_map[field] = best_idx
            claimed.add(best_idx)

    # ── 3. Unmatched → altro ──────────────────────────────────────────────────
    unmatched: dict[int, str] = {
        i: header[i] for i in range(len(header))
        if i not in claimed and header[i].strip()
    }

    return field_map, unmatched, fin_map


def _assign_financial_slots(fin_years: dict[int, int], row: list[str],
                             col_names: list[str]) -> tuple[list, list, list]:
    """Sort years desc, return (values_0_to_4, years_0_to_4, margin_values)."""
    sorted_years = sorted(fin_years.keys(), reverse=True)
    values = []
    years  = []
    for yr in sorted_years[:5]:
        idx = fin_years[yr]
        values.append(idx)
        years.append(yr)
    # pad to 5
    while len(values) < 5:
        values.append(None)
        years.append(None)
    return values, years


# ── CSV Parsing ───────────────────────────────────────────────────────────────
def fetch_csv(local_file: str | None = None) -> list[dict]:
    if local_file:
        log.info(f"Leggendo CSV da file locale: {local_file}")
        with open(local_file, "r", encoding="utf-8") as f:
            content = f.read()
    else:
        log.info("Scaricando CSV da Google Sheets…")
        resp = requests.get(CSV_URL, timeout=60)
        resp.raise_for_status()
        content = resp.text

    reader = csv.reader(io.StringIO(content))
    rows   = list(reader)
    if not rows:
        log.error("CSV vuoto!")
        return []

    # Normalize headers: replace newlines/tabs with space, collapse whitespace
    header = [re.sub(r'\s+', ' ', h).strip() for h in rows[0]]
    log.info(f"Header rilevato: {header}")

    field_map, unmatched_cols, fin_map = detect_columns(header)
    log.info(f"  Campo mappati: {list(field_map.keys())}")
    log.info(f"  EBITDA anni trovati: {sorted(fin_map['ebitda'].keys(), reverse=True)}")
    log.info(f"  Ricavi anni trovati: {sorted(fin_map['ricavi'].keys(), reverse=True)}")
    log.info(f"  Margin anni trovati: {sorted(fin_map['margin'].keys(), reverse=True)}")
    if unmatched_cols:
        log.info(f"  Colonne non mappate (→ altro): {list(unmatched_cols.values())}")

    # Pre-sort financial year lists
    ebitda_years_sorted = sorted(fin_map["ebitda"].keys(), reverse=True)[:5]
    ricavi_years_sorted = sorted(fin_map["ricavi"].keys(), reverse=True)[:5]
    margin_years_sorted = sorted(fin_map["margin"].keys(), reverse=True)[:5]

    # Use ebitda years as canonical anno_X (fallback to ricavi)
    canonical_years = ebitda_years_sorted or ricavi_years_sorted

    def gcell(row: list[str], field: str) -> str:
        """Get cell value for a mapped field, empty string if not mapped."""
        idx = field_map.get(field)
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    records  = []
    seen_slugs: set[str] = set()

    for i, row in enumerate(rows[1:], start=2):
        # Pad row if needed
        while len(row) < len(header):
            row.append("")

        ragione = gcell(row, "ragione_sociale")
        if not ragione:
            continue

        # Slug dedup
        base_slug = slugify(ragione)
        slug      = base_slug
        counter   = 1
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)

        # sheet_row (first column, usually blank header)
        sheet_row_val = row[0].strip() if row else ""
        try:
            sheet_row_int = int(float(sheet_row_val)) if sheet_row_val else None
        except ValueError:
            sheet_row_int = None

        # Interest flag (column "Interesse a vendere" = 1)
        interesse_raw    = gcell(row, "interesse")
        is_interessante  = (interesse_raw == "1")
        livello_interesse = "chiaro" if is_interessante else None

        # Esclusiva flag
        esclusiva_raw = gcell(row, "esclusiva")
        esclusiva     = (esclusiva_raw == "1")

        # ── Financial data ────────────────────────────────────────────────────
        def fin_val(fin_dict: dict[int, int], year: int, converter) -> any:
            idx = fin_dict.get(year)
            if idx is None or idx >= len(row):
                return None
            return converter(row[idx])

        # EBITDA slots 0-4 (most recent first)
        ebitda_vals = [fin_val(fin_map["ebitda"], yr, to_bigint)   for yr in ebitda_years_sorted] + [None]*(5-len(ebitda_years_sorted))
        ricavi_vals = [fin_val(fin_map["ricavi"], yr, to_bigint)   for yr in ricavi_years_sorted] + [None]*(5-len(ricavi_years_sorted))
        margin_vals = [fin_val(fin_map["margin"], yr, to_numeric)  for yr in margin_years_sorted] + [None]*(5-len(margin_years_sorted))

        # anno_X = actual calendar year for slot X
        anno_vals = list(canonical_years) + [None]*(5-len(canonical_years))

        # ── Altro: unmatched columns ──────────────────────────────────────────
        # Alcune colonne hanno nomi diversi nei vari fogli ma corrispondono
        # allo stesso campo interno. Rinominare prima di inserire in altro.
        _ALTRO_REMAP = {
            "description": "descrizione_web",  # campagna email
            "descrizione": "descrizione_web",
        }
        altro: dict[str, str] = {}
        for col_idx, col_name in unmatched_cols.items():
            if col_idx < len(row) and row[col_idx].strip():
                key = _ALTRO_REMAP.get(col_name.lower().strip(), col_name)
                altro[key] = row[col_idx].strip()

        rec = {
            "slug":               slug,
            "ragione_sociale":    ragione,
            "sheet_row":          sheet_row_int,
            "note":               gcell(row, "note")            or None,
            "contatti":           gcell(row, "contatti")        or None,
            "next_steps":         gcell(row, "next_steps")      or None,
            "is_interessante":    is_interessante,
            "livello_interesse":  livello_interesse,
            "esclusiva":          esclusiva,
            "partita_iva":        gcell(row, "partita_iva")     or None,
            "ateco_codice":       gcell(row, "ateco_codice")    or None,
            "regione":            gcell(row, "regione")         or None,
            "provincia":          gcell(row, "provincia")       or None,
            "comune":             gcell(row, "comune")          or None,
            "telefono":           gcell(row, "telefono")        or None,
            "website":            gcell(row, "website")         or None,
            "data_bilancio":      to_date(gcell(row, "data_bilancio")),
            "ebitda_0":           ebitda_vals[0],
            "ebitda_1":           ebitda_vals[1],
            "ebitda_2":           ebitda_vals[2],
            "ebitda_3":           ebitda_vals[3],
            "ebitda_4":           ebitda_vals[4],
            "ebitda_margin_0":    margin_vals[0],
            "ebitda_margin_1":    margin_vals[1],
            "ebitda_margin_2":    margin_vals[2],
            "ebitda_margin_3":    margin_vals[3],
            "ebitda_margin_4":    margin_vals[4],
            "ricavi_0":           ricavi_vals[0],
            "ricavi_1":           ricavi_vals[1],
            "ricavi_2":           ricavi_vals[2],
            "ricavi_3":           ricavi_vals[3],
            "ricavi_4":           ricavi_vals[4],
            "anno_0":             anno_vals[0],
            "anno_1":             anno_vals[1],
            "anno_2":             anno_vals[2],
            "anno_3":             anno_vals[3],
            "anno_4":             anno_vals[4],
            "azionisti":          gcell(row, "azionisti")       or None,
            "csh_nome":           gcell(row, "csh_nome")        or None,
            "dm_nome":            gcell(row, "dm_nome")         or None,
            "dm_codice_fiscale":  gcell(row, "dm_codice_fiscale") or None,
            "altro":              altro if altro else None,
        }
        records.append(rec)

    log.info(f"Parsed {len(records)} aziende dal CSV")
    interessanti = sum(1 for r in records if r["is_interessante"])
    esclusive    = sum(1 for r in records if r["esclusiva"])
    log.info(f"  → {interessanti} interessanti, {esclusive} in esclusiva")
    return records


# ── Campi protetti in altro (non vengono mai sovrascritti durante l'import) ────
PROTECTED = {"descrizione_web"}


def _merge_altro(new_altro: dict | None, existing_altro: dict | None) -> dict | None:
    """
    Unisce new_altro con existing_altro preservando i campi in PROTECTED.
    I campi protetti vengono mantenuti dal DB esistente; tutti gli altri
    vengono aggiornati dal nuovo import.
    """
    merged = dict(new_altro or {})
    if existing_altro:
        for key in PROTECTED:
            if key in existing_altro:
                merged[key] = existing_altro[key]   # proteggi il valore esistente
    return merged if merged else None


def _fetch_existing_altro(slugs: list[str]) -> dict[str, dict]:
    """Ritorna {slug: altro} per le aziende già presenti nel DB."""
    if not slugs:
        return {}
    res = supabase.table("companies").select("slug,altro").in_("slug", slugs).execute()
    return {r["slug"]: (r.get("altro") or {}) for r in res.data}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    import sys
    local_file = sys.argv[1] if len(sys.argv) > 1 else None
    records = fetch_csv(local_file)
    total    = len(records)
    inserted = 0
    errors   = 0

    for start in range(0, total, BATCH_SIZE):
        batch = records[start : start + BATCH_SIZE]
        texts = [build_embedding_text(r) for r in batch]

        # Genera embeddings
        try:
            embeddings = get_embeddings(texts)
        except Exception as e:
            log.warning(f"Embedding batch {start}-{start+len(batch)} fallito: {e} — riprovo tra 5s")
            time.sleep(5)
            try:
                embeddings = get_embeddings(texts)
            except Exception as e2:
                log.error(f"Embedding fallito definitivamente: {e2}")
                errors += len(batch)
                continue

        for rec, emb in zip(batch, embeddings):
            rec["embedding"] = emb

        # ⚠️ Proteggi campi sensibili in altro (es. descrizione_web) ──────────
        existing = _fetch_existing_altro([r["slug"] for r in batch])
        for rec in batch:
            rec["altro"] = _merge_altro(rec.get("altro"), existing.get(rec["slug"]))

        try:
            supabase.table("companies").upsert(batch, on_conflict="slug").execute()
            inserted += len(batch)
            log.info(f"  ✓ {inserted}/{total} inseriti")
        except Exception as e:
            log.error(f"Upsert fallito per batch {start}: {e}")
            errors += len(batch)

        time.sleep(EMBED_DELAY_S)

    log.info(f"\n=== COMPLETATO: {inserted} inseriti, {errors} errori ===")

if __name__ == "__main__":
    main()
