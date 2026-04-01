"""
Import campagna email da Google Sheets → Supabase
===================================================
Sheet: Risultati campagna email 01.2026
URL: https://docs.google.com/spreadsheets/d/1sM_qaiclmM8Q_P2HiEe-YWhTyOsEopxkSxgA9Zd7zRU
Tab: Sheet1

Struttura colonne (0-based):
  A=0   Sito (website)
  B=1   Potential Sell-side ("Si" = interessante)
  C=2   Tier
  D=3   Partita IVA
  E=4   Ragione sociale
  F=5   Interesse a vendere
  G=6   Business
  H=7   Description
  I=8   Data contatto
  J=9   Numero cellulare
  K=10  Indirizzo Email
  L=11  Note
  M=12  Risposta loro
  N=13  Stato Icel
  O=14  Sede operativa - Regione - Regione
  P=15  Sede operativa - Provincia
  Q=16  Sede operativa - Comune
  R=17  Numero di telefono
  S=18  Data di chiusura ultimo bilancio  (MM/DD/YYYY → anno_0)
  T=19  Azionisti
  U=20  CSH Nome
  V=21  DM Nome
  W=22  DM (CF o secondo campo DM)
  X=23  EBITDA        slot 0 (più recente)
  Y=24  EBITDA        slot 1
  Z=25  EBITDA        slot 2
  AA=26 EBITDA        slot 3
  AB=27 EBITDA        slot 4
  AC=28 EBITDA/Vend % slot 0
  AD=29 EBITDA/Vend % slot 1
  AE=30 EBITDA/Vend % slot 2
  AF=31 EBITDA/Vend % slot 3
  AG=32 EBITDA/Vend % slot 4
  AH=33 Ricavi        slot 0
  AI=34 Ricavi        slot 1
  AJ=35 Ricavi        slot 2
  AK=36 Ricavi        slot 3
  AL=37 Ricavi        slot 4
  AM=38 Esclusiva
"""

from __future__ import annotations
import os, re, time, unicodedata, logging, json
from datetime import datetime

import requests, csv, io
from dotenv import load_dotenv
from supabase import create_client, Client
from openai import OpenAI

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
SHEET_ID = "1sM_qaiclmM8Q_P2HiEe-YWhTyOsEopxkSxgA9Zd7zRU"
CSV_URL  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Sheet1"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
OPENAI_KEY   = os.environ["OPENAI_API_KEY"]

BATCH_SIZE      = 20
EMBEDDING_MODEL = "text-embedding-3-small"
EMBED_DELAY_S   = 0.5

# Colonne scalari (0-based index)
COL = {
    "website":           0,
    "potential_sell":    1,
    "tier":              2,
    "partita_iva":       3,
    "ragione_sociale":   4,
    "interesse":         5,
    "business":          6,
    "description":       7,
    "data_contatto":     8,
    "cellulare":         9,
    "email":             10,
    "note":              11,
    "risposta":          12,
    "stato_icel":        13,
    "regione":           14,
    "provincia":         15,
    "comune":            16,
    "telefono":          17,
    "data_bilancio":     18,
    "azionisti":         19,
    "csh_nome":          20,
    "dm_nome":           21,
    "dm_cf":             22,
    "esclusiva":         38,
}

# Colonne finanziarie (posizionali, slot 0=più recente)
FIN_EBITDA  = [23, 24, 25, 26, 27]
FIN_MARGIN  = [28, 29, 30, 31, 32]
FIN_RICAVI  = [33, 34, 35, 36, 37]

# ── Clients ─────────────────────────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
oai = OpenAI(api_key=OPENAI_KEY)

# ── Helpers ─────────────────────────────────────────────────────────────────
def slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")[:120]

def to_bigint(val: str) -> int | None:
    if not val or val.strip() in ("", "n.d.", "n.d", "nd"):
        return None
    try:
        return int(float(val.replace(".", "").replace(",", ".")))
    except Exception:
        return None

def to_numeric(val: str) -> float | None:
    if not val or val.strip() in ("", "n.d.", "n.d", "nd"):
        return None
    try:
        return float(val.replace(",", ".").replace("%", "").strip())
    except Exception:
        return None

def to_date(val: str) -> str | None:
    if not val or val.strip() == "":
        return None
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def extract_year(val: str) -> int | None:
    """Estrae l'anno da una stringa data tipo '12/31/2024'."""
    if not val or val.strip() == "":
        return None
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).year
        except ValueError:
            pass
    # fallback: cerca 4 cifre
    m = re.search(r"\b(20\d{2})\b", val)
    return int(m.group(1)) if m else None

def gcol(row: list[str], key: str) -> str:
    idx = COL.get(key)
    if idx is None or idx >= len(row):
        return ""
    return row[idx].strip()

def build_embedding_text(rec: dict) -> str:
    parts = [rec.get("ragione_sociale", "")]
    for f in ["regione", "provincia", "comune"]:
        if rec.get(f): parts.append(rec[f])
    if rec.get("ricavi_0"):
        parts.append(f"Ricavi: {rec['ricavi_0']:,} EUR")
    if rec.get("ebitda_0"):
        parts.append(f"EBITDA: {rec['ebitda_0']:,} EUR")
    if rec.get("ebitda_margin_0"):
        parts.append(f"EBITDA margin: {rec['ebitda_margin_0']}%")
    return " | ".join(p for p in parts if p)

# ── Main parse ───────────────────────────────────────────────────────────────
def fetch_and_parse() -> list[dict]:
    log.info("Scaricando CSV da Google Sheets…")
    resp = requests.get(CSV_URL, timeout=60)
    resp.raise_for_status()

    reader = csv.reader(io.StringIO(resp.text))
    rows   = list(reader)
    if not rows:
        log.error("CSV vuoto!")
        return []

    header = rows[0]
    log.info(f"Colonne: {len(header)} | Righe dati: {len(rows)-1}")

    records = []
    seen_slugs: set[str] = set()
    skipped = 0

    for i, row in enumerate(rows[1:], start=2):
        while len(row) < 39:
            row.append("")

        ragione = gcol(row, "ragione_sociale").strip()
        if not ragione:
            skipped += 1
            continue

        # Slug
        base_slug = slugify(ragione)
        slug = base_slug; ctr = 1
        while slug in seen_slugs:
            slug = f"{base_slug}-{ctr}"; ctr += 1
        seen_slugs.add(slug)

        # is_interessante: Potential Sell-side = "Si"
        is_int = gcol(row, "potential_sell").strip().lower() == "si"
        # Fallback: anche colonna F "Interesse a vendere" = "1" o "si"
        if not is_int:
            iv = gcol(row, "interesse").strip().lower()
            is_int = iv in ("1", "si", "sì")
        livello = "potenziale" if is_int else None

        # Esclusiva
        escl_raw = gcol(row, "esclusiva").strip().lower()
        esclusiva = escl_raw in ("1", "si", "sì", "true")

        # Anno_0 da data_bilancio
        data_bil_raw = gcol(row, "data_bilancio")
        anno_0 = extract_year(data_bil_raw)
        anno_vals = [(anno_0 - i) if anno_0 else None for i in range(5)]

        # Financials (posizionali)
        def fval(idx, converter):
            if idx >= len(row): return None
            return converter(row[idx])

        ebitda = [fval(c, to_bigint)  for c in FIN_EBITDA]
        margin = [fval(c, to_numeric) for c in FIN_MARGIN]
        ricavi = [fval(c, to_bigint)  for c in FIN_RICAVI]

        # Altro: campi extra non mappati
        extra_keys = ["business", "description", "data_contatto", "cellulare",
                      "email", "risposta", "stato_icel", "dm_cf"]
        altro: dict = {}
        for k in extra_keys:
            v = gcol(row, k)
            if v: altro[k] = v
        tier = gcol(row, "tier")
        if tier: altro["tier"] = tier

        rec = {
            "slug":              slug,
            "ragione_sociale":   ragione,
            "partita_iva":       gcol(row, "partita_iva")  or None,
            "regione":           gcol(row, "regione")      or None,
            "provincia":         gcol(row, "provincia")    or None,
            "comune":            gcol(row, "comune")       or None,
            "telefono":          gcol(row, "telefono")     or None,
            "website":           gcol(row, "website")      or None,
            "note":              gcol(row, "note")         or None,
            "azionisti":         gcol(row, "azionisti")    or None,
            "csh_nome":          gcol(row, "csh_nome")     or None,
            "dm_nome":           gcol(row, "dm_nome")      or None,
            "dm_codice_fiscale": gcol(row, "dm_cf")        or None,
            "data_bilancio":     to_date(data_bil_raw),
            "is_interessante":   is_int,
            "livello_interesse": livello,
            "esclusiva":         esclusiva,
            "ebitda_0": ebitda[0], "ebitda_1": ebitda[1], "ebitda_2": ebitda[2],
            "ebitda_3": ebitda[3], "ebitda_4": ebitda[4],
            "ebitda_margin_0": margin[0], "ebitda_margin_1": margin[1],
            "ebitda_margin_2": margin[2], "ebitda_margin_3": margin[3],
            "ebitda_margin_4": margin[4],
            "ricavi_0": ricavi[0], "ricavi_1": ricavi[1], "ricavi_2": ricavi[2],
            "ricavi_3": ricavi[3], "ricavi_4": ricavi[4],
            "anno_0": anno_vals[0], "anno_1": anno_vals[1], "anno_2": anno_vals[2],
            "anno_3": anno_vals[3], "anno_4": anno_vals[4],
            "altro": altro if altro else None,
        }
        records.append(rec)

    log.info(f"Parsed {len(records)} aziende ({skipped} righe vuote saltate)")
    log.info(f"  → {sum(1 for r in records if r['is_interessante'])} interessanti")
    log.info(f"  → {sum(1 for r in records if r['esclusiva'])} in esclusiva")
    return records


def main():
    records = fetch_and_parse()
    total = len(records); inserted = 0; errors = 0

    for start in range(0, total, BATCH_SIZE):
        batch = records[start:start+BATCH_SIZE]
        texts = [build_embedding_text(r) for r in batch]
        try:
            embs = oai.embeddings.create(model=EMBEDDING_MODEL, input=texts).data
            for rec, e in zip(batch, embs):
                rec["embedding"] = e.embedding
        except Exception as ex:
            log.warning(f"Embedding fallito batch {start}: {ex} — riprovo")
            time.sleep(5)
            try:
                embs = oai.embeddings.create(model=EMBEDDING_MODEL, input=texts).data
                for rec, e in zip(batch, embs):
                    rec["embedding"] = e.embedding
            except Exception as ex2:
                log.error(f"Embedding definitivamente fallito: {ex2}")
                errors += len(batch); continue

        try:
            supabase.table("companies").upsert(batch, on_conflict="slug").execute()
            inserted += len(batch)
            log.info(f"  ✓ {inserted}/{total}")
        except Exception as ex:
            log.error(f"Upsert fallito batch {start}: {ex}")
            errors += len(batch)

        time.sleep(EMBED_DELAY_S)

    log.info(f"\n=== COMPLETATO: {inserted} inseriti/aggiornati, {errors} errori ===")

if __name__ == "__main__":
    main()
