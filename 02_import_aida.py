"""
Import AIDA data from Google Sheets CSV → Supabase
====================================================
Uso:
    python 02_import_aida.py

Variabili d'ambiente richieste (in .env o Railway):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
    OPENAI_API_KEY
"""

from __future__ import annotations
import os, re, time, unicodedata, logging
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
    """Converte una stringa in slug URL-safe."""
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")[:120]

def to_bigint(val: str) -> int | None:
    if not val or val.strip() == "":
        return None
    try:
        return int(float(val.replace(".", "").replace(",", ".")))
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

# Segnali esplicitamente negativi — escludono sempre
NEGATIVE_PATTERNS = [
    "non interessat",
    "no interessat",
    "non iteressati",
    "non fa spurghi",
    "non fanno spurghi",
    "no spurghi",
    "pubblica",
    "parte di",
    "troppo piccola",
    "gia in contatto con bravo",
    "già in contatto con bravo",
]

# Interesse POTENZIALE — segnali soft (controllati PRIMA di quelli chiari)
POTENZIALE_PATTERNS = [
    "potenzialmente interessat",
    "potenzialmente",
    "forse interessat",
    "possibile interesse",
    "da valutare",
    "potrebbe interessarsi",
    "potrebbe essere interessat",
    "aperto a valutare",
    "disponibile a valutare",
]

# Interesse CHIARO — segnali espliciti di apertura alla vendita
CHIARO_PATTERNS = [
    "interessat",          # interessato/a/i/e
    "vuole vendere",
    "vogliono vendere",
    "disponibile a cedere",
    "disponibili a cedere",
    "confermato interesse",
    "aperto alla cessione",
    "si vuole cedere",
    "procediamo",
    "valutiamo",
    "accordo",
]


def compute_interesse(note: str | None, next_steps: str | None = None) -> tuple[bool, str | None]:
    """
    Ritorna (is_interessante, livello_interesse).
    livello_interesse: 'chiaro' | 'potenziale' | None

    Logica strict-whitelist:
    - Nessuna nota → False, None
    - Segnali negativi → False, None
    - Interesse potenziale (es. "potenzialmente interessato") → True, 'potenziale'
    - Interesse chiaro (es. "interessato", "vuole vendere") → True, 'chiaro'
    - Note senza segnali espliciti (es. solo "richiamare") → False, None
    """
    text = ((note or "") + " " + (next_steps or "")).strip().lower()
    if not text:
        return False, None

    # Segnali negativi escludono sempre
    for pat in NEGATIVE_PATTERNS:
        if pat in text:
            return False, None

    # Potenziale prima (più specifico — "potenzialmente interessato" non deve
    # cadere nella bucket "chiaro" per via del match su "interessat")
    for pat in POTENZIALE_PATTERNS:
        if pat in text:
            return True, "potenziale"

    # Interesse chiaro
    for pat in CHIARO_PATTERNS:
        if pat in text:
            return True, "chiaro"

    # Nessun segnale positivo riconosciuto → escludi
    return False, None


# Retrocompatibilità — usato solo internamente
def compute_is_interessante(note: str | None, next_steps: str | None = None) -> bool:
    is_int, _ = compute_interesse(note, next_steps)
    return is_int


def build_embedding_text(row: dict) -> str:
    """Costruisce il testo da embeddare per la ricerca semantica."""
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
    return " | ".join(p for p in parts if p.strip() and p.strip() not in ["ATECO ", "Regione: ", "Provincia: ", "Comune: "])

def get_embeddings(texts: list[str]) -> list[list[float]]:
    resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in resp.data]

# ── CSV Parsing ───────────────────────────────────────────────────────────────
def fetch_csv(local_file: str | None = None) -> list[dict]:
    if local_file:
        log.info(f"Leggendo CSV da file locale: {local_file}")
        with open(local_file, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        log.info(f"Scaricando CSV da Google Sheets…")
        resp = requests.get(CSV_URL, timeout=60)
        resp.raise_for_status()
        content = resp.text
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    # Header row (index 0) — col indices (0-based):
    # 0  = Progressivo (sheet_row)
    # 1  = Ragione sociale
    # 2  = Interesse a vendere  ← NEW (1 = interessato, blank = no)
    # 3  = Note
    # 4  = Contatti
    # 5  = Next steps
    # 6  = Partita IVA
    # 7  = ATECO 2007 codice
    # 8  = Sede operativa - Regione
    # 9  = Sede operativa - Provincia
    # 10 = Sede operativa - Comune
    # 11 = Numero di telefono
    # 12 = Website
    # 13 = Data di chiusura ultimo bilancio
    # 14 = EBITDA EUR Ultimo
    # 15 = EBITDA EUR Anno-1
    # 16 = EBITDA EUR Anno-2
    # 17 = EBITDA EUR Anno-3
    # 18 = EBITDA EUR Anno-4
    # 19 = EBITDA/Vendite % Ultimo
    # 20 = EBITDA/Vendite % Anno-1
    # 21 = EBITDA/Vendite % Anno-2
    # 22 = EBITDA/Vendite % Anno-3
    # 23 = EBITDA/Vendite % Anno-4
    # 24 = Ricavi EUR Ultimo
    # 25 = Ricavi EUR Anno-1
    # 26 = Ricavi EUR Anno-2
    # 27 = Ricavi EUR Anno-3
    # 28 = Ricavi EUR Anno-4
    # 29 = Azionisti Nome
    # 30 = CSH Nome
    # 31 = DM Nome completo
    # 32 = DM Codice fiscale

    # Usa dinamicamente la riga header per tollerare futuri riordini
    header = rows[0] if rows else []
    col_idx: dict[str, int] = {h.strip(): i for i, h in enumerate(header)}

    records = []
    seen_slugs: set[str] = set()

    def gcol(name: str, fallback: int) -> int:
        """Trova l'indice di una colonna per nome (case-insensitive), altrimenti usa fallback."""
        name_low = name.lower()
        for h, i in col_idx.items():
            if h.lower() == name_low:
                return i
        log.warning(f"Colonna '{name}' non trovata nel header, uso indice {fallback}")
        return fallback

    # Risolvi indici per nome (robusto a future aggiunte di colonne)
    IDX_SHEET_ROW    = gcol("",                     0)   # colonna A senza header
    IDX_RAGIONE      = gcol("Ragione sociale",       1)
    IDX_INTERESSE    = gcol("Interesse a vendere",   2)
    IDX_NOTE         = gcol("Note",                  3)
    IDX_CONTATTI     = gcol("Contatti",              4)
    IDX_NEXT_STEPS   = gcol("Next steps",            5)
    IDX_PIVA         = gcol("Partita IVA",           6)
    IDX_ATECO        = gcol("ATECO 2007 codice",     7)
    IDX_REGIONE      = gcol("Sede operativa - Regione",   8)
    IDX_PROVINCIA    = gcol("Sede operativa - Provincia", 9)
    IDX_COMUNE       = gcol("Sede operativa - Comune",    10)
    IDX_TELEFONO     = gcol("Numero di telefono",    11)
    IDX_WEBSITE      = gcol("Website",               12)
    IDX_DATA         = gcol("Data di chiusura ultimo bilancio", 13)

    MIN_ROW_LEN = max(IDX_DATA, IDX_WEBSITE, IDX_WEBSITE) + 20  # abbondanza per colonne finanziari

    for i, row in enumerate(rows[1:], start=2):  # skip header
        # Pad row se mancano colonne
        while len(row) < MIN_ROW_LEN:
            row.append("")

        ragione = row[IDX_RAGIONE].strip()
        if not ragione:
            continue  # skip righe senza nome

        base_slug = slugify(ragione)
        slug = base_slug
        counter = 1
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)

        sheet_row_val  = row[IDX_SHEET_ROW].strip()
        interesse_raw  = row[IDX_INTERESSE].strip()
        note_val       = row[IDX_NOTE].strip()      or None
        contatti_val   = row[IDX_CONTATTI].strip()  or None
        next_steps_val = row[IDX_NEXT_STEPS].strip() or None

        try:
            sheet_row_int = int(float(sheet_row_val)) if sheet_row_val else None
        except ValueError:
            sheet_row_int = None

        # is_interessante = 1 nella colonna "Interesse a vendere"
        is_interessante = (interesse_raw == "1")
        livello_interesse = "chiaro" if is_interessante else None

        # offset dinamico per le colonne finanziarie AIDA
        # (le colonne AIDA iniziano subito dopo Next steps e si susseguono in ordine fisso)
        base = IDX_NEXT_STEPS + 1   # = IDX_PIVA

        rec = {
            "slug":              slug,
            "ragione_sociale":   ragione,
            "sheet_row":         sheet_row_int,
            "note":              note_val,
            "contatti":          contatti_val,
            "next_steps":        next_steps_val,
            "is_interessante":   is_interessante,
            "livello_interesse": livello_interesse,
            "partita_iva":       row[IDX_PIVA].strip()      or None,
            "ateco_codice":      row[IDX_ATECO].strip()     or None,
            "regione":           row[IDX_REGIONE].strip()   or None,
            "provincia":         row[IDX_PROVINCIA].strip() or None,
            "comune":            row[IDX_COMUNE].strip()    or None,
            "telefono":          row[IDX_TELEFONO].strip()  or None,
            "website":           row[IDX_WEBSITE].strip()   or None,
            "data_bilancio":     to_date(row[IDX_DATA]),
            "ebitda_0":          to_bigint(row[IDX_DATA+1]),
            "ebitda_1":          to_bigint(row[IDX_DATA+2]),
            "ebitda_2":          to_bigint(row[IDX_DATA+3]),
            "ebitda_3":          to_bigint(row[IDX_DATA+4]),
            "ebitda_4":          to_bigint(row[IDX_DATA+5]),
            "ebitda_margin_0":   to_numeric(row[IDX_DATA+6]),
            "ebitda_margin_1":   to_numeric(row[IDX_DATA+7]),
            "ebitda_margin_2":   to_numeric(row[IDX_DATA+8]),
            "ebitda_margin_3":   to_numeric(row[IDX_DATA+9]),
            "ebitda_margin_4":   to_numeric(row[IDX_DATA+10]),
            "ricavi_0":          to_bigint(row[IDX_DATA+11]),
            "ricavi_1":          to_bigint(row[IDX_DATA+12]),
            "ricavi_2":          to_bigint(row[IDX_DATA+13]),
            "ricavi_3":          to_bigint(row[IDX_DATA+14]),
            "ricavi_4":          to_bigint(row[IDX_DATA+15]),
            "azionisti":         row[IDX_DATA+16].strip() or None,
            "csh_nome":          row[IDX_DATA+17].strip() or None,
            "dm_nome":           row[IDX_DATA+18].strip() or None,
            "dm_codice_fiscale": row[IDX_DATA+19].strip() or None,
        }
        records.append(rec)

    log.info(f"Parsed {len(records)} aziende dal CSV")
    return records

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    import sys
    local_file = sys.argv[1] if len(sys.argv) > 1 else None
    records = fetch_csv(local_file)
    total   = len(records)
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

        # Aggiunge embedding a ciascun record
        for rec, emb in zip(batch, embeddings):
            rec["embedding"] = emb

        # Upsert in Supabase
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
