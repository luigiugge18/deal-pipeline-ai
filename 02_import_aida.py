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
def fetch_csv() -> list[dict]:
    log.info(f"Scaricando CSV da Google Sheets…")
    resp = requests.get(CSV_URL, timeout=60)
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)
    # Header row (index 0)
    # Col indices (0-based):
    # 0  = progressivo (ignore)
    # 1  = Ragione sociale
    # 2  = Note (ignore)
    # 3  = Contatti (ignore)
    # 4  = Next steps (ignore)
    # 5  = Partita IVA
    # 6  = ATECO 2007 codice
    # 7  = Sede operativa - Regione
    # 8  = Sede operativa - Provincia
    # 9  = Sede operativa - Comune
    # 10 = Numero di telefono
    # 11 = Website
    # 12 = Data di chiusura ultimo bilancio
    # 13 = EBITDA EUR Ultimo
    # 14 = EBITDA EUR Anno-1
    # 15 = EBITDA EUR Anno-2
    # 16 = EBITDA EUR Anno-3
    # 17 = EBITDA EUR Anno-4
    # 18 = EBITDA/Vendite % Ultimo
    # 19 = EBITDA/Vendite % Anno-1
    # 20 = EBITDA/Vendite % Anno-2
    # 21 = EBITDA/Vendite % Anno-3
    # 22 = EBITDA/Vendite % Anno-4
    # 23 = Ricavi EUR Ultimo
    # 24 = Ricavi EUR Anno-1
    # 25 = Ricavi EUR Anno-2
    # 26 = Ricavi EUR Anno-3
    # 27 = Ricavi EUR Anno-4
    # 28 = Azionisti Nome
    # 29 = CSH Nome
    # 30 = DM Nome completo
    # 31 = DM Codice fiscale

    records = []
    seen_slugs: set[str] = set()

    for i, row in enumerate(rows[1:], start=2):  # skip header
        # Pad row se mancano colonne
        while len(row) < 32:
            row.append("")

        ragione = row[1].strip()
        if not ragione:
            continue  # skip righe senza nome

        base_slug = slugify(ragione)
        slug = base_slug
        counter = 1
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)

        rec = {
            "slug":             slug,
            "ragione_sociale":  ragione,
            "partita_iva":      row[5].strip() or None,
            "ateco_codice":     row[6].strip() or None,
            "regione":          row[7].strip() or None,
            "provincia":        row[8].strip() or None,
            "comune":           row[9].strip() or None,
            "telefono":         row[10].strip() or None,
            "website":          row[11].strip() or None,
            "data_bilancio":    to_date(row[12]),
            "ebitda_0":         to_bigint(row[13]),
            "ebitda_1":         to_bigint(row[14]),
            "ebitda_2":         to_bigint(row[15]),
            "ebitda_3":         to_bigint(row[16]),
            "ebitda_4":         to_bigint(row[17]),
            "ebitda_margin_0":  to_numeric(row[18]),
            "ebitda_margin_1":  to_numeric(row[19]),
            "ebitda_margin_2":  to_numeric(row[20]),
            "ebitda_margin_3":  to_numeric(row[21]),
            "ebitda_margin_4":  to_numeric(row[22]),
            "ricavi_0":         to_bigint(row[23]),
            "ricavi_1":         to_bigint(row[24]),
            "ricavi_2":         to_bigint(row[25]),
            "ricavi_3":         to_bigint(row[26]),
            "ricavi_4":         to_bigint(row[27]),
            "azionisti":        row[28].strip() or None,
            "csh_nome":         row[29].strip() or None,
            "dm_nome":          row[30].strip() or None,
            "dm_codice_fiscale":row[31].strip() or None,
        }
        records.append(rec)

    log.info(f"Parsed {len(records)} aziende dal CSV")
    return records

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    records = fetch_csv()
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
