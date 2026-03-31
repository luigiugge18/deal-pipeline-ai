"""
Deal Pipeline AI — ETL Pipeline
================================
Legge Excel/Google Sheets + Google Drive, calcola embeddings OpenAI
e fa upsert su Supabase.

Uso:
    python 02_etl_pipeline.py                    # import completo
    python 02_etl_pipeline.py --limit 50         # test su 50 aziende
    python 02_etl_pipeline.py --skip-embeddings  # solo dati strutturati
    python 02_etl_pipeline.py --company "Rossi SRL"  # singola azienda

Variabili d'ambiente richieste (file .env):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
    OPENAI_API_KEY
    GOOGLE_SHEET_ID          # ID del foglio principale
    GOOGLE_DRIVE_FOLDER_ID   # ID cartella root Drive delle aziende
    GOOGLE_CREDENTIALS_PATH  # Path al file credentials.json (default: credentials.json)
"""

import os
import re
import logging
import argparse
from datetime import datetime
from pathlib import Path

# pip install python-dotenv supabase openai gspread google-api-python-client
# pip install pandas openpyxl PyMuPDF python-docx

from dotenv import load_dotenv
import pandas as pd
from supabase import create_client, Client
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_run.log'),
    ]
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL    = os.environ['SUPABASE_URL']
SUPABASE_KEY    = os.environ['SUPABASE_SERVICE_KEY']
OPENAI_API_KEY  = os.environ['OPENAI_API_KEY']
SHEET_ID        = os.environ['GOOGLE_SHEET_ID']
DRIVE_FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID', '')
CREDS_PATH      = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]

EMBEDDING_MODEL = 'text-embedding-3-small'
EMBEDDING_DIM   = 1536

# Mappa delle colonne Excel → campi DB
# ADATTARE in base al proprio foglio
COLUMN_MAP = {
    'nome'         : 'name',
    'ragione sociale': 'name',
    'company'      : 'name',
    'settore'      : 'sector',
    'sector'       : 'sector',
    'sottosettore' : 'subsector',
    'area'         : 'geography',
    'regione'      : 'geography',
    'geography'    : 'geography',
    'fatturato'    : 'revenue',
    'revenue'      : 'revenue',
    'ebitda'       : 'ebitda',
    'ebitda %'     : 'ebitda_pct',
    'ebitda%'      : 'ebitda_pct',
    'margine'      : 'ebitda_pct',
    'dipendenti'   : 'employees',
    'employees'    : 'employees',
    'fondazione'   : 'founded_year',
    'anno'         : 'founded_year',
    'in vendita'   : 'for_sale',
    'for sale'     : 'for_sale',
    'status'       : 'sale_status',
    'stato vendita': 'sale_status',
    'prezzo'       : 'asking_price',
    'asking price' : 'asking_price',
    'note'         : 'short_note',
    'commento'     : 'short_note',
    'tag'          : 'tags',
    'tags'         : 'tags',
    'drive'        : 'drive_folder_id',
    'drive folder' : 'drive_folder_id',
}


# =============================================================================
# Connessioni
# =============================================================================

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_openai() -> OpenAI:
    return OpenAI(api_key=OPENAI_API_KEY)

def get_google_creds() -> Credentials:
    return Credentials.from_service_account_file(CREDS_PATH, scopes=GOOGLE_SCOPES)

def get_sheets_client():
    return gspread.authorize(get_google_creds())

def get_drive_service():
    return build('drive', 'v3', credentials=get_google_creds())


# =============================================================================
# Lettura Google Sheets
# =============================================================================

def read_sheet(sheet_id: str) -> pd.DataFrame:
    """Legge il primo foglio e restituisce un DataFrame normalizzato."""
    log.info(f'Lettura Google Sheet: {sheet_id}')
    gc = get_sheets_client()
    sh = gc.open_by_key(sheet_id)
    records = sh.sheet1.get_all_records()
    df = pd.DataFrame(records)
    log.info(f'  → {len(df)} righe lette')

    # Normalizza header → campo DB
    df.columns = [_normalize_col(c) for c in df.columns]
    df.rename(columns=COLUMN_MAP, inplace=True)

    # Rimuovi colonne non mappate / unnamed
    known = set(COLUMN_MAP.values()) | {'name'}
    df = df[[c for c in df.columns if c in known]]

    return df

def _normalize_col(col: str) -> str:
    return col.strip().lower().replace('\xa0', ' ')


# =============================================================================
# Normalizzazione dati
# =============================================================================

def parse_numeric(val) -> float | None:
    """Converte '2.3M', '2,300,000', '2300K' → float o None."""
    if pd.isna(val) or val == '':
        return None
    s = str(val).strip().replace('\u20ac', '').replace(' ', '').replace(',', '.')
    multiplier = 1
    s_upper = s.upper()
    if s_upper.endswith('M'):
        multiplier = 1_000_000
        s = s[:-1]
    elif s_upper.endswith('K'):
        multiplier = 1_000
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return None

def to_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('si', 'sì', 'yes', 'true', '1', 'x', 'v')

def make_slug(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug[:100]

def normalize_row(row: dict) -> dict:
    """Normalizza una riga del DataFrame in un dict pronto per il DB."""
    out = {}

    name = str(row.get('name', '')).strip()
    if not name:
        return {}
    out['name'] = name
    out['slug'] = make_slug(name)

    for field in ['sector', 'subsector', 'geography', 'sale_status', 'short_note', 'drive_folder_id']:
        v = row.get(field)
        if v and str(v).strip() not in ('', 'nan', 'None'):
            out[field] = str(v).strip()[:500] if field == 'short_note' else str(v).strip()

    for field in ['revenue', 'ebitda', 'ebitda_pct', 'asking_price']:
        out[field] = parse_numeric(row.get(field))

    if 'employees' in row:
        try:
            out['employees'] = int(float(row['employees']))
        except (ValueError, TypeError):
            pass

    if 'founded_year' in row:
        try:
            out['founded_year'] = int(float(row['founded_year']))
        except (ValueError, TypeError):
            pass

    out['for_sale'] = to_bool(row.get('for_sale', False))

    tags_raw = row.get('tags', '')
    if tags_raw and str(tags_raw).strip() not in ('', 'nan'):
        out['tags'] = [t.strip() for t in str(tags_raw).split(',') if t.strip()]

    out['data_source'] = ['excel']
    return out


# =============================================================================
# Lettura Google Drive
# =============================================================================

def list_folder_files(service, folder_id: str) -> list[dict]:
    """Elenca tutti i file in una cartella Drive (non ricorsivo)."""
    query = f"'{folder_id}' in parents and trashed = false"
    result = service.files().list(
        q=query,
        fields='files(id, name, mimeType)',
    ).execute()
    return result.get('files', [])

def download_file_text(service, file_id: str, mime_type: str) -> str:
    """Scarica un file Drive e restituisce il testo."""
    try:
        if 'google-apps.document' in mime_type:
            # Google Doc → esporta come plain text
            request = service.files().export_media(fileId=file_id, mimeType='text/plain')
        else:
            request = service.files().get_media(fileId=file_id)

        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        raw = buf.read()

        if mime_type == 'application/pdf':
            return _extract_pdf_text(raw)
        elif mime_type in ('application/vnd.openxmlformats-officedocument.wordprocessingml.document',):
            return _extract_docx_text(raw)
        else:
            return raw.decode('utf-8', errors='ignore')
    except Exception as e:
        log.warning(f'  Impossibile scaricare file {file_id}: {e}')
        return ''

def _extract_pdf_text(data: bytes) -> str:
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=data, filetype='pdf')
        return '\n'.join(page.get_text() for page in doc)
    except ImportError:
        log.warning('PyMuPDF non installato. Saltato testo PDF.')
        return ''

def _extract_docx_text(data: bytes) -> str:
    try:
        from docx import Document as DocxDocument
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as f:
            f.write(data)
            tmp = f.name
        doc = DocxDocument(tmp)
        os.unlink(tmp)
        return '\n'.join(p.text for p in doc.paragraphs if p.text.strip())
    except ImportError:
        log.warning('python-docx non installato. Saltato testo DOCX.')
        return ''

def get_company_drive_notes(service, company: dict) -> str:
    """Recupera tutte le note da Google Drive per un'azienda."""
    folder_id = company.get('drive_folder_id')
    if not folder_id:
        # Prova a cercare per nome
        folder_id = _find_folder_by_name(service, company['name'])
        if not folder_id:
            return ''

    files = list_folder_files(service, folder_id)
    texts = []
    for f in files:
        mime = f.get('mimeType', '')
        if any(t in mime for t in ['text', 'document', 'pdf', 'word']):
            log.debug(f'  Lettura file Drive: {f["name"]}')
            text = download_file_text(service, f['id'], mime)
            if text.strip():
                texts.append(f'--- {f["name"]} ---\n{text}')

    return '\n\n'.join(texts)

def _find_folder_by_name(service, name: str) -> str | None:
    """Cerca una sottocartella per nome all'interno di DRIVE_FOLDER_ID."""
    if not DRIVE_FOLDER_ID:
        return None
    query = (
        f"name = '{name.replace(chr(39), chr(39)+chr(39))}' "
        f"and '{DRIVE_FOLDER_ID}' in parents "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    res = service.files().list(q=query, fields='files(id)').execute()
    files = res.get('files', [])
    return files[0]['id'] if files else None


# =============================================================================
# Embeddings
# =============================================================================

def build_text_for_embedding(company: dict, long_notes: str = '') -> str:
    """Costruisce il testo da embeddare per un'azienda."""
    parts = []
    if company.get('name'):
        parts.append(company['name'])
    if company.get('sector'):
        parts.append(f"Settore: {company['sector']}")
    if company.get('subsector'):
        parts.append(f"Sottosettore: {company['subsector']}")
    if company.get('geography'):
        parts.append(f"Geografia: {company['geography']}")
    if company.get('revenue'):
        parts.append(f"Fatturato: {company['revenue']:,.0f} EUR")
    if company.get('ebitda_pct'):
        parts.append(f"EBITDA margin: {company['ebitda_pct']:.1f}%")
    if company.get('short_note'):
        parts.append(company['short_note'])
    if long_notes:
        # Tronca a ~4000 char per non eccedere il limite token
        parts.append(long_notes[:4000])
    if company.get('tags'):
        parts.append('Tag: ' + ', '.join(company['tags']))
    return ' | '.join(parts)

def get_embedding(client: OpenAI, text: str) -> list[float] | None:
    if not text.strip():
        return None
    try:
        resp = client.embeddings.create(input=text, model=EMBEDDING_MODEL)
        return resp.data[0].embedding
    except Exception as e:
        log.error(f'  Errore embedding: {e}')
        return None


# =============================================================================
# Upsert su Supabase
# =============================================================================

def upsert_company(supabase: Client, company: dict) -> bool:
    """Inserisce o aggiorna un'azienda nel DB (upsert su slug)."""
    try:
        # Converti embedding list → stringa pgvector
        if 'embedding' in company and company['embedding'] is not None:
            company['embedding'] = company['embedding']  # supabase-py gestisce la serializzazione

        supabase.table('companies').upsert(
            company,
            on_conflict='slug',
        ).execute()
        return True
    except Exception as e:
        log.error(f'  Errore upsert {company.get("name", "?")}: {e}')
        return False


# =============================================================================
# Pipeline principale
# =============================================================================

def run_etl(limit: int | None = None, skip_embeddings: bool = False,
            filter_company: str | None = None):
    log.info('=' * 60)
    log.info(f'ETL Pipeline — {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    log.info('=' * 60)

    supabase = get_supabase()
    openai   = get_openai() if not skip_embeddings else None

    # 1. Leggi Excel / Sheets
    df = read_sheet(SHEET_ID)
    if filter_company:
        df = df[df['name'].str.contains(filter_company, case=False, na=False)]
        log.info(f'Filtro azienda: "{filter_company}" → {len(df)} righe')
    if limit:
        df = df.head(limit)
        log.info(f'Limite: prime {limit} righe')

    # 2. Google Drive service
    drive_service = None
    try:
        drive_service = get_drive_service()
        log.info('Google Drive: connesso')
    except Exception as e:
        log.warning(f'Google Drive non disponibile: {e}. Salto lettura Drive.')

    # 3. Elabora ogni azienda
    stats = {'new': 0, 'updated': 0, 'errors': 0, 'skipped': 0}

    for idx, row in df.iterrows():
        company = normalize_row(row.to_dict())
        if not company:
            stats['skipped'] += 1
            continue

        name = company['name']
        log.info(f'[{idx+1}/{len(df)}] {name}')

        # Leggi note da Drive
        long_notes = ''
        if drive_service:
            long_notes = get_company_drive_notes(drive_service, company)
            if long_notes:
                company['long_notes'] = long_notes
                if 'drive' not in company.get('data_source', []):
                    company['data_source'] = company.get('data_source', []) + ['drive']
                log.debug(f'  Drive notes: {len(long_notes)} chars')

        # Calcola embedding
        if not skip_embeddings and openai:
            text = build_text_for_embedding(company, long_notes)
            company['embedding'] = get_embedding(openai, text)
            if company['embedding']:
                log.debug(f'  Embedding: OK ({EMBEDDING_DIM} dims)')

        # Upsert su Supabase
        ok = upsert_company(supabase, company)
        if ok:
            stats['updated'] += 1
        else:
            stats['errors'] += 1

    # 4. Report finale
    log.info('-' * 60)
    log.info(
        f'ETL completato: {stats["updated"]} aggiornate, '
        f'{stats["skipped"]} saltate, {stats["errors"]} errori'
    )
    return stats


# =============================================================================
# Entry point
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deal Pipeline ETL')
    parser.add_argument('--limit',           type=int, help='Limita il numero di aziende')
    parser.add_argument('--skip-embeddings', action='store_true', help='Salta calcolo embeddings')
    parser.add_argument('--company',         type=str, help='Filtra per nome azienda')
    args = parser.parse_args()

    run_etl(
        limit=args.limit,
        skip_embeddings=args.skip_embeddings,
        filter_company=args.company,
    )
