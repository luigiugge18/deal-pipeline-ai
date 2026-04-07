"""
Deal Pipeline AI — Backend API (FastAPI)
=========================================
Avvio:
    uvicorn 06_api:app --reload --port 8000

Endpoint:
    POST  /search              Ricerca ibrida buyer→seller
    GET   /company/{slug}      Scheda completa azienda
    POST  /export/pdf          Genera PDF shortlist
    POST  /export/excel        Genera Excel shortlist
    GET   /sectors             Lista settori disponibili
    GET   /health              Health check

Documentazione interattiva:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""

from __future__ import annotations
import os
import re
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# Import motore di ricerca (stesso modulo)
from search_engine_module import search, get_company  # noqa: E402  (vedi alias sotto)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title='Deal Pipeline AI',
    description='Sistema di matching buyer/seller con ricerca semantica',
    version='1.0.0',
)

# CORS: in produzione imposta ALLOWED_ORIGINS come variabile d'ambiente
# es. ALLOWED_ORIGINS=https://tuodominio.com,https://tuonome.github.io
_origins_env = os.environ.get('ALLOWED_ORIGINS', '*')
_origins = [o.strip() for o in _origins_env.split(',')] if _origins_env != '*' else ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_methods=['*'],
    allow_headers=['*'],
)


# =============================================================================
# Modelli Pydantic
# =============================================================================

class SearchRequest(BaseModel):
    query:             Optional[str]        = Field(None,  description='Testo libero del buyer')
    ateco_codici:      Optional[list[str]]  = Field(None,  description='Codici ATECO 2007')
    regione:           Optional[str]        = Field(None,  description='Regione (es. LOMBARDIA)')
    min_ricavi:        Optional[int]        = Field(None,  description='Ricavi minimi (€)')
    max_ricavi:        Optional[int]        = Field(None,  description='Ricavi massimi (€)')
    min_ebitda_pct:    Optional[float]      = Field(None,  description='EBITDA margin % minimo')
    max_ebitda_pct:    Optional[float]      = Field(None,  description='EBITDA margin % massimo')
    solo_interessanti: bool                 = Field(True,  description='Solo aziende interessate/potenzialmente interessate')
    limit:             int                  = Field(50,    ge=1, le=200)
    explain:           bool                 = Field(False, description='Genera spiegazione AI per top 5')

class CompanyResult(BaseModel):
    id:              str
    ragione_sociale: str
    slug:            str
    partita_iva:     Optional[str]  = None
    ateco_codice:    Optional[str]
    regione:         Optional[str]
    provincia:       Optional[str]
    comune:          Optional[str]
    ricavi_0:        Optional[float]
    ebitda_0:        Optional[float]
    ebitda_margin_0: Optional[float]
    website:         Optional[str]
    dm_nome:         Optional[str]
    score:           Optional[float]
    match_explanation: Optional[str]  = None
    note:              Optional[str]  = None
    contatti:          Optional[str]  = None
    next_steps:        Optional[str]  = None
    sheet_row:         Optional[int]  = None
    is_interessante:   Optional[bool] = None
    livello_interesse: Optional[str]  = None
    esclusiva:         Optional[bool] = None
    altro:             Optional[dict] = None
    anno_0:            Optional[int]  = None
    anno_1:            Optional[int]  = None
    anno_2:            Optional[int]  = None
    anno_3:            Optional[int]  = None
    anno_4:            Optional[int]  = None

class SearchResponse(BaseModel):
    results:    list[CompanyResult]
    count:      int
    query_time: float
    query_used: Optional[str]



# =============================================================================
# Endpoint: /search
# =============================================================================

@app.post('/search', response_model=SearchResponse, tags=['Matching'])
async def search_endpoint(req: SearchRequest):
    """
    Ricerca ibrida buyer→seller.

    Combina filtri strutturati (fatturato, EBITDA, settore, geography)
    con ranking semantico basato su embeddings OpenAI.
    """
    import time
    t0 = time.time()

    try:
        results_raw = search(
            query_text=req.query,
            ateco_codici=req.ateco_codici,
            regione=req.regione,
            min_ricavi=req.min_ricavi,
            max_ricavi=req.max_ricavi,
            min_ebitda_pct=req.min_ebitda_pct,
            max_ebitda_pct=req.max_ebitda_pct,
            solo_interessanti=req.solo_interessanti,
            limit=req.limit,
            explain=req.explain,
        )
    except Exception as e:
        log.error(f'Errore search: {e}')
        raise HTTPException(status_code=500, detail=str(e))

    results = [CompanyResult(**{k: r.get(k) for k in CompanyResult.model_fields}) for r in results_raw]

    return SearchResponse(
        results=results,
        count=len(results),
        query_time=round(time.time() - t0, 3),
        query_used=req.query,
    )


# =============================================================================
# Endpoint: /lookup  (ricerca per nome o P.IVA, senza filtro is_interessante)
# =============================================================================

@app.get('/lookup', tags=['Aziende'])
async def lookup_company(q: str):
    """
    Cerca un'azienda per ragione sociale (ILIKE) o P.IVA (exact).
    Non filtra per is_interessante — restituisce tutte le aziende nel DB.
    """
    from supabase import create_client
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])

    q_clean = q.strip()

    FIELDS = (
        'id, ragione_sociale, slug, partita_iva, ateco_codice, regione, provincia, comune, '
        'ricavi_0, ebitda_0, ebitda_margin_0, website, dm_nome, is_interessante, livello_interesse, '
        'note, contatti, next_steps, anno_0'
    )

    # Prima prova: P.IVA esatta (solo cifre)
    if re.fullmatch(r'\d{11}', q_clean):
        resp = sb.table('companies').select(FIELDS).eq('partita_iva', q_clean).execute()
        results = resp.data or []
    else:
        # Ricerca per nome (case-insensitive, substring)
        resp = sb.table('companies').select(FIELDS).ilike('ragione_sociale', f'%{q_clean}%').limit(20).execute()
        results = resp.data or []

        # Fallback fuzzy: inserisce % tra ogni carattere per matchare nomi con punteggiatura
        # es. "tecnodi" → "t%e%c%n%o%d%i" → trova "TEC.NO.DI. S.R.L."
        if not results and len(q_clean) >= 4:
            fuzzy = '%'.join(list(q_clean.lower()))
            resp2 = sb.table('companies').select(FIELDS).ilike('ragione_sociale', f'%{fuzzy}%').limit(20).execute()
            results = resp2.data or []
    # Rimuovi embedding se presente
    for r in results:
        r.pop('embedding', None)
    return {'results': results, 'count': len(results), 'query': q_clean}


# =============================================================================
# Endpoint: /company/{slug}
# =============================================================================

@app.get('/company/{slug}', tags=['Aziende'])
async def company_detail(slug: str):
    """
    Restituisce la scheda completa di un'azienda (financials + note + call history).
    """
    company = get_company(slug)
    if not company:
        raise HTTPException(status_code=404, detail=f'Azienda non trovata: {slug}')
    # Rimuovi embedding dal response (troppo grande e non utile per il frontend)
    company.pop('embedding', None)
    return company


# =============================================================================
# Endpoint: /sectors
# =============================================================================

@app.get('/sectors', tags=['Utility'])
async def list_sectors():
    """Lista tutti i codici ATECO presenti nel database, ordinati per frequenza."""
    from supabase import create_client
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])
    resp = sb.table('companies').select('ateco_codice').not_.is_('ateco_codice', 'null').execute()
    from collections import Counter
    counts = Counter(r['ateco_codice'] for r in (resp.data or []) if r.get('ateco_codice'))
    return [{'ateco_codice': s, 'count': c} for s, c in counts.most_common()]

@app.get('/regioni', tags=['Utility'])
async def list_regioni():
    """Lista tutte le regioni presenti nel database, ordinate per frequenza."""
    from supabase import create_client
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])
    resp = sb.table('companies').select('regione').not_.is_('regione', 'null').execute()
    from collections import Counter
    counts = Counter(r['regione'] for r in (resp.data or []) if r.get('regione'))
    return [{'regione': r, 'count': c} for r, c in counts.most_common()]




# =============================================================================
# Health check
# =============================================================================

@app.get('/health', tags=['Utility'])
async def health():
    return {'status': 'ok', 'timestamp': datetime.now().isoformat()}




# ── Alias per import da search engine ─────────────────────────────────────────
# Permette di usare questo file stand-alone senza dipendenza circolare
import sys, importlib
_mod_path = os.path.dirname(__file__)
if _mod_path not in sys.path:
    sys.path.insert(0, _mod_path)

# Rinomina il modulo search per evitare conflitti col nome del file
spec = importlib.util.spec_from_file_location(
    'search_engine_module',
    os.path.join(_mod_path, '03_search_engine.py')
)
search_engine_module = importlib.util.module_from_spec(spec)
sys.modules['search_engine_module'] = search_engine_module
spec.loader.exec_module(search_engine_module)
from search_engine_module import search, get_company  # noqa: F811
