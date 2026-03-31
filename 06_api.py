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
import io
import os
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
    query:          Optional[str]        = Field(None,  description='Testo libero del buyer')
    sectors:        Optional[list[str]]  = Field(None,  description='Lista settori')
    geography:      Optional[str]        = Field(None,  description='Area geografica')
    min_revenue:    Optional[float]      = Field(None,  description='Fatturato minimo (€)')
    max_revenue:    Optional[float]      = Field(None,  description='Fatturato massimo (€)')
    min_ebitda_pct: Optional[float]      = Field(None,  description='EBITDA% minimo')
    for_sale:       bool                 = Field(True,  description='Solo aziende in vendita')
    limit:          int                  = Field(15,    ge=1, le=50)
    explain:        bool                 = Field(False, description='Genera spiegazione AI per top 5')

class CompanyResult(BaseModel):
    id:          str
    name:        str
    slug:        str
    sector:      Optional[str]
    geography:   Optional[str]
    revenue:     Optional[float]
    ebitda_pct:  Optional[float]
    for_sale:    bool
    sale_status: Optional[str]
    short_note:  Optional[str]
    score:       Optional[float]
    match_explanation: Optional[str] = None

class SearchResponse(BaseModel):
    results:    list[CompanyResult]
    count:      int
    query_time: float
    query_used: Optional[str]

class ExportRequest(BaseModel):
    company_slugs: list[str] = Field(..., description='Lista slug aziende da esportare')
    buyer_name:    Optional[str] = Field(None, description='Nome del buyer (per intestazione)')
    intro_text:    Optional[str] = Field(None, description='Testo intro personalizzabile')
    format:        str           = Field('pdf', description='"pdf" o "excel"')


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
            sectors=req.sectors,
            geography=req.geography,
            min_revenue=req.min_revenue,
            max_revenue=req.max_revenue,
            min_ebitda_pct=req.min_ebitda_pct,
            for_sale=req.for_sale,
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
    """Lista tutti i settori presenti nel database, ordinati per frequenza."""
    from supabase import create_client
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])
    resp = sb.table('companies').select('sector').not_.is_('sector', 'null').execute()
    from collections import Counter
    counts = Counter(r['sector'] for r in (resp.data or []) if r.get('sector'))
    return [{'sector': s, 'count': c} for s, c in counts.most_common()]


# =============================================================================
# Endpoint: /export/pdf  e  /export/excel
# =============================================================================

@app.post('/export/pdf', tags=['Export'])
async def export_pdf(req: ExportRequest):
    """
    Genera un PDF con la shortlist delle aziende selezionate.
    Pronto per essere inviato al buyer.
    """
    companies = _fetch_companies(req.company_slugs)
    if not companies:
        raise HTTPException(status_code=404, detail='Nessuna azienda trovata')

    pdf_bytes = _build_pdf(companies, req.buyer_name, req.intro_text)
    filename = f'shortlist_{datetime.now().strftime("%Y%m%d")}.pdf'
    return Response(
        content=pdf_bytes,
        media_type='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )

@app.post('/export/excel', tags=['Export'])
async def export_excel(req: ExportRequest):
    """Genera un file Excel con la shortlist."""
    companies = _fetch_companies(req.company_slugs)
    if not companies:
        raise HTTPException(status_code=404, detail='Nessuna azienda trovata')

    xlsx_bytes = _build_excel(companies, req.buyer_name)
    filename = f'shortlist_{datetime.now().strftime("%Y%m%d")}.xlsx'
    return Response(
        content=xlsx_bytes,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


# =============================================================================
# Health check
# =============================================================================

@app.get('/health', tags=['Utility'])
async def health():
    return {'status': 'ok', 'timestamp': datetime.now().isoformat()}


# =============================================================================
# Helpers export
# =============================================================================

def _fetch_companies(slugs: list[str]) -> list[dict]:
    from supabase import create_client
    sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])
    resp = sb.table('companies').select(
        'name, slug, sector, geography, revenue, ebitda, ebitda_pct, '
        'employees, sale_status, asking_price, short_note, tags, founded_year'
    ).in_('slug', slugs).execute()
    return resp.data or []

def _fmt_eur(val):
    if val is None:
        return 'N/D'
    if val >= 1_000_000:
        return f'€{val/1_000_000:.1f}M'
    if val >= 1_000:
        return f'€{val/1_000:.0f}K'
    return f'€{val:.0f}'

def _build_pdf(companies: list[dict], buyer_name: str | None, intro: str | None) -> bytes:
    """Genera PDF shortlist con ReportLab."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2.5*cm, bottomMargin=2*cm)

    styles = getSampleStyleSheet()
    BLUE   = colors.HexColor('#1A3557')
    LBLUE  = colors.HexColor('#2E75B6')
    LGRAY  = colors.HexColor('#F2F2F2')
    GRAY   = colors.HexColor('#595959')

    title_style = ParagraphStyle('Title', parent=styles['Title'],
                                 textColor=BLUE, fontSize=22, spaceAfter=4)
    sub_style   = ParagraphStyle('Sub', parent=styles['Normal'],
                                 textColor=GRAY, fontSize=10, spaceAfter=2)
    body_style  = ParagraphStyle('Body', parent=styles['Normal'],
                                 fontSize=9, leading=13)
    co_style    = ParagraphStyle('Co', parent=styles['Normal'],
                                 textColor=BLUE, fontSize=13, spaceBefore=14, spaceAfter=3, fontName='Helvetica-Bold')
    note_style  = ParagraphStyle('Note', parent=styles['Normal'],
                                 fontSize=9, textColor=GRAY, leading=12)

    story = []

    # Intestazione
    story.append(Paragraph('Deal Pipeline AI — Shortlist Aziende', title_style))
    date_str = datetime.now().strftime('%d/%m/%Y')
    buyer_str = f'Per: {buyer_name}  |  ' if buyer_name else ''
    story.append(Paragraph(f'{buyer_str}Data: {date_str}  |  {len(companies)} aziende', sub_style))
    story.append(HRFlowable(width='100%', thickness=2, color=LBLUE, spaceAfter=10))

    if intro:
        story.append(Paragraph(intro, body_style))
        story.append(Spacer(1, 0.4*cm))

    # Scheda per azienda
    for i, c in enumerate(companies, 1):
        story.append(Paragraph(f'{i}. {c["name"]}', co_style))

        tdata = [
            ['Settore', c.get('sector') or 'N/D',
             'Fatturato', _fmt_eur(c.get('revenue'))],
            ['Area', c.get('geography') or 'N/D',
             'EBITDA%', f'{c.get("ebitda_pct","N/D")}%' if c.get('ebitda_pct') else 'N/D'],
            ['Status', c.get('sale_status') or 'N/D',
             'Prezzo richiesto', _fmt_eur(c.get('asking_price'))],
        ]
        t = Table(tdata, colWidths=[3*cm, 5.5*cm, 3.5*cm, 4.5*cm])
        t.setStyle(TableStyle([
            ('BACKGROUND',  (0, 0), (0, -1), LGRAY),
            ('BACKGROUND',  (2, 0), (2, -1), LGRAY),
            ('FONTNAME',    (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME',    (2, 0), (2, -1), 'Helvetica-Bold'),
            ('FONTSIZE',    (0, 0), (-1, -1), 8.5),
            ('TEXTCOLOR',   (0, 0), (0, -1), BLUE),
            ('TEXTCOLOR',   (2, 0), (2, -1), BLUE),
            ('GRID',        (0, 0), (-1, -1), 0.3, colors.HexColor('#CCCCCC')),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#F8FBFF')]),
            ('LEFTPADDING',  (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING',   (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING',(0, 0), (-1, -1), 4),
        ]))
        story.append(t)

        if c.get('short_note'):
            story.append(Spacer(1, 0.2*cm))
            story.append(Paragraph(f'<i>{c["short_note"]}</i>', note_style))

        if i < len(companies):
            story.append(HRFlowable(width='100%', thickness=0.5,
                                    color=colors.HexColor('#DDDDDD'), spaceBefore=8))

    # Footer
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width='100%', thickness=1, color=LBLUE, spaceBefore=4))
    story.append(Paragraph(
        f'<font color="#595959" size="8">Documento riservato — Deal Pipeline AI — {date_str}</font>',
        ParagraphStyle('foot', parent=styles['Normal'], alignment=1)
    ))

    doc.build(story)
    return buf.getvalue()


def _build_excel(companies: list[dict], buyer_name: str | None) -> bytes:
    """Genera Excel shortlist con XlsxWriter."""
    import xlsxwriter

    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True})
    ws = wb.add_worksheet('Shortlist')

    # Formati
    hdr_fmt  = wb.add_format({'bold': True, 'bg_color': '#1A3557', 'font_color': 'white',
                               'border': 1, 'text_wrap': True, 'valign': 'vcenter', 'font_size': 10})
    body_fmt = wb.add_format({'border': 1, 'font_size': 9, 'valign': 'vcenter'})
    alt_fmt  = wb.add_format({'border': 1, 'font_size': 9, 'bg_color': '#EBF3FB', 'valign': 'vcenter'})
    eur_fmt  = wb.add_format({'border': 1, 'num_format': '€#,##0', 'font_size': 9})
    pct_fmt  = wb.add_format({'border': 1, 'num_format': '0.0"%"', 'font_size': 9})

    # Intestazione documento
    title_fmt = wb.add_format({'bold': True, 'font_size': 14, 'font_color': '#1A3557'})
    ws.write('A1', f'Deal Pipeline AI — Shortlist{" per " + buyer_name if buyer_name else ""}', title_fmt)
    ws.write('A2', f'Generata il {datetime.now().strftime("%d/%m/%Y")}',
             wb.add_format({'font_size': 9, 'font_color': '#595959'}))

    # Header colonne
    headers = ['#', 'Azienda', 'Settore', 'Area', 'Fatturato (€)',
               'EBITDA (€)', 'EBITDA %', 'Dipendenti', 'Status', 'Prezzo richiesto (€)', 'Note brevi']
    col_widths = [4, 28, 18, 18, 16, 14, 10, 12, 16, 20, 45]
    START_ROW = 4
    for col, (h, w) in enumerate(zip(headers, col_widths)):
        ws.write(START_ROW, col, h, hdr_fmt)
        ws.set_column(col, col, w)
    ws.set_row(START_ROW, 22)

    # Dati
    for i, c in enumerate(companies):
        row = START_ROW + 1 + i
        fmt = body_fmt if i % 2 == 0 else alt_fmt
        ws.write(row, 0,  i + 1, fmt)
        ws.write(row, 1,  c.get('name', ''), fmt)
        ws.write(row, 2,  c.get('sector', ''), fmt)
        ws.write(row, 3,  c.get('geography', ''), fmt)
        ws.write(row, 4,  c.get('revenue') or '', eur_fmt if c.get('revenue') else fmt)
        ws.write(row, 5,  c.get('ebitda')  or '', eur_fmt if c.get('ebitda')  else fmt)
        ws.write(row, 6,  c.get('ebitda_pct') or '', pct_fmt if c.get('ebitda_pct') else fmt)
        ws.write(row, 7,  c.get('employees') or '', fmt)
        ws.write(row, 8,  c.get('sale_status', ''), fmt)
        ws.write(row, 9,  c.get('asking_price') or '', eur_fmt if c.get('asking_price') else fmt)
        ws.write(row, 10, c.get('short_note', ''), fmt)
        ws.set_row(row, 18)

    # Freeze header
    ws.freeze_panes(START_ROW + 1, 0)
    ws.autofilter(START_ROW, 0, START_ROW + len(companies), len(headers) - 1)

    wb.close()
    return buf.getvalue()


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
