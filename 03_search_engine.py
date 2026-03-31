"""
Deal Pipeline AI — Search Engine
==================================
Motore di ricerca ibrido: filtri strutturati + similarità semantica.

Uso diretto (da terminale):
    python 03_search_engine.py --query "manifatturiera nordest export" --revenue-min 1000000
    python 03_search_engine.py --query "software B2B" --sector software --for-sale
    python 03_search_engine.py --text-only "azienda alimentare Lombardia EBITDA 15%"

Uso come modulo:
    from search_engine import search, get_company
    results = search(query_text="manifatturiera nordest", min_revenue=1_000_000)
"""

import os
import json
import argparse
import logging
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client
from openai import OpenAI

load_dotenv()
log = logging.getLogger(__name__)

SUPABASE_URL   = os.environ['SUPABASE_URL']
SUPABASE_KEY   = os.environ['SUPABASE_SERVICE_KEY']
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
EMBEDDING_MODEL = 'text-embedding-3-small'
GPT_MODEL       = 'gpt-4o-mini'


# =============================================================================
# Client
# =============================================================================

_supabase: Client | None = None
_openai: OpenAI | None = None

def _get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase

def _get_openai() -> OpenAI:
    global _openai
    if _openai is None:
        _openai = OpenAI(api_key=OPENAI_API_KEY)
    return _openai


# =============================================================================
# Embedding
# =============================================================================

def get_embedding(text: str) -> list[float]:
    resp = _get_openai().embeddings.create(input=text, model=EMBEDDING_MODEL)
    return resp.data[0].embedding


# =============================================================================
# Tipo A: Ricerca strutturata pura (filtri SQL)
# =============================================================================

def search_structured(
    sectors: list[str] | None = None,
    geography: str | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_ebitda_pct: float | None = None,
    for_sale: bool = True,
    limit: int = 20,
) -> list[dict]:
    """Ricerca con soli filtri strutturati, ordinata per EBITDA desc."""
    sb = _get_supabase()
    q = sb.table('companies').select(
        'id, name, slug, sector, geography, revenue, ebitda, ebitda_pct, '
        'employees, for_sale, sale_status, asking_price, short_note, tags, last_updated'
    )
    if for_sale:
        q = q.eq('for_sale', True)
    if sectors:
        q = q.in_('sector', sectors)
    if geography:
        q = q.ilike('geography', f'%{geography}%')
    if min_revenue is not None:
        q = q.gte('revenue', min_revenue)
    if max_revenue is not None:
        q = q.lte('revenue', max_revenue)
    if min_ebitda_pct is not None:
        q = q.gte('ebitda_pct', min_ebitda_pct)

    q = q.order('ebitda', desc=True).limit(limit)
    resp = q.execute()
    return resp.data or []


# =============================================================================
# Tipo B: Ricerca semantica pura
# =============================================================================

def search_semantic(
    query_text: str,
    for_sale: bool = True,
    limit: int = 20,
) -> list[dict]:
    """Ricerca full semantica via pgvector."""
    embedding = get_embedding(query_text)
    sb = _get_supabase()
    resp = sb.rpc('match_companies', {
        'query_embedding': embedding,
        'only_for_sale': for_sale,
        'match_count': limit,
    }).execute()
    return resp.data or []


# =============================================================================
# Tipo C: Ricerca ibrida (CONSIGLIATA)
# =============================================================================

def search(
    query_text: str | None = None,
    sectors: list[str] | None = None,
    geography: str | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_ebitda_pct: float | None = None,
    for_sale: bool = True,
    limit: int = 15,
    explain: bool = False,
) -> list[dict]:
    """
    Ricerca ibrida:
      1. Filtra strutturalmente (revenue, sector, ecc.)
      2. Ordina per similarità semantica con query_text
      3. Opzionalmente genera spiegazione AI per top 5
    """
    sb = _get_supabase()

    if query_text:
        # Usa la funzione RPC ibrida
        embedding = get_embedding(query_text)
        resp = sb.rpc('match_companies', {
            'query_embedding': embedding,
            'min_revenue': min_revenue,
            'max_revenue': max_revenue,
            'min_ebitda_pct': min_ebitda_pct,
            'filter_sectors': sectors,
            'filter_geography': geography,
            'only_for_sale': for_sale,
            'match_count': limit,
        }).execute()
        results = resp.data or []
    else:
        # Solo filtri strutturati
        results = search_structured(
            sectors=sectors,
            geography=geography,
            min_revenue=min_revenue,
            max_revenue=max_revenue,
            min_ebitda_pct=min_ebitda_pct,
            for_sale=for_sale,
            limit=limit,
        )

    if explain and query_text and results:
        results = _add_explanations(query_text, results[:5])

    return results


# =============================================================================
# Spiegazione AI (GPT-4o-mini)
# =============================================================================

def _add_explanations(buyer_query: str, companies: list[dict]) -> list[dict]:
    """Aggiunge spiegazione del match in italiano per ogni azienda."""
    client = _get_openai()
    for c in companies:
        try:
            prompt = (
                f"Sei un advisor M&A. Il buyer cerca: '{buyer_query}'.\n"
                f"Spiega in 1-2 frasi perché questa azienda è un buon match:\n"
                f"- Nome: {c.get('name')}\n"
                f"- Settore: {c.get('sector')}\n"
                f"- Fatturato: {c.get('revenue', 'N/D')}\n"
                f"- EBITDA%: {c.get('ebitda_pct', 'N/D')}\n"
                f"- Geografia: {c.get('geography')}\n"
                f"- Note: {c.get('short_note', '')}"
            )
            resp = client.chat.completions.create(
                model=GPT_MODEL,
                messages=[{'role': 'user', 'content': prompt}],
                max_tokens=120,
                temperature=0.3,
            )
            c['match_explanation'] = resp.choices[0].message.content.strip()
        except Exception as e:
            c['match_explanation'] = f'(Errore generazione spiegazione: {e})'
    return companies


# =============================================================================
# Dettaglio singola azienda
# =============================================================================

def get_company(slug: str) -> dict | None:
    """Restituisce la scheda completa di un'azienda per slug."""
    sb = _get_supabase()
    resp = sb.table('companies').select('*').eq('slug', slug).limit(1).execute()
    if not resp.data:
        return None
    company = resp.data[0]

    # Aggiungi call notes
    notes_resp = sb.table('call_notes').select(
        'call_date, source, summary, content'
    ).eq('company_id', company['id']).order('call_date', desc=True).execute()
    company['call_notes'] = notes_resp.data or []

    return company


# =============================================================================
# CLI
# =============================================================================

def _fmt_eur(val):
    if val is None:
        return 'N/D'
    if val >= 1_000_000:
        return f'€{val/1_000_000:.1f}M'
    if val >= 1_000:
        return f'€{val/1_000:.0f}K'
    return f'€{val:.0f}'

def _print_results(results: list[dict], query: str = ''):
    if not results:
        print('\nNessun risultato trovato.')
        return
    print(f'\n{"─"*70}')
    print(f'  {len(results)} risultati' + (f' per "{query}"' if query else ''))
    print(f'{"─"*70}')
    for i, c in enumerate(results, 1):
        score = c.get('score')
        score_str = f'  score: {score:.2f}' if score is not None else ''
        print(f'\n#{i}  {c["name"]}  [{c.get("sector","?")}]{score_str}')
        print(f'    📍 {c.get("geography","N/D")}  |  💰 {_fmt_eur(c.get("revenue"))}  |  EBITDA: {c.get("ebitda_pct","N/D")}%')
        print(f'    Status: {c.get("sale_status","N/D")}')
        if c.get('short_note'):
            print(f'    📝 {c["short_note"][:120]}...' if len(c['short_note']) > 120 else f'    📝 {c["short_note"]}')
        if c.get('match_explanation'):
            print(f'    🤖 {c["match_explanation"]}')
    print(f'\n{"─"*70}\n')


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(description='Deal Pipeline Search Engine')
    parser.add_argument('--query',       '-q', type=str, help='Query semantica in linguaggio naturale')
    parser.add_argument('--sector',      '-s', type=str, help='Settore (es. manufacturing, food)')
    parser.add_argument('--geography',   '-g', type=str, help='Area geografica (es. Lombardia)')
    parser.add_argument('--revenue-min', type=float, help='Fatturato minimo (€)')
    parser.add_argument('--revenue-max', type=float, help='Fatturato massimo (€)')
    parser.add_argument('--ebitda-min',  type=float, help='EBITDA% minimo')
    parser.add_argument('--for-sale',    action='store_true', default=True, help='Solo aziende in vendita')
    parser.add_argument('--limit',       type=int, default=15, help='Numero max risultati')
    parser.add_argument('--explain',     action='store_true', help='Genera spiegazione AI per top 5')
    parser.add_argument('--company',     type=str, help='Mostra scheda azienda per slug')
    args = parser.parse_args()

    if args.company:
        company = get_company(args.company)
        if company:
            print(json.dumps(company, indent=2, ensure_ascii=False, default=str))
        else:
            print(f'Azienda non trovata: {args.company}')
    else:
        results = search(
            query_text=args.query,
            sectors=[args.sector] if args.sector else None,
            geography=args.geography,
            min_revenue=args.revenue_min,
            max_revenue=args.revenue_max,
            min_ebitda_pct=args.ebitda_min,
            for_sale=args.for_sale,
            limit=args.limit,
            explain=args.explain,
        )
        _print_results(results, args.query or '')
