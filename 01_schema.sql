-- =============================================================================
-- Deal Pipeline AI — Schema Database Supabase
-- =============================================================================
-- Eseguire questo script in: Supabase Dashboard → SQL Editor → New query
-- Prerequisito: abilitare l'estensione pgvector
--   Dashboard → Database → Extensions → cercare "vector" → Enable
-- =============================================================================

-- Abilita pgvector (idempotente)
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- TABELLA: companies
-- Tabella principale con tutti i dati delle aziende seller
-- =============================================================================
CREATE TABLE IF NOT EXISTS companies (
    id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT            NOT NULL,
    slug             TEXT            UNIQUE NOT NULL,       -- es. "mario-rossi-srl"
    sector           TEXT,                                  -- es. "manufacturing", "food", "software"
    subsector        TEXT,
    geography        TEXT,                                  -- es. "Nord Italia", "Lombardia"
    revenue          NUMERIC,                               -- Fatturato ultimo anno (€)
    ebitda           NUMERIC,                               -- EBITDA ultimo anno (€)
    ebitda_pct       NUMERIC,                               -- EBITDA margin %
    employees        INTEGER,
    founded_year     INTEGER,
    for_sale         BOOLEAN         DEFAULT false,
    sale_status      TEXT            DEFAULT 'disponibile', -- 'disponibile' | 'in trattativa' | 'venduto'
    asking_price     NUMERIC,                               -- Prezzo richiesto se noto (€)
    short_note       TEXT,                                  -- Commento breve dall'Excel (max 500 char)
    long_notes       TEXT,                                  -- Note estese aggregate da Drive e call
    embedding        VECTOR(1536),                          -- Embedding OpenAI (pgvector)
    drive_folder_id  TEXT,                                  -- ID cartella Google Drive corrispondente
    last_updated     TIMESTAMPTZ     DEFAULT now(),
    created_at       TIMESTAMPTZ     DEFAULT now(),
    tags             TEXT[],                                -- es. '{export, b2b, nord-italia}'
    data_source      TEXT[]                                 -- es. '{excel, drive, call_notes}'
);

-- Indici per performance
CREATE INDEX IF NOT EXISTS idx_companies_sector    ON companies (sector);
CREATE INDEX IF NOT EXISTS idx_companies_for_sale  ON companies (for_sale);
CREATE INDEX IF NOT EXISTS idx_companies_geography ON companies (geography);
CREATE INDEX IF NOT EXISTS idx_companies_revenue   ON companies (revenue);
CREATE INDEX IF NOT EXISTS idx_companies_slug      ON companies (slug);

-- Indice vettoriale per ricerca semantica veloce (ivfflat)
-- NOTA: creare DOPO aver inserito almeno qualche centinaio di righe
-- CREATE INDEX IF NOT EXISTS idx_companies_embedding
--     ON companies USING ivfflat (embedding vector_cosine_ops)
--     WITH (lists = 100);  -- lists ≈ sqrt(numero_righe)

-- =============================================================================
-- TABELLA: call_notes
-- Note delle singole call (cronologia separata per azienda)
-- =============================================================================
CREATE TABLE IF NOT EXISTS call_notes (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID        NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    call_date   DATE,
    source      TEXT,       -- es. 'otter_ai', 'fireflies', 'manuale'
    content     TEXT,       -- Testo completo della nota/trascrizione
    summary     TEXT,       -- Summary AI (se disponibile)
    embedding   VECTOR(1536),
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_notes_company_id ON call_notes (company_id);
CREATE INDEX IF NOT EXISTS idx_call_notes_call_date  ON call_notes (call_date DESC);

-- =============================================================================
-- TABELLA: buyers
-- Profili buyer salvati per matching proattivo
-- =============================================================================
CREATE TABLE IF NOT EXISTS buyers (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT        NOT NULL,   -- Nome buyer o nome deal
    criteria_text       TEXT,                   -- Descrizione libera dei criteri
    criteria_json       JSONB,                  -- Criteri strutturati
    criteria_embedding  VECTOR(1536),           -- Embedding dei criteri per matching veloce
    created_at          TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- FUNZIONE: match_companies
-- Ricerca ibrida: filtri strutturati + similarità vettoriale
-- Chiamata dall'API Python: supabase.rpc('match_companies', {...})
-- =============================================================================
CREATE OR REPLACE FUNCTION match_companies(
    query_embedding    VECTOR(1536),
    min_revenue        NUMERIC  DEFAULT NULL,
    max_revenue        NUMERIC  DEFAULT NULL,
    min_ebitda_pct     NUMERIC  DEFAULT NULL,
    filter_sectors     TEXT[]   DEFAULT NULL,
    filter_geography   TEXT     DEFAULT NULL,
    only_for_sale      BOOLEAN  DEFAULT true,
    match_count        INT      DEFAULT 20
)
RETURNS TABLE (
    id           UUID,
    name         TEXT,
    slug         TEXT,
    sector       TEXT,
    geography    TEXT,
    revenue      NUMERIC,
    ebitda_pct   NUMERIC,
    for_sale     BOOLEAN,
    sale_status  TEXT,
    short_note   TEXT,
    score        FLOAT
)
LANGUAGE sql STABLE
AS $$
    SELECT
        c.id,
        c.name,
        c.slug,
        c.sector,
        c.geography,
        c.revenue,
        c.ebitda_pct,
        c.for_sale,
        c.sale_status,
        c.short_note,
        1 - (c.embedding <=> query_embedding) AS score
    FROM companies c
    WHERE
        (only_for_sale IS FALSE OR c.for_sale = true)
        AND (min_revenue    IS NULL OR c.revenue    >= min_revenue)
        AND (max_revenue    IS NULL OR c.revenue    <= max_revenue)
        AND (min_ebitda_pct IS NULL OR c.ebitda_pct >= min_ebitda_pct)
        AND (filter_sectors IS NULL OR c.sector = ANY(filter_sectors))
        AND (filter_geography IS NULL OR c.geography ILIKE '%' || filter_geography || '%')
        AND c.embedding IS NOT NULL
    ORDER BY c.embedding <=> query_embedding
    LIMIT match_count;
$$;

-- =============================================================================
-- ROW LEVEL SECURITY (RLS) — abilitare dopo aver configurato l'auth
-- =============================================================================
-- ALTER TABLE companies   ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE call_notes  ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE buyers      ENABLE ROW LEVEL SECURITY;

-- Policy esempio: solo utenti autenticati vedono i dati
-- CREATE POLICY "Authenticated users only" ON companies
--     FOR ALL USING (auth.role() = 'authenticated');

-- =============================================================================
-- TRIGGER: aggiorna last_updated automaticamente
-- =============================================================================
CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.last_updated = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_companies_last_updated
    BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_last_updated();

-- =============================================================================
-- DATI DI TEST (decommentare per testare)
-- =============================================================================
-- INSERT INTO companies (name, slug, sector, geography, revenue, ebitda, ebitda_pct,
--                        employees, for_sale, sale_status, short_note, tags)
-- VALUES
-- ('Esempio SRL', 'esempio-srl', 'manufacturing', 'Nord Italia',
--  3500000, 420000, 12.0, 45, true, 'disponibile',
--  'Azienda manifatturiera con forte export in Europa', ARRAY['export', 'b2b', 'nord-italia']),
-- ('Beta Food SpA', 'beta-food-spa', 'food', 'Lombardia',
--  8200000, 1100000, 13.4, 120, true, 'in trattativa',
--  'Produzione alimentare premium, canale GDO e HoReCa', ARRAY['gdo', 'horeca', 'premium']);
