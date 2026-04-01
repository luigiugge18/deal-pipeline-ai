-- =============================================================================
-- Deal Pipeline AI — Schema Database Supabase v2
-- Basato sul foglio AIDA (Spurghi_AIDA)
-- =============================================================================
-- Eseguire questo script in: Supabase Dashboard → SQL Editor → New query
-- =============================================================================

-- Abilita pgvector (idempotente)
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- TABELLA: companies
-- =============================================================================
DROP TABLE IF EXISTS call_notes CASCADE;
DROP TABLE IF EXISTS companies  CASCADE;

CREATE TABLE companies (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    slug                TEXT        UNIQUE NOT NULL,           -- URL-friendly, generato da ragione_sociale
    ragione_sociale     TEXT        NOT NULL,
    partita_iva         TEXT,
    ateco_codice        TEXT,                                  -- ATECO 2007 codice
    regione             TEXT,
    provincia           TEXT,
    comune              TEXT,
    telefono            TEXT,
    website             TEXT,
    data_bilancio       DATE,                                  -- Data chiusura ultimo bilancio

    -- EBITDA (5 anni: 0 = ultimo disponibile)
    ebitda_0            BIGINT,
    ebitda_1            BIGINT,
    ebitda_2            BIGINT,
    ebitda_3            BIGINT,
    ebitda_4            BIGINT,

    -- EBITDA margin % (5 anni)
    ebitda_margin_0     NUMERIC(8,2),
    ebitda_margin_1     NUMERIC(8,2),
    ebitda_margin_2     NUMERIC(8,2),
    ebitda_margin_3     NUMERIC(8,2),
    ebitda_margin_4     NUMERIC(8,2),

    -- Ricavi delle vendite (5 anni)
    ricavi_0            BIGINT,
    ricavi_1            BIGINT,
    ricavi_2            BIGINT,
    ricavi_3            BIGINT,
    ricavi_4            BIGINT,

    -- Governance / persone
    azionisti           TEXT,
    csh_nome            TEXT,                                  -- Controller / CSH
    dm_nome             TEXT,                                  -- Decision Maker nome
    dm_codice_fiscale   TEXT,                                  -- Decision Maker CF

    -- Qualificazione interesse
    is_interessante     BOOLEAN,
    livello_interesse   TEXT,                                  -- 'chiaro' | 'potenziale' | NULL

    -- Vettore embedding per ricerca semantica
    embedding           VECTOR(1536),

    last_updated        TIMESTAMPTZ DEFAULT now(),
    created_at          TIMESTAMPTZ DEFAULT now()
);

-- Indici per filtri strutturati
CREATE INDEX idx_companies_regione    ON companies (regione);
CREATE INDEX idx_companies_provincia  ON companies (provincia);
CREATE INDEX idx_companies_ateco      ON companies (ateco_codice);
CREATE INDEX idx_companies_ricavi     ON companies (ricavi_0);
CREATE INDEX idx_companies_ebitda     ON companies (ebitda_0);
CREATE INDEX idx_companies_slug       ON companies (slug);

-- Indice vettoriale (abilitare dopo aver inserito almeno 200 righe):
-- CREATE INDEX idx_companies_embedding ON companies
--     USING ivfflat (embedding vector_cosine_ops) WITH (lists = 50);

-- =============================================================================
-- TABELLA: call_notes
-- =============================================================================
CREATE TABLE call_notes (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id  UUID        NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    call_date   DATE,
    source      TEXT,
    content     TEXT,
    summary     TEXT,
    embedding   VECTOR(1536),
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_call_notes_company_id ON call_notes (company_id);
CREATE INDEX idx_call_notes_call_date  ON call_notes (call_date DESC);

-- =============================================================================
-- TABELLA: buyers
-- =============================================================================
DROP TABLE IF EXISTS buyers CASCADE;
CREATE TABLE buyers (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT        NOT NULL,
    criteria_text       TEXT,
    criteria_json       JSONB,
    criteria_embedding  VECTOR(1536),
    created_at          TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- FUNZIONE: match_companies (ricerca ibrida vettoriale + filtri)
-- =============================================================================
CREATE OR REPLACE FUNCTION match_companies(
    query_embedding      VECTOR(1536),
    min_ricavi           BIGINT   DEFAULT NULL,
    max_ricavi           BIGINT   DEFAULT NULL,
    min_ebitda_pct       NUMERIC  DEFAULT NULL,
    filter_ateco         TEXT[]   DEFAULT NULL,
    filter_regione       TEXT     DEFAULT NULL,
    match_count          INT      DEFAULT 20,
    filter_interessanti  BOOLEAN  DEFAULT TRUE
)
RETURNS TABLE (
    id                 UUID,
    ragione_sociale    TEXT,
    slug               TEXT,
    ateco_codice       TEXT,
    regione            TEXT,
    provincia          TEXT,
    comune             TEXT,
    ricavi_0           BIGINT,
    ebitda_0           BIGINT,
    ebitda_margin_0    NUMERIC,
    website            TEXT,
    dm_nome            TEXT,
    note               TEXT,
    contatti           TEXT,
    next_steps         TEXT,
    sheet_row          INT,
    is_interessante    BOOLEAN,
    livello_interesse  TEXT,
    esclusiva          BOOLEAN,
    altro              JSONB,
    score              FLOAT
)
LANGUAGE sql STABLE
AS $func$
    SELECT
        c.id,
        c.ragione_sociale,
        c.slug,
        c.ateco_codice,
        c.regione,
        c.provincia,
        c.comune,
        c.ricavi_0,
        c.ebitda_0,
        c.ebitda_margin_0,
        c.website,
        c.dm_nome,
        c.note,
        c.contatti,
        c.next_steps,
        c.sheet_row,
        c.is_interessante,
        c.livello_interesse,
        c.esclusiva,
        c.altro,
        1 - (c.embedding <=> query_embedding) AS score
    FROM companies c
    WHERE
        (min_ricavi         IS NULL OR c.ricavi_0        >= min_ricavi)
        AND (max_ricavi     IS NULL OR c.ricavi_0        <= max_ricavi)
        AND (min_ebitda_pct IS NULL OR c.ebitda_margin_0 >= min_ebitda_pct)
        AND (filter_ateco   IS NULL OR c.ateco_codice = ANY(filter_ateco))
        AND (filter_regione IS NULL OR strpos(lower(c.regione), lower(filter_regione)) > 0)
        AND (NOT filter_interessanti OR c.is_interessante = TRUE)
        AND c.embedding IS NOT NULL
    ORDER BY c.embedding <=> query_embedding
    LIMIT match_count;
$func$;

-- =============================================================================
-- TRIGGER: aggiorna last_updated
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
