-- Loob andmebaasi skeemid ja toorandmete tabelid.
-- See skript käivitatakse automaatselt analytics-db konteineri käivitumisel
-- (docker-entrypoint-initdb.d mehhanism).

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Pipeline auditilogi: iga DAG käivituse kirje
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id          uuid         PRIMARY KEY,
    fetched_at      timestamptz  NOT NULL,
    source_name     text         NOT NULL,             -- 'tme' | 'farnell'
    laetud_kuupaev  date,
    kirjete_arv     integer,
    status          text         NOT NULL,             -- 'running' | 'success' | 'failed' | 'skipped'
    message         text
);

-- Toorandmed: tarnijakataloogide päevased hetktõmmised (TME + Farnell allikad).
-- Primaarvõti (tarnija_kood, sumbol, laetud_kuupaev) välistab duplikaadid.
-- Airflow DAG kasutab INSERT ... ON CONFLICT DO NOTHING, mis tagab idempotentsuse.
-- `mpn` = tooted.csv seedist pärinev tootja-osakood, mille alusel päring tehti.
-- `sumbol` = poe enda sisemine kood (TME symbol või Farnell SKU) — võib MPN-ist erineda.
-- `min_kogus` = vähim tellitav kogus salvestatud hinna juures (TME tavaliselt 1, Farnell varieerub).
CREATE TABLE IF NOT EXISTS staging.tooted_raw (
    run_id          uuid            NOT NULL,
    tarnija_kood    text            NOT NULL,           -- 'TME' | 'FARNELL'
    sumbol          text            NOT NULL,           -- poe enda kood (TME symbol / Farnell SKU)
    mpn             text            NOT NULL,           -- tooted.csv seedist pärinev tootja-osakood
    nimi            text,                              -- toote kirjeldus
    tootja          text,                              -- tootja nimi
    hind                numeric(12, 4),                -- ühiku hind (väikseim kogusetasand) poodi valuutas
    valuuta             text        DEFAULT 'EUR',     -- valuuta (TME=EUR, Farnell=GBP)
    valuuta_eur_kurss   numeric(10, 6),                -- valuutakurss EUR-i suunas (TME=1.0, Farnell~1.18)
    min_kogus           integer,                       -- minimaalne tellitav kogus selle hinna juures
    laoseis         integer,                           -- laos olevate ühikute arv
    kategooria      text,                              -- tooted.csv seedist (mitte otsingutermin)
    laetud_kell     timestamptz     NOT NULL,
    laetud_kuupaev  date            NOT NULL,
    PRIMARY KEY (tarnija_kood, sumbol, laetud_kuupaev)
);

-- Sekundaarindeks: MPN-i järgi otsing (mart_hinnavordlus.sql vajab)
CREATE INDEX IF NOT EXISTS tooted_raw_mpn_idx ON staging.tooted_raw (mpn, laetud_kuupaev);
