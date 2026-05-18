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
    source_name     text         NOT NULL,             -- 'tme'
    laetud_kuupaev  date,
    kirjete_arv     integer,
    status          text         NOT NULL,             -- 'running' | 'success' | 'failed' | 'skipped'
    message         text
);

-- Toorandmed: tarnijakataloogide päevased hetktõmmised (TME andmeallikas)
-- Primaarvõti (tarnija_kood, sumbol, laetud_kuupaev) välistab duplikaadid.
-- Airflow DAG kasutab INSERT ... ON CONFLICT DO NOTHING, mis tagab idempotentsuse.
CREATE TABLE IF NOT EXISTS staging.tooted_raw (
    run_id          uuid            NOT NULL,
    tarnija_kood    text            NOT NULL,           -- nt 'RESIST', 'MCU'
    sumbol          text            NOT NULL,           -- TME tootesümbol (nt 'BCR133')
    nimi            text,                              -- toote kirjeldus
    tootja          text,                              -- tootja nimi
    hind            numeric(12, 4),                    -- ühiku hind (esimene hinnatasand)
    valuuta         text            DEFAULT 'EUR',     -- valuuta
    laoseis         integer,                           -- laos olevate ühikute arv
    kategooria      text,                              -- otsingutermin (nt 'resistors')
    laetud_kell     timestamptz     NOT NULL,
    laetud_kuupaev  date            NOT NULL,
    PRIMARY KEY (tarnija_kood, sumbol, laetud_kuupaev)
);
