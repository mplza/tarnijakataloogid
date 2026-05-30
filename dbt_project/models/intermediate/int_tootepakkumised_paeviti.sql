-- Intermediate mudel: Iga päevased saadavalolevad unikaalsed tooted tarnija ja kategooria jaotuses
--
-- Sammud:
--   1. base   — ühendab staging andmed tarnijainfo seediga
--   2. lõpp   — valib unikaalsed tooted iga päev, tarnija ja kategooria lõikes


WITH base AS (
    SELECT
        s.tarnija_kood,
        s.nimi,
        s.mpn,
        s.tootja,
        s.laetud_kuupaev,
        s.kategooria,
        t.tarnija_nimi
    FROM {{ ref('stg_tooted') }} s
    LEFT JOIN {{ ref('tarnijad') }} t
        ON s.tarnija_kood = t.tarnija_kood
        WHERE on_laost_otsas = false
)

SELECT DISTINCT
laetud_kuupaev,
tarnija_kood, 
tarnija_nimi,
tootja, 
nimi as toote_nimi, 
mpn,
kategooria  
    FROM base

