-- Intermediate mudel: päevased hinnad toote, tarnija, tootja kaupa
--
-- Sammud:
--   1. base   — ühendab staging andmed tarnijainfo seediga
--   2.  


WITH base AS (
    SELECT
        s.tarnija_kood,
        s.sumbol,
        s.mpn,
        s.nimi,
        s.tootja,
        s.laetud_kuupaev,
        s.hind,
        s.hind_eur,
        s.loplik_hind,
        s.valuuta,
        s.min_kogus,
        s.laoseis,
        s.on_laost_otsas,
        s.kategooria,
        s.aasta,
        s.kuu,
        LAG(s.hind) OVER (
            PARTITION BY s.tarnija_kood, s.sumbol
            ORDER BY s.laetud_kuupaev
        ) AS eelmine_hind,
        t.tarnija_nimi,
        t.riik
    FROM {{ ref('stg_tooted') }} s
    LEFT JOIN {{ ref('tarnijad') }} t
        ON s.tarnija_kood = t.tarnija_kood
    WHERE s.hind IS NOT NULL
)

SELECT *
FROM base
