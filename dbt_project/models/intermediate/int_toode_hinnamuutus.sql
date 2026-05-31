-- Intermediate mudel: arvutab päevased hinnamuutused toote kaupa.
--
-- Sammud:
--   1. base   — ühendab staging andmed tarnijainfo seediga, leiab eelmise päeva hinna
--   2. muutus — arvutab protsentuaalse päevase hinnamuutuse 
--
-- LAG() aken töötab tarnija + sümbol tasandil: võrdleb tänast hinda
-- eelmise laaditud päeva hinnaga. Esimesel päeval on eelmine hind NULL.

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
        LAG(s.hind_eur) OVER (
            PARTITION BY s.tarnija_kood, s.sumbol
            ORDER BY s.laetud_kuupaev
        ) AS eelmine_hind_eur,
        t.tarnija_nimi,
        t.riik
    FROM {{ ref('stg_tooted') }} s
    LEFT JOIN {{ ref('tarnijad') }} t
        ON s.tarnija_kood = t.tarnija_kood
),

muutus AS (
    SELECT
        *,
        CASE
            WHEN eelmine_hind_eur IS NOT NULL AND eelmine_hind_eur <> 0
            THEN ROUND((hind_eur - eelmine_hind_eur) / eelmine_hind_eur * 100, 2)
            ELSE NULL
        END AS hinna_muutus_pct
    FROM base
)

SELECT * FROM muutus
