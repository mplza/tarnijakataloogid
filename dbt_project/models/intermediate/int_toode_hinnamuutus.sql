-- Intermediate mudel: arvutab päevased hinnamuutused toote kaupa.
--
-- Sammud:
--   1. base   — ühendab staging andmed tarnijainfo seediga, leiab eelmise päeva hinna
--   2. muutus — arvutab protsentuaalse päevase hinnamuutuse ja suuna
--
-- LAG() aken töötab tarnija + sümbol tasandil: võrdleb tänast hinda
-- eelmise laaditud päeva hinnaga. Esimesel päeval on eelmine hind NULL.

WITH base AS (
    SELECT
        s.tarnija_kood,
        s.sumbol,
        s.nimi,
        s.tootja,
        s.laetud_kuupaev,
        s.hind,
        s.loplik_hind,
        s.valuuta,
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
),

muutus AS (
    SELECT
        *,
        CASE
            WHEN eelmine_hind IS NOT NULL AND eelmine_hind <> 0
            THEN ROUND((hind - eelmine_hind) / eelmine_hind * 100, 2)
            ELSE NULL
        END AS hinna_muutus_pct,
        CASE
            WHEN eelmine_hind IS NULL     THEN 'Esimene kirje'
            WHEN hind > eelmine_hind      THEN 'Hinnatõus'
            WHEN hind < eelmine_hind      THEN 'Hinnalangetamine'
            ELSE                               'Muutuseta'
        END AS hinna_suund
    FROM base
)

SELECT * FROM muutus
