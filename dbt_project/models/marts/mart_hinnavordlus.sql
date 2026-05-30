-- Marts mudel: hinnavõrdlus mpn × tootja × kategooria × päev tasandil TME vs FARNELL.
-- Näitab iga toote parima hinna ja kättesaadavuse mõlemas poes.
-- Superset kasutab seda mudelit hinnavõrdluse tabeli ja erinevuse-graafiku aluseks.
-- UPPER(tootja) tagab et 'STMicroelectronics' ja 'STMICROELECTRONICS' grupeeritakse kokku.
-- Hinnad on EUR-isse konverteeritud Frankfurter API kursi järgi (vt valuuta_eur_kurss).

WITH ranked AS (
    SELECT
        mpn,
        UPPER(tootja) AS tootja,
        kategooria,
        laetud_kuupaev,
        tarnija_kood,
        hind_eur,
        min_kogus,
        on_laost_otsas,
        ROW_NUMBER() OVER (
            PARTITION BY mpn, UPPER(tootja), kategooria, laetud_kuupaev, tarnija_kood
            ORDER BY hind_eur ASC, min_kogus ASC NULLS LAST, sumbol ASC
        ) AS hind_rank
    FROM {{ ref('int_toode_hinnamuutus') }}
),

koond AS (
    SELECT
        mpn,
        tootja,
        kategooria,
        laetud_kuupaev,
        BOOL_OR(tarnija_kood = 'TME' AND NOT on_laost_otsas)     AS tme_laos,
        BOOL_OR(tarnija_kood = 'FARNELL' AND NOT on_laost_otsas) AS farnell_laos
    FROM ranked
    GROUP BY mpn, tootja, kategooria, laetud_kuupaev
),

parimad AS (
    SELECT
        mpn,
        tootja,
        kategooria,
        laetud_kuupaev,
        MIN(CASE WHEN tarnija_kood = 'TME'     AND hind_rank = 1 THEN hind_eur END)  AS tme_parim_hind_eur,
        MIN(CASE WHEN tarnija_kood = 'TME'     AND hind_rank = 1 THEN min_kogus END) AS tme_min_kogus,
        MIN(CASE WHEN tarnija_kood = 'FARNELL' AND hind_rank = 1 THEN hind_eur END)  AS farnell_parim_hind_eur,
        MIN(CASE WHEN tarnija_kood = 'FARNELL' AND hind_rank = 1 THEN min_kogus END) AS farnell_min_kogus
    FROM ranked
    WHERE hind_rank = 1
    GROUP BY mpn, tootja, kategooria, laetud_kuupaev
)

SELECT
    p.mpn,
    p.tootja,
    p.kategooria,
    p.laetud_kuupaev,
    p.tme_parim_hind_eur,
    p.tme_min_kogus,
    k.tme_laos,
    p.farnell_parim_hind_eur,
    p.farnell_min_kogus,
    k.farnell_laos
FROM parimad p
JOIN koond k
    ON p.mpn = k.mpn
   AND p.tootja = k.tootja
   AND p.kategooria = k.kategooria
   AND p.laetud_kuupaev = k.laetud_kuupaev
