-- Marts mudel: hinnavõrdlus mpn × tootja × kategooria × päev tasandil TME vs FARNELL.
-- Näitab iga toote parima hinna (väikseima qty tasandilt EUR-is) ja kättesaadavuse mõlemas poes.
-- Superset kasutab seda mudelit hinnavõrdluse tabeli ja erinevuse-graafiku aluseks.
-- UPPER(tootja) tagab et 'STMicroelectronics' ja 'STMICROELECTRONICS' grupeeritakse kokku.
-- Hinnad on EUR-isse konverteeritud Frankfurter API kursi järgi (vt valuuta_eur_kurss).

SELECT
    mpn,
    UPPER(tootja)                                                  AS tootja,
    kategooria,
    laetud_kuupaev,
    MIN(CASE WHEN tarnija_kood = 'TME'     THEN hind_eur END)      AS tme_parim_hind_eur,
    MIN(CASE WHEN tarnija_kood = 'TME'     THEN min_kogus END)     AS tme_min_kogus,
    BOOL_OR(tarnija_kood = 'TME' AND NOT on_laost_otsas)           AS tme_laos,
    MIN(CASE WHEN tarnija_kood = 'FARNELL' THEN hind_eur END)      AS farnell_parim_hind_eur,
    MIN(CASE WHEN tarnija_kood = 'FARNELL' THEN min_kogus END)     AS farnell_min_kogus,
    BOOL_OR(tarnija_kood = 'FARNELL' AND NOT on_laost_otsas)       AS farnell_laos
FROM {{ ref('int_toode_hinnamuutus') }}
GROUP BY mpn, UPPER(tootja), kategooria, laetud_kuupaev
