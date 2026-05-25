-- Marts mudel: hinnajaotus kategooria ja päeva kaupa.
-- Näitab iga elektroonikakategooria hinnakujundust ajas: min/max/keskmist hinda,
-- toodete arvu, laoseisu ja hinnamuutuste suundumusi.
-- Superset kasutab seda mudelit kategooriate trendigraafiku ja heatmapi aluseks.

SELECT
    kategooria,
    laetud_kuupaev,
    aasta,
    kuu,
    COUNT(DISTINCT mpn)                                         AS toodete_arv,
    COUNT(DISTINCT tarnija_kood)                                AS tarnijate_arv,
    ROUND(MIN(hind_eur), 4)                                     AS min_hind_eur,
    ROUND(MAX(hind_eur), 4)                                     AS max_hind_eur,
    ROUND(AVG(hind_eur), 4)                                     AS kesk_hind_eur,
    SUM(CASE WHEN on_laost_otsas THEN 1 ELSE 0 END)             AS laost_otsas_arv,
    COUNT(*) FILTER (WHERE hinna_suund = 'Hinnatõus')           AS hinnatousud,
    COUNT(*) FILTER (WHERE hinna_suund = 'Hinnalangetamine')    AS hinnalangetused
FROM {{ ref('int_toode_hinnamuutus') }}
GROUP BY kategooria, laetud_kuupaev, aasta, kuu
