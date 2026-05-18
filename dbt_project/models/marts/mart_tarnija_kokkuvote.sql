-- Marts mudel: koondstatistika tarnija kaupa (viimane hetktõmmis).
-- Kuvab iga tarnija kataloogi mahtu, hinnastruktuuri, laoseisu ja hinnamuutusi.
-- Superset kasutab seda mudelit tarnijate võrdlustabeli ja tulpdiagrammi aluseks.

WITH viimane_paev AS (
    SELECT MAX(laetud_kuupaev) AS kuupaev
    FROM {{ ref('int_toode_hinnamuutus') }}
),

koond AS (
    SELECT
        i.tarnija_kood,
        i.tarnija_nimi,
        i.riik,
        i.laetud_kuupaev,
        COUNT(*)                                            AS toodete_arv,
        ROUND(AVG(i.hind), 4)                              AS kesk_hind,
        ROUND(MIN(i.hind), 4)                              AS min_hind,
        ROUND(MAX(i.hind), 4)                              AS max_hind,
        SUM(CASE WHEN i.on_laost_otsas THEN 1 ELSE 0 END)  AS laost_otsas_arv,
        COUNT(DISTINCT i.kategooria)                        AS kategooriate_arv,
        COUNT(*) FILTER (WHERE i.hinna_suund = 'Hinnatõus')
                                                            AS hinnatousud,
        COUNT(*) FILTER (WHERE i.hinna_suund = 'Hinnalangetamine')
                                                            AS hinnalangetused
    FROM {{ ref('int_toode_hinnamuutus') }} i
    CROSS JOIN viimane_paev
    WHERE i.laetud_kuupaev = viimane_paev.kuupaev
    GROUP BY i.tarnija_kood, i.tarnija_nimi, i.riik, i.laetud_kuupaev
)

SELECT
    *,
    ROUND(laost_otsas_arv::numeric / NULLIF(toodete_arv, 0) * 100, 1)
                                                AS laopuudus_pct,
    RANK() OVER (ORDER BY toodete_arv DESC)     AS kataloogi_maht_rank
FROM koond
