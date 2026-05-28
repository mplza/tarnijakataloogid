-- Marts mudel: TOP 10 saadavalolevat kallimat toodet hinnavõrdluse järgi päeva seisuga


WITH ranked AS (
    SELECT
        mpn,
        UPPER(tootja) AS tootja,
        kategooria,
        laetud_kuupaev,
        hind_eur,
        ROW_NUMBER() OVER (
            PARTITION BY mpn, UPPER(tootja), kategooria, laetud_kuupaev
            ORDER BY hind_eur ASC
        ) AS hind_rank
    FROM {{ ref('int_toode_hinnamuutus') }}
    WHERE on_laost_otsas = false
      AND hind_eur IS NOT NULL
)

SELECT *
FROM ranked
WHERE hind_rank = 1
ORDER BY hind_eur DESC
LIMIT 10