-- Marts mudel: päevased tootehinnad eurodes ja nende muutused protsentides võrreldes eelmise päeva või viimase kirjega. 
-- Kuvab ka hinnamuutuse suuna (tõus, langus, muutuseta).
WITH base AS (
    SELECT
        *,
           CASE
            WHEN eelmine_hind_eur IS NULL     THEN 'Esimene kirje'
            WHEN hind_eur > eelmine_hind_eur      THEN 'Hinnatõus'
            WHEN hind_eur < eelmine_hind_eur      THEN 'Hinnalangetamine'
            ELSE                               'Muutuseta'
        END AS hinna_suund
    FROM {{ ref('int_toode_hinnamuutus') }} s
),


SELECT * 
FROM base