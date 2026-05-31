-- Marts mudel: päevased min/max hinnad eurodes toote, tarnija, tootja kaupa
-- Kuvab iga toote soodsaimat ja kalleimat pakkumist ning nende vahet iga päev. 
-- Kasulik toote hinnavõrdluseks ja turu analüüsiks.


WITH ranked AS (

    SELECT
        laetud_kuupaev,
        mpn,
        nimi,
        kategooria,

        tarnija_kood,
        tootja,
        hind_eur,

        ROW_NUMBER() OVER (
            PARTITION BY laetud_kuupaev, mpn
            ORDER BY hind_eur ASC, tarnija_kood ASC
        ) AS rn_soodsaim,

        ROW_NUMBER() OVER (
            PARTITION BY laetud_kuupaev, mpn
            ORDER BY hind_eur DESC, tarnija_kood ASC
        ) AS rn_kalleim

    FROM {{ ref('int_toote_hinnad_paeviti') }}

    WHERE hind_eur IS NOT NULL
      AND on_laost_otsas = false

),

aggregated AS (

    SELECT
        laetud_kuupaev,
        mpn,

        MIN(hind_eur) AS min_hind_eur,
        MAX(hind_eur) AS max_hind_eur,
        COUNT(DISTINCT tarnija_kood) AS pakkujate_arv

    FROM ranked

    GROUP BY
        laetud_kuupaev,
        mpn

),

final AS (

    SELECT
        a.laetud_kuupaev,
        a.mpn,
        MAX(CASE WHEN r.rn_soodsaim = 1 THEN r.nimi END) AS nimi,
        MAX(CASE WHEN r.rn_soodsaim = 1 THEN r.kategooria END) AS kategooria,

        a.min_hind_eur,
        MAX(CASE WHEN r.rn_soodsaim = 1 THEN r.tarnija_kood END) AS soodsaim_tarnija_kood,
        MAX(CASE WHEN r.rn_soodsaim = 1 THEN r.tootja END) AS soodsaim_tootja,

        a.max_hind_eur,
        MAX(CASE WHEN r.rn_kalleim = 1 THEN r.tarnija_kood END) AS kalleim_tarnija_kood,
        MAX(CASE WHEN r.rn_kalleim = 1 THEN r.tootja END) AS kalleim_tootja,

        a.max_hind_eur - a.min_hind_eur AS hinnavahe_eur,

        ROUND(
            (a.max_hind_eur - a.min_hind_eur)
            / NULLIF(a.min_hind_eur, 0) * 100,
            2
        ) AS hinnavahe_pct,

        a.pakkujate_arv

    FROM aggregated a
    LEFT JOIN ranked r
        ON a.laetud_kuupaev = r.laetud_kuupaev
       AND a.mpn = r.mpn

    GROUP BY
        a.laetud_kuupaev,
        a.mpn,
        a.min_hind_eur,
        a.max_hind_eur,
        a.pakkujate_arv

)

SELECT *
FROM final

