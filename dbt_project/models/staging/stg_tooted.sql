-- Staging mudel: puhastab ja rikastab tarnijakataloogi toorandmed (TME + Farnell).
-- Lisab kuupäevaväljad ja märgistab laost otsas olevad tooted.
-- Allahindlust ega kasutajahinnangut ei ole, seega loplik_hind = hind.

SELECT
    run_id,
    tarnija_kood,
    sumbol,
    mpn,
    nimi,
    tootja,
    hind,
    valuuta,
    valuuta_eur_kurss,
    ROUND(hind * valuuta_eur_kurss, 4)                       AS hind_eur,
    hind                                                     AS loplik_hind,
    min_kogus,
    laoseis,
    CASE WHEN laoseis IS NULL OR laoseis = 0
         THEN true
         ELSE false
    END                                                      AS on_laost_otsas,
    kategooria,
    laetud_kell,
    laetud_kuupaev,
    EXTRACT(YEAR  FROM laetud_kuupaev)::integer               AS aasta,
    EXTRACT(MONTH FROM laetud_kuupaev)::integer               AS kuu
FROM {{ source('staging', 'tooted_raw') }}
WHERE hind         IS NOT NULL
  AND hind          > 0
  AND sumbol       IS NOT NULL
  AND mpn          IS NOT NULL
  AND tarnija_kood IS NOT NULL
