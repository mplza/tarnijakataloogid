-- Marts mudel: päevased tootehinnad eurodes ja nende muutused protsentides võrreldes eelmise päeva või viimase kirjega. 
-- Kuvab ka hinnamuutuse suuna (tõus, langus, muutuseta), mis tuleb
-- int_toode_hinnamuutus mudelist (hinna_suund arvutatakse seal).

SELECT *
FROM {{ ref('int_toode_hinnamuutus') }}