-- Marts mudel: KPI ülevaade: unikaalsete toodete, tootjate, tarnijate ja kategooriate arv iga päeva seisuga
-- 1. Unikaalsete tootjate arv
-- 2. Unikaalsete tarnijate arv
-- 3. Unikaalsete toodete arv
-- 4. Elektroonikakomponentide kategooriate arv

SELECT laetud_kuupaev,
       count(distinct toote_nimi) AS unikaalsed_tooted,
       count(distinct tootja) AS unikaalsed_tootjad,
       count(distinct kategooria) AS unikaalsed_kategooriad,
       count(distinct tarnija_kood) AS unikaalsed_tarnijad
FROM {{ ref('int_tootepakkumised_paeviti') }}
GROUP BY laetud_kuupaev