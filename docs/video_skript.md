# Demo video skript: E-poe tarnijakataloogide haldus- ja seirepaneel

**Kogupikkus:** kuni 10 minutit
**Keel:** Eesti keel
**Formaat:** ekraanisalvestus koos häälega

---

## 1. Probleem ja äriküsimus (1 minut)

> *Ekraanil: README-i tiitelrida või lihtne slaid pealkirjaga.*

---

Tere. Täna tutvustame andmetorude projekti, mis lahendab praktilise e-kaubanduse probleemi.

Elektroonikakomponente müüb üle maailma kümneid hulgimüüjaid, igaüks oma hindade, valuutade ja laovaruga. Ostujuhi või insenerina on keeruline vastata lihtsale küsimusele: milline tarnija pakub sama toodet soodsaima hinnaga ja kellel on see hetkel laos?

Meie projekt vastab sellele küsimusele automaatselt. Laadime iga päev hinnaandmed kahest suurest Euroopa elektroonikamüüjast, TME-st Poolast ja Farnell UK-st Suurbritanniast, töötleme need dbt abil ning kuvame tulemused Apache Superseti interaktiivsel näidikulaual.

Konkreetsed mõõdikud on: TOP 10 kallimat toodet, kategooriate hinnajaotus, tarnijate võrdlustabel ning KPI-d unikaalsete toodete, tarnijate ja tootjate arvu kohta. Andmebaasis on 600 toote hinnad kahest allikast, mis teeb hinnavõrdluse otse võrreldavaks.

---

## 2. Arhitektuur ja tööriistade valik (2 minutit)

> *Ekraanil: arhitektuuriskeem docs/arhitektuur.md-ist.*

---

Vaatame süsteemi ülesehitust.

Andmevoog koosneb kolmest põhikihist.

**Sissevõtt.** Apache Airflow orchestreerib kahte iseseisvat DAG-i, ühe TME jaoks ja teise Farnell UK jaoks. See on teadlik arhitektuuriotsus: kui üks API langeb, teine jätkab tööd sõltumatult. TME kasutab OAuth 2.0 autentimist, Bearer token kehtib 5 minutit ja uuendatakse automaatselt. Farnell kasutab API võtit. Kumbki DAG hangib iga päev 600 tootenumbri hinnad ja laoseisu ning salvestab need PostgreSQL-i staging kihti päevase hetktõmmisena.

Üks nüanss: TME annab hinnad eurodes, Farnell naelsterlingites. GBP-EUR kurssi küsitakse Frankfurter API-st, mis kasutab Euroopa Keskpanga ametlikke kursse. Kursiväärtus salvestatakse staging-kihis iga rea juures, et tagantjärele saaks kontrollida, millise kursiga arvutus tehti.

**Transformatsioon.** dbt Core ehitab staging andmetest kolm kihti. Staging view puhastab toorandmed ja märgistab, millised tooted on laost otsas. Intermediate kiht arvutab hinnamuutused LAG()-funktsiooni abil ja koondab päevased unikaalsed pakkumised. Marts kiht on analüütiline lõppkiht: tarnijate koondstatistika, kategooriate hinnajaotus, KPI-d, TOP 10 kalleim toode ja hinnavõrdlus TME vs Farnell kõrvuti.

**Tööriistade valik.** Airflow sobib hästi kahe paralleelse igapäevase DAG-i haldamiseks. dbt muudab transformatsioonid dokumenteeritavaks ja testitud. PostgreSQL on hostitav Neon pilveplatvormil, mis eemaldab vajaduse ise andmebaasiserverit hallata. Superset on võimsa filtreeringu ja diagrammidega avatud lähtekoodiga BI-tööriist.

---

## 3. Demo: töövoog ja näidikulaud töös (3 kuni 4 minutit)

> *Ekraanil: brauser Airflow UI-ga, seejärel Superset.*

---

Näitame nüüd, kuidas kõik see päriselus töötab.

**3a. Airflow DAG-ide käivitamine**

> *Ekraanil: Airflow UI, localhost:8083, mõlemad DAG-id nähtaval.*

Avame Airflow. Vasakul näeme kahte DAG-i: `tme_tarnijakataloog_pipeline` TME jaoks ja `element14_tarnijakataloog_pipeline` Farnell UK jaoks. Mõlemad on seatud käima igapäevaselt.

Käivitame ühe DAG-i käsitsi, et näidata töövoogu. Iga DAG koosneb neljast taskist. Esimene task `dbt_seed` laadib CSV-viitetabelid (tooted.csv, tarnijad.csv) andmebaasi. Teine task `laadi_kataloogid` hangib API-st 600 tootenumbri hinnad. Näeme Airflow logis, kuidas token uuendatakse, päringud saadetakse ja read salvestatakse staging tabelisse. `ON CONFLICT DO NOTHING` tagab, et sama päeva andmeid ei laadita topelt. Kolmas task `dbt_run` ehitab kõik SQL-mudelid staging-ist läbi intermediate kuni mart-kihini. Neljas task `dbt_test` käivitab 59-testilise test-suite'i. Näeme, et kõik testid saavad rohelise tulukese.

**3b. Superset näidikulaud**

> *Ekraanil: Superset, localhost:8090, näidikulaud avatud.*

Avame Supersetis näidikulaua. Vaatame järjest olulisimad vaated.

Esimene vaade on KPI-riba. See näitab hetkeseisu: mitu unikaalset toodet, tarnijat, tootjat ja kategooriat andmebaasis on. Need arvud uuenevad iga päev automaatselt pärast DAG-i edukat käivitust.

Teine vaade on hinnavõrdlus TME vs Farnell. Tabelis on kõrvuti TME hind eurodes ja Farnell hind eurodes, koos miinimum- ja maksimumhinnaga ning odavaima pakkuja märgendiga. Siin näeme kohe, et teatud tootekategoorias on Farnell keskmiselt kallim, kuigi üksikutel toodetel on hinnad vastupidised. Filtreid kasutades saame vaadata ainult ühte kategooriat, näiteks mikroprotsessoreid.

Kolmas vaade on TOP 10 kalleim toode. Tulpdiagramm näitab, millised konkreetsed tooted on kõige kõrgema ühikuhinnaga, millise tarnija kaudu ja millises valuutas. See aitab otsustada, kus on mõtet mahuhinnast läbi rääkida.

Neljas vaade on kategooriate hinnajaotus. Karpdiagramm näitab hindade hajuvust igas kategoorias. Näeme, et sensorite kategoorias on hinnavahemik palju laiem kui resistorite kategoorias, mis viitab suuremale võimalusele optimeerida tarnijat.

---

## 4. Andmekvaliteet ja turve (1 kuni 2 minutit)

> *Ekraanil: Airflow logi dbt_test taskist, roheline olekurida.*

---

Andmekvaliteet on selles projektis täielikult automatiseeritud.

Projektis on 59 automaatset dbt testi, mis käivituvad iga DAG-i jooksuga.

Staging kihis on 17 testi. Need kontrollivad not_null väljasid kriitilistes veergudes nagu hind, tootenumber ja kuupäev. Samuti kontrollitakse referentsiaalset terviklikkust: iga tarnija kood staging-tabelis peab esinema ka `tarnijad.csv` seed-tabelis. Unikaalsust kontrollitakse tarnija koodi järgi seed-tabelis.

Intermediate kihis on 17 testi, mis tagavad, et hinnamuutuste arvutus ja päevased pakkumised ei sisalda null-väärtusi kriitilistes veergudes.

Marts kihis on 25 testi. Need katavad viit mart-mudelit: tarnija koondstatistika, kategooriate hinnajaotus, KPI ülevaade, TOP 10 kallimat toodet ja hinnavõrdlus. Lisaks not_null kontrollidele on unikaalsuse testid KPI kuupäeva ja tarnija koodi järgi.

Andmeturbe poolt: projektis isikuandmeid ei ole. TME ja Farnell API-d tagastavad ainult tootekataloogide infot. API võtmed, andmebaasi parool ja DATABASE_URL on `.env` failis, mis on `.gitignore`-s ja mida kunagi reposse ei panda. Repos on ainult `.env.example`. GBP-EUR kursi arvutuses kasutatakse Frankfurter API-t, mis toetub EKP kursile, ning fallback-kurss 1.18 on kasutusel, kui API ei vasta.

---

## 5. Õppetunnid ja refleksioon (1 minut)

> *Ekraanil: lihtne slaid meeskonnaga või lihtsalt kõneleja.*

---

Mida see projekt meile õpetas?

Esiteks: kahe eraldi DAG-i arhitektuur osutus õigeks valikuks. Testimisel ebaõnnestus Farnell API korra rate limit-i tõttu. TME DAG jätkas tööd ja andmebaas jäi kasutatavaks, kuigi ühe päeva Farnell andmed puudusid.

Teiseks: dbt muutis transformatsioonide halduse palju lihtsamaks. Iga mudeli loogika on eraldi SQL-failis, testid on schema.yml-is kirjas ja dokumentatsioon genereeritakse automaatselt. Ilma dbt-ta oleks 59 testi käsitsi hallata väga keeruline.

Kolmandaks: valuutakonversioon pidi olema lihtne, aga osutus keerukamaks. GBP-EUR kurss muutub iga päev ja tagantjärele analüüs nõuab, et iga rea juures oleks salvestatud kurss, millega arvutus tehti.

Kui aega oleks rohkem, lisaksime inkrementaalse laadimise, mis töötleks ainult uusi snapshot-e, mitte kogu ajaloolist tabelit. Samuti oleks huvitav lisada hinnaajaloo graafik Supersetis, et näha, kuidas ühe toote hind on muutunud nädala või kuu lõikes.

Täname tähelepanu eest. Küsimused on teretulnud.

---

*Meeskond: Merilin Paas-Loeza (kvaliteet), Triin Bulõgina (transformatsioonid), Bernard Puström (andmeallikas), Martin Aasna (näidikulaud)*
