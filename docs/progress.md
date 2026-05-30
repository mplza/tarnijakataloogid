# Edenemisraport

## Mis on valmis

- Docker Compose käivitab kõik teenused: analytics-db, Airflow, Superset.
- Open-Meteo API-st saadakse 10 Eesti asukoha 7-päevane tunnipõhine prognoos.
- Airflow DAG laadib andmed `staging.stg_tooted` tabelisse.
- dbt seed laadib tarnijate (`tarnijad`) ja toodete (`tooted`) mappingu andmed.
- dbt staging mudel (`stg_tooted`) puhastab toorandmed.
- dbt intermediate mudel (`int_tootepakkumised_paeviti`) loob saadavalolevad unikaalsed tooted tarnija ja kategooria jaotuses, mis on sisendiks dbt marts mudelitele.
- dbt marts mudelid (`mart_KPI`, `mart_TOP10_kallimat_toodet`) on töös.
- dbt testid läbivad: not_null, unique, accepted_values testid kõigi mudelite kohta.
- Superset on käivitunud aadressil http://localhost:8088 (admin/admin).

## Järgmised sammud

- Luua Superset'is andmebaasi ühendus analytics-db'ga.
- Ehitada 3 chart'i: päevane skoor linnade lõikes, parimad ajaaknad, KPI paarik.
- Eksportida dashboard ZIP-failina → commitida reposse.
- Täpsustada README piirangute ja järelduste osa.

## Mis takistab

- Superset'i esimene käivitamine võtab ~3 minutit — oota, enne kui proovid sisse logida.
- Kui port 8088 on hõivatud, muuda `.env` failis `SUPERSET_PORT_HOST` väärtust.

## Kontrollpunkt

```bash
# Kontrolli, et kõik teenused töötavad
docker compose ps

# Kontrolli dbt mudelite olekut Airflow logidest
docker compose logs airflow | grep -E "dbt (run|test)"

# Käivita DAG käsitsi esimesel korral (kui @hourly veel ei ole käivitanud)
# Ava Airflow UI: http://localhost:8080 → ilmaandmed_pipeline → Trigger DAG
```

Oodatav tulemus: kõik teenused `healthy`, Airflow DAG viimase käivituse olek `success`, dbt testid `passed`.