# Edenemisraport

## Mis on valmis

- Docker Compose käivitab kõik teenused: airflow-db, airflow-apiserver, airflow-scheduler, superset.
- TME API-st ja Farnell API-st saadakse 600 MPN-i kohta toodete hinnad ja laoseis.
- Airflow DAG-id laadivad andmed `staging.tooted_raw` tabelisse.
- dbt seed laadib tarnijate (`tarnijad`) ja toodete (`tooted`) mappingu andmed.
- dbt staging mudel (`stg_tooted`) puhastab toorandmed.
- dbt intermediate mudel (`int_tootepakkumised_paeviti`) loob saadavalolevad unikaalsed tooted tarnija ja kategooria jaotuses, mis on sisendiks dbt marts mudelitele.
- dbt marts mudelid (`mart_KPI`, `mart_TOP10_kallimat_toodet`) on töös.
- dbt testid läbivad: not_null, unique, accepted_values testid kõigi mudelite kohta.
- Superset on käivitunud aadressil http://localhost:8090 (admin/admin).

## Järgmised sammud

- Ehitada vähemalt 1 chart: nt hinnavõrdlus TME vs Farnell, TOP 10 kallimat toodet või KPI ülevaade.
- Eksportida dashboard ZIP-failina → commitida reposse.
- Täpsustada README piirangute ja järelduste osa.

## Mis takistab

- Superset dashboard'i eksportimine ja jagamine reposse vajab veel lahendamist.

## Kontrollpunkt

```bash
# Kontrolli, et kõik teenused töötavad
docker compose ps

# Kontrolli dbt mudelite olekut Airflow logidest
docker compose logs airflow | grep -E "dbt (run|test)"

# Käivita DAG käsitsi
# Ava Airflow UI: http://localhost:8083 (airflow / airflow)
# → DAGs → tme_tarnijakataloog_pipeline → Trigger DAG
# → DAGs → element14_tarnijakataloog_pipeline → Trigger DAG
```

Oodatav tulemus: kõik teenused `healthy`, Airflow DAG viimase käivituse olek `success`, dbt testid `passed`.