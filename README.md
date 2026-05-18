# Andmeinseneeria projektitöö — E-poe tarnijakataloogide haldus- ja seirepaneel

Andmetorude pipeline, mis laadib e-poe tarnijakataloogide andmed, töötleb
need dbt abil ning kuvab tulemused Apache Supersetis.

## Stack

| Komponent | Tööriist |
|-----------|---------|
| Orkestreerimine | Apache Airflow 3.1.8 |
| Transformatsioon | dbt Core 1.12.0-b1 |
| Andmehoidla | PostgreSQL (Neon) |
| Näidikulaud | Apache Superset 6.0.0 |
| Andmeallikas | TME API v2 |

## Andmeallikas

**TME** (`https://api.tme.eu`) — Transfer Multisort Elektronik elektroonikamüüja REST API v2.
Autentimine: OAuth 2.0 (`client_credentials`), Bearer token kehtib 5 minutit.
Kredentsiaalid seadista `.env` failis (`API_PRIVATE_KEY`, `API_APP_SECRET`).

Kuus tarnijat, kellest igaühel oma elektroonikakategooria:

| Tarnija kood | Tarnija nimi       | TME otsingutermin  | Riik   |
|--------------|--------------------|--------------------|--------|
| RESIST       | ResistorTrade OÜ   | resistors          | Eesti  |
| CAPS         | CapacitorTech AS   | capacitors         | Läti   |
| MCU          | MicroChip SIA      | microcontrollers   | Leedu  |
| LED          | LightTech OÜ       | led diodes         | Soome  |
| SENSOR       | SensorPro OÜ       | sensors            | Eesti  |
| CONNECT      | ConnectWay AS      | connectors         | Läti   |

## Projekti struktuur

```
projektitoo-tarnijakataloogid/
├── airflow/dags/
│   └── tarnijakataloog_pipeline.py   — Airflow DAG
├── dbt_project/
│   ├── seeds/tarnijad.csv            — tarnijate viitetabel
│   └── models/
│       ├── staging/stg_tooted.sql          — puhastatud toorandmed
│       ├── intermediate/int_toode_hinnamuutus.sql  — hinnamuutused LAG()-ga
│       └── marts/
│           ├── mart_tarnija_kokkuvote.sql  — tarnijate koondstatistika
│           └── mart_kategooria_hinnajaotus.sql — hinnajaotus kategooriate kaupa
├── init/01_create_schemas.sql        — andmebaasi skeemid ja tabelid
├── superset/superset_config.py
├── compose.yml
├── Dockerfile.superset
└── .env.example
```

## Käivitamine

```bash
# 1. Kopeeri keskkonnamuutujad
cp .env.example .env

# 2. Käivita stack
docker compose up -d --build

# Oota ~2-3 minutit, kuni kõik teenused käivituvad

# 3. Ava Airflow UI
# http://localhost:8083  (airflow / airflow)
# → DAGs → tarnijakataloog_pipeline → ▶ Trigger DAG

# 4. Ava Superset
# http://localhost:8090  (admin / admin)
```

## DAG käitumine

Pipeline koosneb kolmest taskist:

1. **laadi_kataloogid** — hangib TME API-st OAuth2 Bearer tokeni, otsib
   iga kategooria tootesümbolid (`/products/search`), laeb hinnad ning
   laoseisu (`/products/data`, kuni 50 sümbolit korraga). Salvestab
   `staging.tooted_raw` tabelisse päevase hetktõmmisena.
   `ON CONFLICT DO NOTHING` tagab, et sama päeva andmeid ei dubleerita.

2. **dbt_run** — käivitab `dbt seed` (tarnijad.csv → andmebaasi) ja
   `dbt run` (kõik mudelid staging → intermediate → marts).

3. **dbt_test** — kontrollib andmekvaliteeti: mitte-null väljad,
   accepted_values tarnija koodide jaoks.

## Andmemudel

```
staging.tooted_raw          ← Airflow laadib siia
       ↓
staging.stg_tooted          ← dbt view: puhastus + loplik_hind + on_laost_otsas
       ↓
intermediate.int_toode_hinnamuutus  ← dbt view: LAG() hinnamuutus + tarnija JOIN
       ↓
marts.mart_tarnija_kokkuvote        ← dbt table: tarnijate võrdlus (Superset)
marts.mart_kategooria_hinnajaotus   ← dbt table: kategooriate trendid (Superset)
```

## Superset seadistamine (esmakordselt)

1. Lisa andmebaasiühendus: **Settings → Database Connections → + Database**
   - Engine: PostgreSQL
   - Neon ühendusstring `.env` failist (`DATABASE_URL`)

2. Lisa datasetid: **Datasets → + Dataset**
   - `marts.mart_tarnija_kokkuvote`
   - `marts.mart_kategooria_hinnajaotus`

3. Loo graafikud ja dashboard.

## Peatamine

```bash
docker compose down        # peatab konteinerid, säilitab andmed
docker compose down -v     # peatab ja kustutab kõik andmed (täielik lähtestamine)
```

## Meeskond

| Nimi | Roll |
|------|------|
| Merilin Paas-Loeza | [Roll] |
| Triin Bulõgina | [Roll] |
| Bernard Puström | [Roll] |
| Martin Aasna | [Roll] |