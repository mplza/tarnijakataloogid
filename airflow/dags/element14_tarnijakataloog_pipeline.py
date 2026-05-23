"""
Element14 API tarnijakataloogi pipeline - laeb andmed Element14 API-st -> staging -> dbt run -> dbt test
DAG: laadi_kataloogid -> dbt_run -> dbt_test

Ajakava: @daily, catchup=False ja vahele jäänud käivitusi ei korrata.

Andmeallikas: Element14 / Farnell UK API Version 1.2
Autentimine: API key URLis (ELEMENT14_API_KEY)
GET /catalog/products?term=any:...&storeInfo.id=uk.farnell.com&callInfo.apiKey=...&resultsSettings.responseGroup=large
   → tooted koos hindade ja laoseisuga

Esimesel käivitusel laaditakse kõigi kategooriate tooted täismahus.
Järgmistel käivitustel salvestatakse uus päevane hetktõmmis (snapshot).
ON CONFLICT DO NOTHING tagab, et sama päeva andmeid ei dubleerita."""

import os
import uuid
from datetime import date, datetime, timezone

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Tarnijate nimekiri: (tarnija_kood, otsingutermin)
# Peaks ühtima seeds/tarnijad.csv failiga.
TARNIJAD = [
    ("E14_RESIST", "resistors"),
    ("E14_CAPS", "capacitors"),
    ("E14_MCU", "microcontrollers"),
    ("E14_LED", "led diodes"),
    ("E14_SENSOR", "sensors"),
    ("E14_CONNECT", "connectors"),
]

E14_BASE_URL = "https://api.element14.com/catalog/products"
E14_VERSION = "1.2"
E14_FORMAAT = "json"
E14_VASTUSE_GRUPP = "large"
E14_POOD = "uk.farnell.com"
E14_VALUUTA = "GBP"
E14_OTSINGU_PIIR = 100   # maksimaalne toodete arv otsingu kohta
# PAKETI_SUURUS pole, sest E14 tagastab hinnad ja laoseisu ühe päringuga.


def _hangi_tooted(otsingutermin: str) -> list:
    """
    Küsib Element14 APIst tooteid, mille nimetus sisaldab otsinguterminit
    ning laeb hinnad ja laoseisu. Tagastab sku, nimi, tootja, hind, valuuta, laoseis.
    """
    api_key = os.environ["ELEMENT14_API_KEY"]
    resp = requests.get(
        E14_BASE_URL,
        params=[
            ("term", f"any:{otsingutermin}"),
            ("callInfo.apiKey", api_key),
            ("versionNumber", E14_VERSION),
            ("callInfo.responseDataFormat", E14_FORMAAT),
            ("resultsSettings.responseGroup", E14_VASTUSE_GRUPP),
            ("resultsSettings.offset", 0),
            ("storeInfo.id", E14_POOD),
            ("resultsSettings.numberOfResults", E14_OTSINGU_PIIR),
        ],
        timeout=30,
    )

    resp.raise_for_status()
    data = resp.json().get("keywordSearchReturn") or {}
    products = data.get("products") or []
    tulem = []
    for toode in products:
        sku = toode.get("sku")
        if not sku:
            continue
        nimi = toode.get("displayName") or ""
        tootja = toode.get("brandName") or ""
        prices = toode.get("prices") or []
        hind = prices[0].get("cost") if prices else None
        if hind is None:
            continue
        stock = toode.get("stock") or {}
        laoseis = stock.get("level")

        tulem.append({
            "sku": sku,
            "nimi": nimi,
            "tootja": tootja,
            "hind": hind,
            "laoseis": laoseis,
            "valuuta": E14_VALUUTA,
        })
    return tulem


def laadi_kataloogid(**context):
    """
    Küsib Element14 APIst iga tarnija elektroonikakataloogid ja laadib
    staging.tooted_raw tabelisse päevase hetktõmmisena.
    """
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    today = date.today()
    hook.run(
        """
        INSERT INTO staging.pipeline_runs
            (run_id, fetched_at, source_name, laetud_kuupaev, status)
        VALUES (%s, %s, 'element14', %s, 'running')
        """,
        parameters=(run_id, now, today),
    )
    try:
        kokku_kirjeid = 0

        for tarnija_kood, otsingutermin in TARNIJAD:
            # Hangi tooted (otsing + hinnad + laoseis ühe päringuga)
            tooted = _hangi_tooted(otsingutermin)

            if not tooted:
                continue

            # Koosta batch parameetrid INSERT-i jaoks
            batch = []
            for toode in tooted:
                batch.append((
                    run_id, tarnija_kood, toode["sku"],
                    toode["nimi"], toode["tootja"],
                    toode["hind"], toode["valuuta"],
                    toode["laoseis"], otsingutermin,
                    now, today,
                ))

            if not batch:
                continue

            # Üks INSERT kõigi tarnija toodete jaoks
            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO staging.tooted_raw
                            (run_id, tarnija_kood, sumbol, nimi, tootja, hind,
                             valuuta, laoseis, kategooria, laetud_kell, laetud_kuupaev)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tarnija_kood, sumbol, laetud_kuupaev) DO NOTHING
                        """,
                        batch,
                    )
                conn.commit()
            finally:
                conn.close()

            kokku_kirjeid += len(batch)

        
        hook.run(
            """
            UPDATE staging.pipeline_runs
            SET status = 'success', kirjete_arv = %s
            WHERE run_id = %s
            """,
            parameters=(kokku_kirjeid, run_id),
        )

    except Exception as exc:
        hook.run(
            """
            UPDATE staging.pipeline_runs
            SET status = 'failed', message = %s
            WHERE run_id = %s
            """,
            parameters=(str(exc)[:500], run_id),
        )
        raise


with DAG(
    dag_id="element14_tarnijakataloog_pipeline",
    description="Laeb elektroonikakataloogid Element14 API-st ja käivitab dbt transformatsioonid",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tarnijakataloogid", "projektitoo", "element14"],
) as dag:

    lae_andmed = PythonOperator(
        task_id="laadi_kataloogid",
        python_callable=laadi_kataloogid,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt seed --profiles-dir . && "
            "dbt run --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir ."
        ),
    )

    lae_andmed >> dbt_run >> dbt_test
