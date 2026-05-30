"""
Element14 API tarnijakataloogi pipeline - laeb andmed Element14 API-st -> staging -> dbt run -> dbt test
DAG: dbt_seed -> laadi_kataloogid -> dbt_run -> dbt_test

Ajakava: @daily, catchup=False ja vahele jäänud käivitusi ei korrata.

Andmeallikas: Element14 / Farnell UK API Version 1.2
Autentimine: API key URLis (ELEMENT14_API_KEY)
GET /catalog/products?term=any:<MPN>&storeInfo.id=uk.farnell.com&callInfo.apiKey=...&resultsSettings.responseGroup=large
   → tooted koos hindade ja laoseisuga

MPN-id loetakse marts.tooted seed-tabelist (täidetud `dbt seed` poolt enne).
Iga MPN võib anda mitu varianti (eri tootjad, pakendid) — kõik salvestatakse.
Hind võetakse väikseima `from` koguse hinnatasandilt (vähim ostukogus).

Esimesel käivitusel laaditakse kõik MPN-id täismahus.
Järgmistel käivitustel salvestatakse uus päevane hetktõmmis (snapshot).
ON CONFLICT DO NOTHING tagab, et sama päeva andmeid ei dubleerita."""

import os
import time
import uuid
from datetime import date, datetime, timezone

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

E14_BASE_URL = "https://api.element14.com/catalog/products"
E14_VERSION = "1.2"
E14_FORMAAT = "json"
E14_VASTUSE_GRUPP = "large"
E14_POOD = "uk.farnell.com"
E14_VALUUTA = "GBP"
E14_OTSINGU_PIIR = 5    # maksimaalne variantide arv MPN-i kohta
RETRY_KATSEID = 5       # mitu korda HTTP päringut proovida transient vea puhul
FX_API_BASE = "https://api.frankfurter.app"
GBP_EUR_FALLBACK = 1.18  # GBP→EUR fallback kurss kui Frankfurter API ei vasta
# PAKETI_SUURUS pole, sest E14 tagastab hinnad ja laoseisu ühe päringuga.


def _paringus_retry(fn, *args, **kwargs):
    """Käivita HTTP funktsioon retry-loogikaga SSL/Connection/Timeout vigade puhul.
    Kasutab exponential backoff (1s, 2s, 4s, 8s, 16s; ülempiir 30s)."""
    last_err = None
    for katse in range(RETRY_KATSEID):
        try:
            return fn(*args, **kwargs)
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_err = e
            time.sleep(min(30, 2 ** katse))
    raise last_err


def _hangi_gbp_eur_kurss() -> float:
    """
    Tagastab GBP→EUR valuutakursi Frankfurter API-st (ECB põhjal).
    API tõrke korral kasutame GBP_EUR_FALLBACK constantist.
    """
    try:
        r = _paringus_retry(
            requests.get,
            f"{FX_API_BASE}/latest",
            params={"from": "GBP", "to": "EUR"},
            timeout=15,
        )
        r.raise_for_status()
        return float(r.json()["rates"]["EUR"])
    except Exception:
        return GBP_EUR_FALLBACK


def _hangi_tooted(otsingutermin: str) -> list:
    """
    Küsib Element14 APIst tooteid otsinguterminiga, laeb hinnad ja laoseisu
    ning filtreerib vastused täpse MPN-i järgi.
    Tagastab sku, nimi, tootja, hind, valuuta, min_kogus, laoseis.
    """
    api_key = os.environ["ELEMENT14_API_KEY"]
    resp = _paringus_retry(
        requests.get,
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
    mu = otsingutermin.upper()
    tulem = []
    for toode in products:
        sku = toode.get("sku")
        if not sku:
            continue
        # Filtreeri ainult täpse MPN-iga matchivad variandid
        toote_mpn = (toode.get("translatedManufacturerPartNumber") or "").upper()
        if toote_mpn != mu:
            continue
        nimi = toode.get("displayName") or ""
        tootja = toode.get("brandName") or ""
        prices = toode.get("prices") or []
        # Vali väikseima `from` koguse hinnatasand (vähim ostukogus)
        tier = min(prices, key=lambda p: p.get("from") or 999999) if prices else None
        hind = tier.get("cost") if tier else None
        min_kogus = tier.get("from") if tier else None
        if hind is None:
            continue
        stock = toode.get("stock") or {}
        laoseis = stock.get("level")

        tulem.append({
            "sku": sku,
            "nimi": nimi,
            "tootja": tootja,
            "hind": hind,
            "min_kogus": min_kogus,
            "laoseis": laoseis,
            "valuuta": E14_VALUUTA,
        })
    return tulem


def _loe_mpn_seed(hook: PostgresHook) -> list:
    """Loe MPN-id ja kategooriad marts.tooted seed-tabelist (dbt seed täidab)."""
    return hook.get_records(
        "SELECT mpn, kategooria FROM marts.tooted ORDER BY kategooria, mpn"
    )


def laadi_kataloogid(**context):
    """
    Küsib Element14 APIst iga MPN-i kohta variandid + hinnad ja laoseisu ning
    laadib staging.tooted_raw tabelisse päevase hetktõmmisena.
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
        mpn_kategooriad = _loe_mpn_seed(hook)
        if not mpn_kategooriad:
            raise RuntimeError("marts.tooted on tühi — käivita 'dbt seed' enne")

        gbp_eur_kurss = _hangi_gbp_eur_kurss()
        kokku_kirjeid = 0

        for mpn, kategooria in mpn_kategooriad:
            # Hangi variandid (otsing + hinnad + laoseis ühe päringuga)
            tooted = _hangi_tooted(mpn)

            if not tooted:
                continue

            # Koosta batch parameetrid INSERT-i jaoks (üks rida iga variandi kohta)
            batch = []
            for toode in tooted:
                batch.append((
                    run_id, "FARNELL", toode["sku"], mpn,
                    toode["nimi"], toode["tootja"],
                    toode["hind"], toode["valuuta"], gbp_eur_kurss,
                    toode["min_kogus"], toode["laoseis"],
                    kategooria,
                    now, today,
                ))

            if not batch:
                continue

            # Üks INSERT MPN-i kõigi variantide jaoks — uus ühendus iga batch-i jaoks
            conn = None
            try:
                conn = hook.get_conn()
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO staging.tooted_raw
                            (run_id, tarnija_kood, sumbol, mpn, nimi, tootja, hind,
                             valuuta, valuuta_eur_kurss, min_kogus, laoseis, kategooria,
                             laetud_kell, laetud_kuupaev)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tarnija_kood, sumbol, laetud_kuupaev) DO NOTHING
                        """,
                        batch,
                    )
                conn.commit()
            finally:
                if conn:
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
    description="Laeb elektroonikakataloogid Element14 API-st MPN-i kaupa ja käivitab dbt transformatsioonid",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tarnijakataloogid", "projektitoo", "element14"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt seed --full-refresh --profiles-dir ."
        ),
    )

    lae_andmed = PythonOperator(
        task_id="laadi_kataloogid",
        python_callable=laadi_kataloogid,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
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

    dbt_seed >> lae_andmed >> dbt_run >> dbt_test
