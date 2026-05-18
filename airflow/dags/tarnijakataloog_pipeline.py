"""
E-poe tarnijakataloogide pipeline — TME API → staging → dbt run → dbt test

Lihtne järjestikune DAG:
    laadi_kataloogid >> dbt_run >> dbt_test

Ajakava: iga päev (@daily), catchup=False — vahele jäänud käivitusi ei korrata.

Andmeallikas: TME (Transfer Multisort Elektronik) API v2 — elektroonikamüüja.
Autentimine: OAuth 2.0 client_credentials.
    1. POST /auth/token (Basic Auth: API_PRIVATE_KEY:API_APP_SECRET) → Bearer token
    2. GET /products/search?phrase=... → tootesümbolite nimekiri
    3. GET /products/data?symbols[]=...&scope[]=prices&scope[]=stock → hinnad ja laoseis

Esimesel käivitusel laaditakse kõigi kategooriate tooted täismahus.
Järgmistel käivitustel salvestatakse uus päevane hetktõmmis (snapshot).
ON CONFLICT DO NOTHING tagab, et sama päeva andmeid ei dubleerita.
"""

import base64
import os
import uuid
from datetime import date, datetime, timezone

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Tarnijate nimekiri: (tarnija_kood, tme_otsingutermin)
# Peab ühtima seeds/tarnijad.csv failiga.
TARNIJAD = [
    ("RESIST", "resistors"),
    ("CAPS", "capacitors"),
    ("MCU", "microcontrollers"),
    ("LED", "led diodes"),
    ("SENSOR", "sensors"),
    ("CONNECT", "connectors"),
]

TME_API_BASE = "https://api.tme.eu"
TME_RIIK = "EE"
TME_VALUUTA = "EUR"
TME_OTSINGU_PIIR = 100   # maksimaalne toodete arv otsingu kohta
TME_PAKETI_SUURUS = 50   # maksimaalne sümbolite arv /products/data päringu kohta


def _hangi_token() -> str:
    """
    Küsib TME OAuth2 access tokeni (kehtib 5 minutit).
    Kasutab HTTP Basic Auth: API_PRIVATE_KEY (kasutajanimi) + API_APP_SECRET (parool).
    """
    private_key = os.environ["API_PRIVATE_KEY"]
    app_secret = os.environ["API_APP_SECRET"]
    credentials = base64.b64encode(f"{private_key}:{app_secret}".encode()).decode()
    resp = requests.post(
        f"{TME_API_BASE}/auth/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _otsimise_symbolid(token: str, otsingutermin: str) -> dict:
    """
    Otsib TME API-st tooteid otsinguterminiga.
    Tagastab: {symbol: {nimi, tootja}} sõnastiku.
    """
    resp = requests.get(
        f"{TME_API_BASE}/products/search",
        headers={"Authorization": f"Bearer {token}"},
        params=[
            ("phrase", otsingutermin),
            ("scope[]", "products"),
            ("country", TME_RIIK),
            ("currency", TME_VALUUTA),
            ("limit", TME_OTSINGU_PIIR),
        ],
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json().get("data") or {}
    products = data.get("products") or {}
    elements = products.get("elements") or []

    symbol_info = {}
    for el in elements:
        sym = el.get("symbol")
        if sym:
            manufacturer = el.get("manufacturer") or {}
            symbol_info[sym] = {
                "nimi": el.get("description") or "",
                "tootja": manufacturer.get("name") or "",
            }
    return symbol_info


def _hangi_hinnad_laoseis(token: str, symbolid: list) -> dict:
    """
    Laeb TME-st hinnad ja laoseisu korraga (max 50 sümbolit päringu kohta).
    Tagastab: {symbol: {hind, valuuta, laoseis}} sõnastiku.
    """
    andmed = {}
    for i in range(0, len(symbolid), TME_PAKETI_SUURUS):
        pakett = symbolid[i : i + TME_PAKETI_SUURUS]
        params = [
            ("country", TME_RIIK),
            ("currency", TME_VALUUTA),
            ("scope[]", "prices"),
            ("scope[]", "stock"),
        ]
        for sym in pakett:
            params.append(("symbols[]", sym))
        resp = requests.get(
            f"{TME_API_BASE}/products/data",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        elements = (resp.json().get("data") or {}).get("elements") or []
        for el in elements:
            sym = el.get("symbol")
            if not sym:
                continue
            prices = el.get("prices") or {}
            price_list = prices.get("elements") or []
            hind = price_list[0].get("price") if price_list else None
            valuuta = prices.get("currency") or TME_VALUUTA
            laoseis = el.get("stock_quantity")
            andmed[sym] = {"hind": hind, "valuuta": valuuta, "laoseis": laoseis}
    return andmed


def laadi_kataloogid(**context):
    """
    Küsib TME API-st iga tarnija elektroonikakataloogid ja laadib
    staging.tooted_raw tabelisse päevase hetktõmmisena.

    Loogika:
    1. Hangi OAuth2 access token.
    2. Otsi iga kategooria tootesümbolid ja põhiinfo (/products/search).
    3. Laadi hinnad ja laoseis (/products/data, max 50 sümbolit korraga).
    4. Sisesta staging.tooted_raw — ON CONFLICT DO NOTHING välistab duplikaadid.
    5. Uuenda pipeline_runs olek 'success' või 'failed'.
    """
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    today = date.today()

    hook.run(
        """
        INSERT INTO staging.pipeline_runs
            (run_id, fetched_at, source_name, laetud_kuupaev, status)
        VALUES (%s, %s, 'tme', %s, 'running')
        """,
        parameters=(run_id, now, today),
    )

    try:
        token = _hangi_token()
        kokku_kirjeid = 0

        # Kogu kõik read enne andmebaasi kirjutamist — üks batch INSERT tarnija kohta
        for tarnija_kood, otsingutermin in TARNIJAD:
            # Samm 1: otsi tootesümbolid ja põhiinfo
            symbol_info = _otsimise_symbolid(token, otsingutermin)
            symbolid = list(symbol_info.keys())

            if not symbolid:
                continue

            # Samm 2: laadi hinnad ja laoseis
            hinnad_laoseis = _hangi_hinnad_laoseis(token, symbolid)

            # Samm 3: koosta batch parameetrid
            batch = []
            for sym in symbolid:
                info = symbol_info[sym]
                hl = hinnad_laoseis.get(sym, {})
                hind = hl.get("hind")
                if hind is None:
                    continue
                batch.append((
                    run_id, tarnija_kood, sym,
                    info["nimi"], info["tootja"],
                    hind, hl.get("valuuta", TME_VALUUTA),
                    hl.get("laoseis"), otsingutermin,
                    now, today,
                ))

            if not batch:
                continue

            # Samm 4: üks INSERT kõigi tarnija toodete jaoks (vähendab ühenduse koormust)
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
    dag_id="tarnijakataloog_pipeline",
    description="Laeb elektroonikakataloogid TME API-st ja käivitab dbt transformatsioonid",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tarnijakataloogid", "projektitoo"],
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
