"""
E-poe tarnijakataloogide pipeline — TME API → staging → dbt run → dbt test

Lihtne järjestikune DAG:
    dbt_seed >> laadi_kataloogid >> dbt_run >> dbt_test

Ajakava: iga päev (@daily), catchup=False — vahele jäänud käivitusi ei korrata.

Andmeallikas: TME (Transfer Multisort Elektronik) API v2 — elektroonikamüüja.
Autentimine: OAuth 2.0 client_credentials.
    1. POST /auth/token (Basic Auth: API_PRIVATE_KEY:API_APP_SECRET) → Bearer token
    2. GET /products/search?phrase=<MPN> → variantide leidmine MPN-i järgi
    3. GET /products/data?symbols[]=...&scope[]=prices&scope[]=stock → hinnad ja laoseis

MPN-id loetakse marts.tooted seed-tabelist (täidetud `dbt seed` poolt enne).
Iga MPN võib anda mitu varianti (eri tootjad, pakendid) — kõik salvestatakse.
Hind võetakse väikseima `amount`-iga tasandilt (qty=1 või vähim ostukogus).

Esimesel käivitusel laaditakse kõik MPN-id täismahus.
Järgmistel käivitustel salvestatakse uus päevane hetktõmmis (snapshot).
ON CONFLICT DO NOTHING tagab, et sama päeva andmeid ei dubleerita.
"""

import base64
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _db_insert_with_retry(hook, batch, max_retries=3):
    """Sisesta andmed andmebaasi retry-loogikaga."""
    for attempt in range(max_retries):
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
            return len(batch)
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
                conn.close()
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Andmebaasi sisestamine ebaõnnestus (katse {attempt + 1}/{max_retries}). Proovi uuesti {wait_time}s pärast: {e}")
                time.sleep(wait_time)
            else:
                raise

TME_API_BASE = "https://api.tme.eu"
TME_RIIK = "EE"
TME_VALUUTA = "EUR"
TME_OTSINGU_PIIR = 100   # maksimaalne toodete arv otsingu kohta
TME_PAKETI_SUURUS = 25   # maksimaalne sümbolite arv /products/data päringu kohta
TOKEN_PARINGUTE_LIMIIT = 40  # uuenda token enne 5-min aegumist (~50 päringu juures)
RETRY_KATSEID = 5            # mitu korda HTTP päringut proovida transient vea puhul
TME_PARINGU_PAUS = 0.6       # väike paus iga HTTP päringu ees, et TME rate limitit vähendada


def _paringus_retry(fn, *args, **kwargs):
    """Käivita HTTP funktsioon retry-loogikaga SSL/Connection/Timeout vigade puhul.
    Kasutab exponential backoff (1s, 2s, 4s, 8s, 16s; ülempiir 30s)."""
    last_err = None
    for katse in range(RETRY_KATSEID):
        try:
            time.sleep(TME_PARINGU_PAUS)
            return fn(*args, **kwargs)
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_err = e
            time.sleep(min(30, 2 ** katse))
    raise last_err


def _hangi_token() -> str:
    """
    Küsib TME OAuth2 access tokeni (kehtib 5 minutit).
    Kasutab HTTP Basic Auth: API_PRIVATE_KEY (kasutajanimi) + API_APP_SECRET (parool).
    """
    private_key = os.environ["API_PRIVATE_KEY"]
    app_secret = os.environ["API_APP_SECRET"]
    credentials = base64.b64encode(f"{private_key}:{app_secret}".encode()).decode()
    resp = _paringus_retry(
        requests.post,
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
    resp = _paringus_retry(
        requests.get,
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
    Laeb TME-st hinnad ja laoseisu korraga (max 25 sümbolit päringu kohta).
    Hind võetakse väikseima `amount`-iga tasandilt (qty=1 või vähim ostukogus).
    Tagastab: {symbol: {hind, valuuta, min_kogus, laoseis}} sõnastiku.
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
        resp = _paringus_retry(
            requests.get,
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
            # Vali väikseima amount-iga hinnatasand (qty=1 või vähim ostukogus)
            tier = min(price_list, key=lambda p: p.get("amount") or 999999) if price_list else None
            hind = tier.get("price") if tier else None
            min_kogus = tier.get("amount") if tier else None
            valuuta = prices.get("currency") or TME_VALUUTA
            laoseis = el.get("stock_quantity")
            andmed[sym] = {
                "hind": hind,
                "valuuta": valuuta,
                "min_kogus": min_kogus,
                "laoseis": laoseis,
            }
    return andmed


def _loe_mpn_seed(hook: PostgresHook) -> list:
    """Loe MPN-id ja kategooriad marts.tooted seed-tabelist (dbt seed täidab)."""
    return hook.get_records(
        "SELECT mpn, kategooria FROM marts.tooted ORDER BY kategooria, mpn"
    )


def _process_mpn_tme(mpn: str, kategooria: str, run_id: str, now, today) -> list:
    """Töötle üks MPN: hangi sümbolid ja hinnad. Käivitatakse paralleelselt."""
    token = _hangi_token()
    read = []

    symbol_info = _otsimise_symbolid(token, mpn)

    mu = mpn.upper()
    symbol_info = {
        s: i for s, i in symbol_info.items()
        if s.upper() == mu
    }
    symbolid = list(symbol_info.keys())

    if not symbolid:
        return read

    hinnad_laoseis = _hangi_hinnad_laoseis(token, symbolid)

    for sym in symbolid:
        info = symbol_info[sym]
        hl = hinnad_laoseis.get(sym, {})
        hind = hl.get("hind")
        if hind is None:
            continue
        read.append((
            run_id, "TME", sym, mpn,
            info["nimi"], info["tootja"],
            hind, hl.get("valuuta", TME_VALUUTA), 1.0,
            hl.get("min_kogus"), hl.get("laoseis"),
            kategooria,
            now, today,
        ))

    return read


def laadi_kataloogid(**context):
    """
    Küsib TME API-st iga MPN-i kohta variandid + hinnad ja laoseisu ning
    laadib staging.tooted_raw tabelisse päevase hetktõmmisena.

    Loogika:
    1. Avatud ühendus: loe MPN-id seedist, salvesta käivitus.
    2. Sulge ühendus, hangi kõik andmed API-st (ilma ühenduseta, et vältida timeout-i).
    3. Avatud ühendus: sisesta kõik andmed, uuenda status.
    """
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    today = date.today()

    # Samm 1: Avatud ühendus — salvesta käivitus, loe seed
    try:
        hook.run(
            """
            INSERT INTO staging.pipeline_runs
                (run_id, fetched_at, source_name, laetud_kuupaev, status)
            VALUES (%s, %s, 'tme', %s, 'running')
            """,
            parameters=(run_id, now, today),
        )
        mpn_kategooriad = _loe_mpn_seed(hook)
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

    if not mpn_kategooriad:
        raise RuntimeError("marts.tooted on tühi — käivita 'dbt seed' enne")

    # Samm 2: API-päringud paralleelselt ilma andmebaasi ühenduseta
    koik_read = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_process_mpn_tme, mpn, kategooria, run_id, now, today): (mpn, kategooria)
                   for mpn, kategooria in mpn_kategooriad}

        for future in as_completed(futures):
            mpn, kategooria = futures[future]
            try:
                read = future.result()
                koik_read.extend(read)
            except Exception as e:
                print(f"API päringu viga MPN-i {mpn} jaoks: {e}")

    # Samm 3: Avatud ühendus — sisesta kõik read ja uuenda status
    try:
        kokku_kirjeid = 0
        if koik_read:
            kokku_kirjeid = _db_insert_with_retry(hook, koik_read)

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
    dag_id="tme_tarnijakataloog_pipeline",
    description="Laeb elektroonikakataloogid TME API-st MPN-i kaupa ja käivitab dbt transformatsioonid",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tarnijakataloogid", "projektitoo"],
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
        execution_timeout=timedelta(minutes=20),
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
