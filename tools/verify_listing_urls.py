#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Verifica que los avisos que la app va a mostrar sigan publicados.

    python tools/verify_listing_urls.py                 # verifica los activos
    python tools/verify_listing_urls.py --limit 500     # prueba corta
    python tools/verify_listing_urls.py --dry-run       # no escribe en S3

Escribe s3://<bucket>/gold/listing_url_health/health.parquet con
(url, url_status, url_checked_at, url_detail), que el ETL cruza contra Gold.

Por que mira el cuerpo y no el codigo HTTP
------------------------------------------
Se midieron 250 URLs de la tabla real. Los portales colombianos responden
200 OK para avisos ya retirados y devuelven una pagina de "no disponible".
El codigo HTTP practicamente nunca es 404, asi que filtrar por status no
elimina ni un enlace muerto. Lo que si discrimina es el texto del cuerpo y
su longitud: una ficha viva trae 1.100-2.300 caracteres de texto; una
retirada trae ~245 y la frase "Este inmueble no esta disponible".

Cuidado con el patron: '404' suelto NO sirve como marcador, hace match
contra precios y telefonos y marcaba como muerto el 20% de los avisos
recien vistos. Solo se usan frases inequivocas.
"""

import argparse
import concurrent.futures as futures
import datetime as dt
import io
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality.listing_lifecycle import (  # noqa: E402
    ACTIVE_WINDOW_DAYS, URL_MUERTA, URL_VIVA,
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

DEAD_MARKERS = re.compile(
    r"(este inmueble no est|este inmueble ya no|ya no est. disponible|"
    r"no se encuentra disponible|publicaci.n finalizada|"
    r"p.gina no encontrada|error 404|inmueble no disponible|"
    r"esta propiedad ya no|aviso finalizado|publicaci.n pausada)"
)

MIN_LIVE_BODY_CHARS = 400
DEAD_HTTP_CODES = {404, 410}

_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<script.*?</script>|<style.*?</style>", re.S | re.I)
_WS_RE = re.compile(r"\s+")

_SSL_CONTEXT = ssl.create_default_context()
_SSL_CONTEXT.check_hostname = False
_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


def page_text(html: str) -> str:
    text = _SCRIPT_RE.sub(" ", html)
    text = _TAG_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip().lower()


def check_url(url, timeout=20):
    """(url_status, detalle). Ante un error de red devuelve sin_verificar."""
    if not url or not str(url).startswith("http"):
        return "sin_verificar", "url_invalida"
    request = urllib.request.Request(str(url), headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_SSL_CONTEXT) as response:
            html = response.read(300_000).decode("utf-8", "ignore")
    except urllib.error.HTTPError as exc:
        if exc.code in DEAD_HTTP_CODES:
            return URL_MUERTA, f"http_{exc.code}"
        return "sin_verificar", f"http_{exc.code}"
    except Exception as exc:                      # timeouts, DNS, TLS
        return "sin_verificar", type(exc).__name__[:24]

    text = page_text(html)
    if DEAD_MARKERS.search(text):
        return URL_MUERTA, "marcador_retirado"
    if len(text) < MIN_LIVE_BODY_CHARS:
        return URL_MUERTA, "cuerpo_vacio"
    return URL_VIVA, "ok"


def load_credentials():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(repo_root, "aws_secrets.json")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "bucket_name": os.environ.get("S3_BUCKET_NAME", "bronce-scrap-date"),
    }


def s3_client(config):
    import boto3

    return boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key"],
        aws_secret_access_key=config["aws_secret_key"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def load_candidates(config, window_days, limit):
    """Avisos suficientemente recientes como para valer la verificacion."""
    import pandas as pd
    import pyarrow.dataset as ds
    import s3fs

    bucket = config.get("bucket_name", "bronce-scrap-date")
    filesystem = s3fs.S3FileSystem(
        key=config["aws_access_key"], secret=config["aws_secret_key"]
    )
    dataset = ds.dataset(
        f"{bucket}/gold/app_inmuebles_scored/", filesystem=filesystem, format="parquet"
    )
    frame = dataset.to_table(columns=["url", "fuente", "fecha_extraccion"]).to_pandas()

    frame["fecha_extraccion"] = pd.to_datetime(frame["fecha_extraccion"], errors="coerce")
    last_crawl = frame.groupby("fuente")["fecha_extraccion"].transform("max")
    frame["dias_sin_ver"] = (last_crawl - frame["fecha_extraccion"]).dt.days

    candidates = frame[frame["dias_sin_ver"] <= window_days]
    candidates = candidates.dropna(subset=["url"]).drop_duplicates(subset=["url"])
    if limit:
        candidates = candidates.head(limit)
    return candidates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-days", type=int, default=ACTIVE_WINDOW_DAYS)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    import pandas as pd

    config = load_credentials()
    print(f"Cargando avisos vistos en los ultimos {args.window_days} dias...")
    candidates = load_candidates(config, args.window_days, args.limit)
    print(f"  {len(candidates):,} URLs unicas por verificar")
    if candidates.empty:
        print("  nada que verificar.")
        return 0

    urls = candidates["url"].tolist()
    results = []
    done = 0
    with futures.ThreadPoolExecutor(args.workers) as pool:
        for url, (status, detail) in zip(urls, pool.map(check_url, urls)):
            results.append({"url": url, "url_status": status, "url_detail": detail})
            done += 1
            if done % 250 == 0:
                print(f"  {done:,}/{len(urls):,}")

    health = pd.DataFrame(results)
    health["url_checked_at"] = dt.datetime.utcnow()

    summary = health["url_status"].value_counts()
    print("\nResultado:")
    for status, count in summary.items():
        print(f"  {status:16s} {count:6,d}  ({count / len(health) * 100:5.1f}%)")

    if args.dry_run:
        print("\n--dry-run: no se escribio en S3.")
        return 0

    bucket = config.get("bucket_name", "bronce-scrap-date")
    key = "gold/listing_url_health/health.parquet"
    buffer = io.BytesIO()
    health.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client(config).put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"\nGuardado en s3://{bucket}/{key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
