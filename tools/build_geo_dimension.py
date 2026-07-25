#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Materializa la dimension geografica DIVIPOLA en S3 y en disco local.

    python tools/build_geo_dimension.py            # construye y sube a S3
    python tools/build_geo_dimension.py --dry-run  # solo local, no sube

Escribe en s3://<bucket>/reference/geo/<tabla>.parquet, que es lo que leen
los notebooks de Databricks y la app de Streamlit. Es una tabla de
referencia estatica: se reconstruye solo cuando el DANE publica una version
nueva de DIVIPOLA, no en cada corrida del ETL.
"""

import argparse
import io
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.geo.divipola import build_all  # noqa: E402

S3_PREFIX = "reference/geo"
LOCAL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reference", "geo"
)


def load_credentials():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secrets_path = os.path.join(repo_root, "aws_secrets.json")
    if os.path.isfile(secrets_path):
        with open(secrets_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "bucket_name": os.environ.get("S3_BUCKET_NAME", "bronce-scrap-date"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=None, help="Ruta al libro DIVIPOLA")
    parser.add_argument("--dry-run", action="store_true", help="No subir a S3")
    args = parser.parse_args()

    print("Construyendo dimension geografica desde DIVIPOLA...")
    dims = build_all(args.source)

    os.makedirs(LOCAL_DIR, exist_ok=True)
    for name, frame in dims.items():
        local_path = os.path.join(LOCAL_DIR, f"{name}.parquet")
        frame.to_parquet(local_path, index=False)
        print(f"  {name:22s} {len(frame):6,d} filas -> {local_path}")

    if args.dry_run:
        print("\n--dry-run: no se subio nada a S3.")
        return 0

    import boto3

    config = load_credentials()
    bucket = config.get("bucket_name", "bronce-scrap-date")
    client = boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key"],
        aws_secret_access_key=config["aws_secret_key"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    print(f"\nSubiendo a s3://{bucket}/{S3_PREFIX}/")
    for name, frame in dims.items():
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False)
        buffer.seek(0)
        key = f"{S3_PREFIX}/{name}.parquet"
        client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        print(f"  s3://{bucket}/{key}  ({buffer.getbuffer().nbytes / 1024:.0f} KB)")

    print("\nListo. Los notebooks pueden leerla con:")
    print(f"  spark.read.parquet('s3a://{bucket}/{S3_PREFIX}/dim_municipio.parquet')")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
