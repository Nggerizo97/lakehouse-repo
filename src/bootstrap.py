# -*- coding: utf-8 -*-
"""Arranque comun de los notebooks de Databricks.

Cada notebook repetia el mismo bloque de ~40 lineas para armar sys.path,
buscar credenciales y cargar modulos del repo. Estaban copiados cinco veces
y ya habian divergido entre si (02_Silver traia ademas una copia embebida
del catalogo geografico que quedo desactualizada frente a src/geo/).

Uso en un notebook:

    from src.bootstrap import init
    ctx = init()
    spark_df = ctx.read_delta("silver/master_inmuebles")
"""

import json
import os
import sys

DEFAULT_BUCKET = "bronce-scrap-date"
SECRET_SCOPE = "aws"


def _repo_candidates():
    """Rutas donde puede vivir el repo, segun el runtime."""
    candidates = []

    # Databricks: la ruta del notebook en el Workspace
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()  # noqa: F821
            .notebook().getContext().notebookPath().get()
        )
        candidates.append("/Workspace" + str(notebook_path).rsplit("/", 2)[0])
    except Exception:
        pass

    # VS Code / Jupyter local
    vscode_file = globals().get("__vsc_ipynb_file__", "")
    if vscode_file:
        notebook_dir = os.path.dirname(os.path.abspath(vscode_file))
        candidates.append(notebook_dir)
        parent = os.path.dirname(notebook_dir)
        if parent and parent != notebook_dir:
            candidates.append(parent)

    # Ejecucion como modulo del repo
    candidates.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    candidates.append(os.getcwd())
    return [path for path in candidates if path]


def _load_config(candidates):
    """Databricks Secrets en produccion, aws_secrets.json en desarrollo."""
    try:
        config = {
            "aws_access_key": dbutils.secrets.get(scope=SECRET_SCOPE, key="access_key"),  # noqa: F821
            "aws_secret_key": dbutils.secrets.get(scope=SECRET_SCOPE, key="secret_key"),  # noqa: F821
            "bucket_name": DEFAULT_BUCKET,
            "credentials_source": "databricks_secrets",
        }
        return config
    except Exception:
        pass

    for directory in candidates:
        path = os.path.join(directory, "aws_secrets.json")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as handle:
                config = json.load(handle)
            config.setdefault("bucket_name", DEFAULT_BUCKET)
            config["credentials_source"] = f"archivo local ({path})"
            return config

    env_key = os.environ.get("AWS_ACCESS_KEY_ID")
    if env_key:
        return {
            "aws_access_key": env_key,
            "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "bucket_name": os.environ.get("S3_BUCKET_NAME", DEFAULT_BUCKET),
            "credentials_source": "variables de entorno",
        }

    raise RuntimeError(
        "Credenciales AWS no disponibles. Configura el scope 'aws' en "
        "Databricks Secrets, o coloca aws_secrets.json en la raiz del repo."
    )


class PipelineContext:
    """Credenciales, rutas y lectores/escritores de S3 en un solo objeto."""

    def __init__(self, spark=None, verbose=True):
        self.candidates = _repo_candidates()
        for path in self.candidates:
            if path not in sys.path:
                sys.path.insert(0, path)

        self.config = _load_config(self.candidates)
        self.bucket = self.config.get("bucket_name", DEFAULT_BUCKET)
        self.s3_options = {
            "fs.s3a.access.key": self.config["aws_access_key"],
            "fs.s3a.secret.key": self.config["aws_secret_key"],
            "fs.s3a.endpoint": "s3.amazonaws.com",
        }
        self._spark = spark
        if verbose:
            print(f"Credenciales: {self.config['credentials_source']}")
            print(f"Bucket: {self.bucket}")

    # ── acceso a Spark sin importarlo a nivel de modulo ───────────
    @property
    def spark(self):
        if self._spark is None:
            from pyspark.sql import SparkSession

            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def path(self, relative):
        return f"s3a://{self.bucket}/{relative.strip('/')}/"

    # ── boto3 ─────────────────────────────────────────────────────
    def boto3_client(self, service="s3"):
        import boto3

        return boto3.client(
            service,
            aws_access_key_id=self.config["aws_access_key"],
            aws_secret_access_key=self.config["aws_secret_key"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )

    def exists(self, relative):
        """True si el prefijo tiene al menos un objeto."""
        client = self.boto3_client()
        prefix = relative.strip("/") + "/"
        response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0

    # ── lectura y escritura ───────────────────────────────────────
    def read(self, relative, fmt="delta"):
        reader = self.spark.read.format(fmt)
        for key, value in self.s3_options.items():
            reader = reader.option(key, value)
        return reader.load(self.path(relative))

    def read_first_available(self, relatives, fmt="delta"):
        """Primera ruta que exista, con su nombre. Evita el patron
        try/except anidado que se repetia en Gold."""
        last_error = None
        for relative in relatives:
            try:
                return self.read(relative, fmt), relative
            except Exception as exc:
                last_error = exc
        raise last_error

    def write(self, df, relative, fmt="delta", mode="overwrite", coalesce=1, label=None):
        frame = df.coalesce(coalesce) if coalesce else df
        writer = frame.write.format(fmt).mode(mode)
        if fmt == "delta":
            writer = writer.option("overwriteSchema", "true")
        for key, value in self.s3_options.items():
            writer = writer.option(key, value)
        writer.save(self.path(relative))
        print(f"  guardado {label or relative}: {self.path(relative)}")

    def clear_prefix(self, relative):
        """Vacia un prefijo. Necesario al cambiar de parquet a delta en la
        misma ruta, y para que overwrite no deje archivos huerfanos."""
        client = self.boto3_client()
        prefix = relative.strip("/") + "/"
        paginator = client.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            for start in range(0, len(objects), 1000):
                batch = objects[start:start + 1000]
                if batch:
                    client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
                    deleted += len(batch)
        if deleted:
            print(f"  limpiados {deleted} objetos de {prefix}")
        return deleted


def init(spark=None, verbose=True) -> PipelineContext:
    return PipelineContext(spark=spark, verbose=verbose)
