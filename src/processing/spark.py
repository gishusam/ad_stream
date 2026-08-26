"""Spark configuration for AdStream storage backends."""

from collections.abc import Mapping
import os


ICEBERG_REQUIRED_ENV = (
    "SUPABASE_PROJECT_REF",
    "SUPABASE_CATALOG_TOKEN",
    "SUPABASE_ICEBERG_WAREHOUSE",
    "SUPABASE_S3_ACCESS_KEY",
    "SUPABASE_S3_SECRET_KEY",
    "SUPABASE_S3_REGION",
)


def get_iceberg_settings(
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Validate and return Supabase Iceberg connection settings."""
    env = os.environ if env is None else env

    missing = [
        name
        for name in ICEBERG_REQUIRED_ENV
        if not env.get(name)
    ]

    if missing:
        raise ValueError(
            "Missing required Supabase Iceberg environment variables: "
            + ", ".join(missing)
        )

    project_ref = env["SUPABASE_PROJECT_REF"]

    return {
        "project_ref": project_ref,
        "warehouse": env["SUPABASE_ICEBERG_WAREHOUSE"],
        "catalog_token": env["SUPABASE_CATALOG_TOKEN"],
        "access_key": env["SUPABASE_S3_ACCESS_KEY"],
        "secret_key": env["SUPABASE_S3_SECRET_KEY"],
        "region": env["SUPABASE_S3_REGION"],
        "catalog_uri": (
            f"https://{project_ref}.supabase.co/storage/v1/iceberg"
        ),
        "s3_endpoint": (
            f"https://{project_ref}.supabase.co/storage/v1/s3"
        ),
    }


ICEBERG_PACKAGES = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:"
    "iceberg-aws-bundle:1.6.1"
)


def get_storage_spark_config(
    backend: str,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return Spark configuration for a Bronze storage backend."""

    if backend == "delta":
        return {
            "spark.sql.extensions": (
                "io.delta.sql.DeltaSparkSessionExtension"
            ),
            "spark.sql.catalog.spark_catalog": (
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            ),
        }

    if backend == "iceberg":
        settings = get_iceberg_settings(env)

        return {
            "spark.jars.packages": ICEBERG_PACKAGES,
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.supabase": (
                "org.apache.iceberg.spark.SparkCatalog"
            ),
            "spark.sql.catalog.supabase.type": "rest",
            "spark.sql.catalog.supabase.uri": (
                settings["catalog_uri"]
            ),
            "spark.sql.catalog.supabase.warehouse": (
                settings["warehouse"]
            ),
            "spark.sql.catalog.supabase.token": (
                settings["catalog_token"]
            ),
            "spark.sql.catalog.supabase.s3.endpoint": (
                settings["s3_endpoint"]
            ),
            "spark.sql.catalog.supabase.s3.path-style-access": "true",
            "spark.sql.catalog.supabase.s3.access-key-id": (
                settings["access_key"]
            ),
            "spark.sql.catalog.supabase.s3.secret-access-key": (
                settings["secret_key"]
            ),
            "spark.sql.catalog.supabase.s3.remote-signing-enabled": (
                "false"
            ),
            "spark.sql.catalog.supabase.client.region": (
                settings["region"]
            ),
            "spark.executorEnv.AWS_REGION": settings["region"],
        }

    raise ValueError(
        f"Unknown storage backend {backend!r}; "
        "expected one of: delta, iceberg"
    )
