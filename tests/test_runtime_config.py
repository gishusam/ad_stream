import pytest

from src.config.runtime import (
    ConfigurationError,
    validate_runtime_config,
    validate_serving_config,
)


ICEBERG_ENV = {
    "SUPABASE_PROJECT_REF": "project-ref",
    "SUPABASE_CATALOG_TOKEN": "catalog-token",
    "SUPABASE_ICEBERG_WAREHOUSE": "warehouse",
    "SUPABASE_S3_ACCESS_KEY": "access-key",
    "SUPABASE_S3_SECRET_KEY": "secret-key",
    "SUPABASE_S3_REGION": "eu-west-1",
}


def test_delta_runtime_requires_no_cloud_configuration():
    config = validate_runtime_config(
        {
            "ADSTREAM_STORAGE_BACKEND": "delta",
        }
    )

    assert config["storage_backend"] == "delta"


def test_default_runtime_backend_is_delta():
    config = validate_runtime_config({})

    assert config["storage_backend"] == "delta"


def test_invalid_storage_backend_is_rejected():
    with pytest.raises(ConfigurationError) as exc:
        validate_runtime_config(
            {
                "ADSTREAM_STORAGE_BACKEND": "parquet",
            }
        )

    assert "ADSTREAM_STORAGE_BACKEND" in str(exc.value)
    assert "delta" in str(exc.value)
    assert "iceberg" in str(exc.value)


def test_iceberg_runtime_requires_supabase_configuration():
    with pytest.raises(ConfigurationError) as exc:
        validate_runtime_config(
            {
                "ADSTREAM_STORAGE_BACKEND": "iceberg",
            }
        )

    message = str(exc.value)

    assert "SUPABASE_PROJECT_REF" in message
    assert "SUPABASE_CATALOG_TOKEN" in message
    assert "SUPABASE_ICEBERG_WAREHOUSE" in message
    assert "SUPABASE_S3_ACCESS_KEY" in message
    assert "SUPABASE_S3_SECRET_KEY" in message
    assert "SUPABASE_S3_REGION" in message


def test_iceberg_runtime_accepts_complete_configuration():
    env = {
        "ADSTREAM_STORAGE_BACKEND": "iceberg",
        **ICEBERG_ENV,
    }

    config = validate_runtime_config(env)

    assert config["storage_backend"] == "iceberg"
    assert config["iceberg"]["project_ref"] == "project-ref"
    assert config["iceberg"]["region"] == "eu-west-1"


def test_serving_requires_postgres_url():
    with pytest.raises(ConfigurationError) as exc:
        validate_serving_config({})

    assert "SUPABASE_POSTGRES_URL" in str(exc.value)


def test_serving_accepts_postgres_url():
    config = validate_serving_config(
        {
            "SUPABASE_POSTGRES_URL": (
                "postgresql://user:secret@example.test/db"
            )
        }
    )

    assert config["database_url"].startswith(
        "postgresql://"
    )


def test_configuration_errors_do_not_expose_secret_values():
    secret = "super-secret-value"

    env = {
        "ADSTREAM_STORAGE_BACKEND": "invalid",
        "SUPABASE_CATALOG_TOKEN": secret,
    }

    with pytest.raises(ConfigurationError) as exc:
        validate_runtime_config(env)

    assert secret not in str(exc.value)
