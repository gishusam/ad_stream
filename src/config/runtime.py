"""Validate AdStream runtime configuration before work begins."""

from collections.abc import Mapping

from src.processing.spark import get_iceberg_settings


class ConfigurationError(RuntimeError):
    """Raised when required AdStream configuration is invalid."""


def validate_serving_config(
    env: Mapping[str, str],
) -> dict:
    database_url = (
        env.get("SUPABASE_POSTGRES_URL", "").strip()
    )

    if not database_url:
        raise ConfigurationError(
            "Missing required serving configuration: "
            "SUPABASE_POSTGRES_URL"
        )

    return {
        "database_url": database_url,
    }


def validate_runtime_config(
    env: Mapping[str, str],
) -> dict:
    backend = (
        env.get(
            "ADSTREAM_STORAGE_BACKEND",
            "delta",
        )
        .strip()
        .lower()
    )

    if backend not in {"delta", "iceberg"}:
        raise ConfigurationError(
            "Invalid ADSTREAM_STORAGE_BACKEND "
            f"{backend!r}; expected one of: "
            "delta, iceberg"
        )

    result = {
        "storage_backend": backend,
    }

    if backend == "iceberg":
        try:
            iceberg = get_iceberg_settings(env)
        except (KeyError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(
                str(exc)
            ) from exc

        result["iceberg"] = iceberg

    return result
