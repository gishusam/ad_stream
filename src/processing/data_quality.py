"""Cross-layer data-quality checks for the AdStream medallion pipeline."""


def validate_pipeline_results(
    silver_result: dict[str, int],
    gold_result: dict[str, int],
) -> dict[str, str]:
    """Validate reconciliation between Silver and Gold pipeline outputs."""

    if silver_result["bronze"] != (
        silver_result["silver"] + silver_result["quarantine"]
    ):
        raise RuntimeError(
            "Silver reconciliation failed: "
            f"bronze={silver_result['bronze']}, "
            f"silver={silver_result['silver']}, "
            f"quarantine={silver_result['quarantine']}"
        )

    if silver_result["written_silver"] != silver_result["silver"]:
        raise RuntimeError(
            "Silver write reconciliation failed: "
            f"expected={silver_result['silver']}, "
            f"written={silver_result['written_silver']}"
        )

    if silver_result["written_quarantine"] != silver_result["quarantine"]:
        raise RuntimeError(
            "Quarantine write reconciliation failed: "
            f"expected={silver_result['quarantine']}, "
            f"written={silver_result['written_quarantine']}"
        )

    if (
        gold_result["silver"] != silver_result["silver"]
        or gold_result["quarantine"] != silver_result["quarantine"]
    ):
        raise RuntimeError(
            "Gold input reconciliation failed: "
            f"silver_expected={silver_result['silver']}, "
            f"silver_actual={gold_result['silver']}, "
            f"quarantine_expected={silver_result['quarantine']}, "
            f"quarantine_actual={gold_result['quarantine']}"
        )

    gold_outputs = (
        "advertiser_daily",
        "creative_daily",
        "traffic_quality_daily",
    )

    for table in gold_outputs:
        if gold_result[table] <= 0:
            raise RuntimeError(
                f"Gold output validation failed: {table}=0"
            )

    return {"status": "PASS"}
