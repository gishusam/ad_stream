import pytest


def load_validator():
    try:
        from src.processing.data_quality import validate_pipeline_results
    except ModuleNotFoundError:
        pytest.fail("validate_pipeline_results is not implemented yet")

    return validate_pipeline_results


def healthy_results():
    silver = {
        "bronze": 1000,
        "silver": 990,
        "quarantine": 10,
        "written_silver": 990,
        "written_quarantine": 10,
    }

    gold = {
        "silver": 990,
        "quarantine": 10,
        "advertiser_daily": 4,
        "creative_daily": 12,
        "traffic_quality_daily": 1,
    }

    return silver, gold


def test_quality_gate_accepts_reconciled_pipeline():
    validate = load_validator()
    silver, gold = healthy_results()

    result = validate(silver, gold)

    assert result["status"] == "PASS"


def test_quality_gate_rejects_bronze_silver_reconciliation_failure():
    validate = load_validator()
    silver, gold = healthy_results()

    silver["bronze"] = 1001

    with pytest.raises(RuntimeError, match="Silver reconciliation"):
        validate(silver, gold)


def test_quality_gate_rejects_gold_input_mismatch():
    validate = load_validator()
    silver, gold = healthy_results()

    gold["silver"] = 989

    with pytest.raises(RuntimeError, match="Gold input reconciliation"):
        validate(silver, gold)


def test_quality_gate_rejects_empty_gold_output():
    validate = load_validator()
    silver, gold = healthy_results()

    gold["creative_daily"] = 0

    with pytest.raises(RuntimeError, match="Gold output"):
        validate(silver, gold)
