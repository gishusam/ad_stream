from pathlib import Path


DASHBOARD_PATH = Path("dashboard/adstream_dashboard.html")


def dashboard_source() -> str:
    return DASHBOARD_PATH.read_text()


def test_dashboard_uses_query_api_v1():
    source = dashboard_source()

    assert "/api/v1/advertisers/daily" in source
    assert "/api/v1/creatives/daily" in source
    assert "/api/v1/traffic-quality/daily" in source


def test_dashboard_does_not_use_legacy_api_endpoints():
    source = dashboard_source()

    legacy_endpoints = [
        "/api/summary",
        "/api/revenue",
        "/api/fraud",
        "/api/device_split",
        "/api/country_revenue",
        "/api/content",
    ]

    for endpoint in legacy_endpoints:
        assert endpoint not in source


def test_dashboard_does_not_present_unsupported_fraud_metrics():
    source = dashboard_source()

    unsupported_metrics = [
        "Fraud Caught",
        "Revenue Protected",
        "Fraud Prevention",
        "Gold · Fraud",
    ]

    for metric in unsupported_metrics:
        assert metric not in source


def test_dashboard_presents_gold_v2_metrics():
    source = dashboard_source()

    expected_metrics = [
        "Total Spend",
        "Impressions",
        "Auction Savings",
        "Warning",
        "Traffic Quality",
        "Creative Performance",
    ]

    for metric in expected_metrics:
        assert metric in source


def test_dashboard_identifies_current_pipeline_layers():
    source = dashboard_source()

    for layer in ["Bronze", "Silver", "Gold", "Serving", "API"]:
        assert layer in source
