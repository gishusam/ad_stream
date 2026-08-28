from pathlib import Path


DASHBOARD = Path(
    "dashboard/adstream_dashboard.html"
)


def source():
    return DASHBOARD.read_text()


def test_dashboard_has_sidebar_navigation():
    text = source()

    assert "Analytics" in text
    assert "Pipeline Health" in text


def test_dashboard_preserves_analytics_view():
    text = source()

    for metric in [
        "Total Spend",
        "Impressions",
        "Auction Savings",
        "Warning Events",
        "Traffic Quality",
        "Creative Performance",
    ]:
        assert metric in text


def test_dashboard_uses_pipeline_health_api():
    text = source()

    assert "/api/v1/pipeline-health" in text


def test_dashboard_has_pipeline_health_view():
    text = source()

    assert 'id="pipeline-health-view"' in text
    assert 'id="analytics-view"' in text


def test_dashboard_has_system_health_indicators():
    text = source()

    assert 'id="sidebar-api-status"' in text
    assert 'id="sidebar-db-status"' in text


def test_dashboard_has_latest_run_fields():
    text = source()

    assert 'id="pipeline-run-id"' in text
    assert 'id="pipeline-run-status"' in text
    assert 'id="pipeline-duration"' in text


def test_dashboard_has_stage_health_container():
    text = source()

    assert 'id="pipeline-stages"' in text


def test_dashboard_has_recent_runs_container():
    text = source()

    assert 'id="pipeline-recent-runs"' in text


def test_dashboard_handles_empty_pipeline_history():
    text = source()

    assert "No pipeline runs recorded yet." in text


def test_dashboard_keeps_current_query_endpoints():
    text = source()

    for endpoint in [
        "/api/v1/advertisers/daily",
        "/api/v1/creatives/daily",
        "/api/v1/traffic-quality/daily",
    ]:
        assert endpoint in text
