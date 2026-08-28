import pytest


def load_pipeline():
    try:
        from src.serving.pipeline import ServingPipeline
    except ModuleNotFoundError:
        pytest.fail("ServingPipeline is not implemented yet")

    return ServingPipeline


def test_serving_pipeline_requires_postgres_url(monkeypatch):
    ServingPipeline = load_pipeline()

    monkeypatch.delenv("SUPABASE_POSTGRES_URL", raising=False)

    with pytest.raises(
        RuntimeError,
        match="SUPABASE_POSTGRES_URL",
    ):
        ServingPipeline()


def test_serving_pipeline_runs_refresh_job(monkeypatch):
    from src.serving import pipeline as pipeline_module

    calls = {}

    class FakeRefreshJob:
        def __init__(self, gold_backend, serving_store):
            calls["gold_backend"] = gold_backend
            calls["serving_store"] = serving_store

        def run(self):
            return {
                "advertiser_daily": 1,
                "creative_daily": 1,
                "traffic_quality_daily": 1,
            }

    monkeypatch.setattr(
        pipeline_module,
        "ServingRefreshJob",
        FakeRefreshJob,
        raising=False,
    )
    monkeypatch.setattr(
        pipeline_module,
        "get_spark",
        lambda backend: "spark",
        raising=False,
    )
    monkeypatch.setattr(
        pipeline_module,
        "build_gold_backend",
        lambda spark, backend: "gold",
        raising=False,
    )
    monkeypatch.setattr(
        pipeline_module,
        "PostgresServingStore",
        lambda database_url, schema: "store",
        raising=False,
    )

    pipeline = pipeline_module.ServingPipeline(
        database_url="postgresql://example",
    )

    result = pipeline.run()

    assert result == {
        "advertiser_daily": 1,
        "creative_daily": 1,
        "traffic_quality_daily": 1,
    }
    assert calls["gold_backend"] == "gold"
    assert calls["serving_store"] == "store"
