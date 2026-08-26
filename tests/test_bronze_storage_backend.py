import pytest

import src.processing.bronze_writer as bronze_writer


def _resolver():
    resolver = getattr(
        bronze_writer,
        "resolve_bronze_backend_name",
        None,
    )
    assert resolver is not None, (
        "resolve_bronze_backend_name has not been implemented yet"
    )
    return resolver


def test_bronze_backend_defaults_to_delta():
    resolve = _resolver()

    assert resolve({}) == "delta"


def test_bronze_backend_accepts_explicit_delta():
    resolve = _resolver()

    assert resolve({"ADSTREAM_STORAGE_BACKEND": "delta"}) == "delta"


def test_bronze_backend_accepts_explicit_iceberg():
    resolve = _resolver()

    assert resolve({"ADSTREAM_STORAGE_BACKEND": "iceberg"}) == "iceberg"


def test_bronze_backend_rejects_unknown_value():
    resolve = _resolver()

    with pytest.raises(ValueError, match="delta.*iceberg|iceberg.*delta"):
        resolve({"ADSTREAM_STORAGE_BACKEND": "postgres"})


def _spark_config_module():
    import importlib

    return importlib.import_module("src.processing.spark")


def _complete_iceberg_env():
    return {
        "SUPABASE_PROJECT_REF": "example-project",
        "SUPABASE_CATALOG_TOKEN": "test-token",
        "SUPABASE_ICEBERG_WAREHOUSE": "adstream-analytics",
        "SUPABASE_S3_ACCESS_KEY": "test-access",
        "SUPABASE_S3_SECRET_KEY": "test-secret",
        "SUPABASE_S3_REGION": "eu-west-1",
    }


def test_iceberg_settings_require_catalog_token():
    spark_config = _spark_config_module()
    env = _complete_iceberg_env()
    del env["SUPABASE_CATALOG_TOKEN"]

    with pytest.raises(
        ValueError,
        match="SUPABASE_CATALOG_TOKEN",
    ):
        spark_config.get_iceberg_settings(env)


def test_iceberg_settings_require_all_supabase_values():
    spark_config = _spark_config_module()

    with pytest.raises(
        ValueError,
        match="SUPABASE_PROJECT_REF",
    ):
        spark_config.get_iceberg_settings({})


def test_iceberg_settings_build_supabase_endpoints():
    spark_config = _spark_config_module()

    settings = spark_config.get_iceberg_settings(
        _complete_iceberg_env()
    )

    assert settings["warehouse"] == "adstream-analytics"
    assert settings["region"] == "eu-west-1"

    assert settings["catalog_uri"] == (
        "https://example-project.supabase.co/storage/v1/iceberg"
    )

    assert settings["s3_endpoint"] == (
        "https://example-project.supabase.co/storage/v1/s3"
    )


def test_iceberg_settings_preserve_credentials_without_logging_or_transforming():
    spark_config = _spark_config_module()

    settings = spark_config.get_iceberg_settings(
        _complete_iceberg_env()
    )

    assert settings["catalog_token"] == "test-token"
    assert settings["access_key"] == "test-access"
    assert settings["secret_key"] == "test-secret"


def test_delta_spark_config_uses_delta_extensions_only():
    spark_config = _spark_config_module()

    config = spark_config.get_storage_spark_config(
        "delta",
        {},
    )

    assert config["spark.sql.extensions"] == (
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    assert config["spark.sql.catalog.spark_catalog"] == (
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    assert not any(
        key.startswith("spark.sql.catalog.supabase")
        for key in config
    )


def test_iceberg_spark_config_targets_supabase_catalog():
    spark_config = _spark_config_module()

    config = spark_config.get_storage_spark_config(
        "iceberg",
        _complete_iceberg_env(),
    )

    assert config["spark.sql.extensions"] == (
        "org.apache.iceberg.spark.extensions."
        "IcebergSparkSessionExtensions"
    )

    assert config["spark.sql.catalog.supabase"] == (
        "org.apache.iceberg.spark.SparkCatalog"
    )
    assert config["spark.sql.catalog.supabase.type"] == "rest"

    assert config["spark.sql.catalog.supabase.uri"] == (
        "https://example-project.supabase.co/storage/v1/iceberg"
    )

    assert config["spark.sql.catalog.supabase.warehouse"] == (
        "adstream-analytics"
    )

    assert config["spark.sql.catalog.supabase.s3.endpoint"] == (
        "https://example-project.supabase.co/storage/v1/s3"
    )

    assert config["spark.sql.catalog.supabase.client.region"] == (
        "eu-west-1"
    )


def test_iceberg_spark_config_contains_required_runtime_packages():
    spark_config = _spark_config_module()

    config = spark_config.get_storage_spark_config(
        "iceberg",
        _complete_iceberg_env(),
    )

    packages = config["spark.jars.packages"]

    assert (
        "org.apache.iceberg:"
        "iceberg-spark-runtime-3.5_2.12:1.6.1"
    ) in packages

    assert (
        "org.apache.iceberg:"
        "iceberg-aws-bundle:1.6.1"
    ) in packages


def test_iceberg_spark_config_passes_storage_credentials():
    spark_config = _spark_config_module()

    config = spark_config.get_storage_spark_config(
        "iceberg",
        _complete_iceberg_env(),
    )

    assert (
        config["spark.sql.catalog.supabase.s3.access-key-id"]
        == "test-access"
    )
    assert (
        config["spark.sql.catalog.supabase.s3.secret-access-key"]
        == "test-secret"
    )
    assert (
        config["spark.sql.catalog.supabase.token"]
        == "test-token"
    )


def _bronze_storage_module():
    import importlib

    try:
        return importlib.import_module("src.storage.bronze")
    except ModuleNotFoundError:
        pytest.fail(
            "src.storage.bronze has not been implemented yet"
        )


class _RecordingWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def partitionBy(self, value):
        self.calls.append(("partitionBy", value))
        return self

    def save(self, value):
        self.calls.append(("save", value))


class _RecordingReader:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def load(self, value):
        self.calls.append(("load", value))
        return "delta-read-result"


class _FakeField:
    def __init__(self, name, sql_type):
        self.name = name
        self.dataType = self
        self._sql_type = sql_type

    def simpleString(self):
        return self._sql_type


class _FakeSchema:
    def __init__(self):
        self.fields = [
            _FakeField("impression_id", "string"),
            _FakeField("timestamp", "timestamp"),
            _FakeField("ingestion_date", "date"),
        ]


class _FakeDataFrame:
    def __init__(self):
        self.write = _RecordingWriter()
        self.schema = _FakeSchema()
        self.temp_views = []

    def createOrReplaceTempView(self, name):
        self.temp_views.append(name)


class _FakeSpark:
    def __init__(self):
        self.read = _RecordingReader()
        self.sql_calls = []
        self.table_calls = []

    def sql(self, query):
        normalized = " ".join(query.split())
        self.sql_calls.append(normalized)
        return None

    def table(self, name):
        self.table_calls.append(name)
        return "iceberg-read-result"


def test_backend_factory_returns_delta_backend():
    storage = _bronze_storage_module()
    spark = _FakeSpark()

    backend = storage.build_bronze_backend(
        spark,
        "delta",
    )

    assert isinstance(
        backend,
        storage.DeltaBronzeBackend,
    )


def test_backend_factory_returns_iceberg_backend():
    storage = _bronze_storage_module()
    spark = _FakeSpark()

    backend = storage.build_bronze_backend(
        spark,
        "iceberg",
    )

    assert isinstance(
        backend,
        storage.IcebergBronzeBackend,
    )


def test_delta_backend_appends_partitioned_data():
    storage = _bronze_storage_module()
    spark = _FakeSpark()
    df = _FakeDataFrame()

    backend = storage.DeltaBronzeBackend(spark)
    backend.write(df)

    assert df.write.calls == [
        ("format", "delta"),
        ("mode", "append"),
        ("partitionBy", "ingestion_date"),
        ("save", "data/bronze/impressions"),
    ]


def test_delta_backend_reads_local_delta():
    storage = _bronze_storage_module()
    spark = _FakeSpark()

    backend = storage.DeltaBronzeBackend(spark)

    assert backend.read() == "delta-read-result"
    assert spark.read.calls == [
        ("format", "delta"),
        ("load", "data/bronze/impressions"),
    ]


def test_iceberg_backend_creates_table_then_inserts():
    storage = _bronze_storage_module()
    spark = _FakeSpark()
    df = _FakeDataFrame()

    backend = storage.IcebergBronzeBackend(spark)
    backend.write(df)

    assert any(
        "CREATE NAMESPACE IF NOT EXISTS supabase.bronze"
        in query
        for query in spark.sql_calls
    )

    create_table_calls = [
        query
        for query in spark.sql_calls
        if "CREATE TABLE IF NOT EXISTS" in query
    ]

    assert len(create_table_calls) == 1
    assert "supabase.bronze.impressions" in create_table_calls[0]
    assert "USING iceberg" in create_table_calls[0]
    assert "PARTITIONED BY (`ingestion_date`)" in create_table_calls[0]

    assert df.temp_views == ["adstream_bronze_batch"]

    assert any(
        "INSERT INTO supabase.bronze.impressions"
        in query
        for query in spark.sql_calls
    )


def test_iceberg_backend_reads_cloud_table():
    storage = _bronze_storage_module()
    spark = _FakeSpark()

    backend = storage.IcebergBronzeBackend(spark)

    assert backend.read() == "iceberg-read-result"
    assert spark.table_calls == [
        "supabase.bronze.impressions"
    ]


class _FakeBackend:
    def __init__(self):
        self.written = []
        self.read_result = object()

    def write(self, df):
        self.written.append(df)

    def read(self):
        return self.read_result


class _FakeSparkForWriter:
    def __init__(self):
        self.created_rows = None
        self.created_schema = None
        self.result_df = None

    def createDataFrame(self, rows, schema):
        self.created_rows = rows
        self.created_schema = schema
        self.result_df = _FakePreparedDataFrame()
        return self.result_df


class _FakePreparedDataFrame:
    def __init__(self):
        self.with_column_calls = []

    def withColumn(self, name, expression):
        self.with_column_calls.append((name, expression))
        return self


def _sample_impression_event():
    from src.models.events import ImpressionEvent

    return ImpressionEvent(
        impression_id="imp-1",
        user_id="user-1",
        advertiser_id="2997",
        campaign_id="campaign-1",
        content_id="11908",
        bid_price=277.0,
        currency="CNY",
        country_code="CN",
        device_type="mobile",
        ad_format="banner",
        timestamp="2013-10-23T17:10:05.542Z",
        is_fraud=False,
        paying_price=19.0,
        pricing_basis="CPM",
        clicked=False,
        source_dataset="ipinyou",
        source_bid_id="source-bid-1",
        ad_exchange=None,
        slot_id="1",
        source_user_agent="test-agent",
    )


def test_bronze_writer_uses_selected_backend(monkeypatch):
    fake_spark = _FakeSparkForWriter()
    fake_backend = _FakeBackend()

    monkeypatch.setattr(
        bronze_writer,
        "build_bronze_backend",
        lambda spark, backend: fake_backend,
        raising=False,
    )

    writer = bronze_writer.BronzeWriter(
        spark=fake_spark,
        backend_name="iceberg",
    )

    assert writer.backend is fake_backend
    assert writer.backend_name == "iceberg"


def test_bronze_writer_defaults_backend_from_environment(monkeypatch):
    fake_spark = _FakeSparkForWriter()
    fake_backend = _FakeBackend()
    captured = {}

    monkeypatch.setenv(
        "ADSTREAM_STORAGE_BACKEND",
        "iceberg",
    )

    def fake_build(spark, backend):
        captured["backend"] = backend
        return fake_backend

    monkeypatch.setattr(
        bronze_writer,
        "build_bronze_backend",
        fake_build,
        raising=False,
    )

    writer = bronze_writer.BronzeWriter(
        spark=fake_spark,
    )

    assert writer.backend_name == "iceberg"
    assert captured["backend"] == "iceberg"


def test_bronze_writer_empty_batch_does_not_write(monkeypatch):
    fake_spark = _FakeSparkForWriter()
    fake_backend = _FakeBackend()

    monkeypatch.setattr(
        bronze_writer,
        "build_bronze_backend",
        lambda spark, backend: fake_backend,
        raising=False,
    )

    writer = bronze_writer.BronzeWriter(
        spark=fake_spark,
        backend_name="delta",
    )

    assert writer.write_batch([]) == 0
    assert fake_backend.written == []


def test_bronze_writer_delegates_prepared_dataframe(monkeypatch):
    fake_spark = _FakeSparkForWriter()
    fake_backend = _FakeBackend()

    # This is a unit test with a fake SparkSession, so stub only the
    # PySpark expression builders that normally require a live JVM.
    monkeypatch.setattr(
        bronze_writer.F,
        "col",
        lambda name: ("col", name),
    )
    monkeypatch.setattr(
        bronze_writer.F,
        "to_date",
        lambda expression: ("to_date", expression),
    )

    monkeypatch.setattr(
        bronze_writer,
        "build_bronze_backend",
        lambda spark, backend: fake_backend,
        raising=False,
    )

    writer = bronze_writer.BronzeWriter(
        spark=fake_spark,
        backend_name="delta",
    )

    written = writer.write_batch(
        [_sample_impression_event()]
    )

    assert written == 1
    assert fake_backend.written == [
        fake_spark.result_df
    ]

    assert len(
        fake_spark.result_df.with_column_calls
    ) == 1

    assert (
        fake_spark.result_df.with_column_calls[0][0]
        == "ingestion_date"
    )
    assert (
        fake_spark.result_df.with_column_calls[0][1]
        == ("to_date", ("col", "timestamp"))
    )


def test_bronze_writer_reads_through_backend(monkeypatch):
    fake_spark = _FakeSparkForWriter()
    fake_backend = _FakeBackend()

    monkeypatch.setattr(
        bronze_writer,
        "build_bronze_backend",
        lambda spark, backend: fake_backend,
        raising=False,
    )

    writer = bronze_writer.BronzeWriter(
        spark=fake_spark,
        backend_name="delta",
    )

    assert writer.read_bronze() is fake_backend.read_result
