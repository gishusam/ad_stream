"""Persistence backends for AdStream Gold aggregates."""

from pyspark.sql import DataFrame, SparkSession


GOLD_ADVERTISER_PATH = "data/gold/advertiser_daily"
GOLD_CREATIVE_PATH = "data/gold/creative_daily"
GOLD_QUALITY_PATH = "data/gold/traffic_quality_daily"


class DeltaGoldBackend:
    """Rebuild Gold aggregate tables in local Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        advertiser_path: str = GOLD_ADVERTISER_PATH,
        creative_path: str = GOLD_CREATIVE_PATH,
        quality_path: str = GOLD_QUALITY_PATH,
    ):
        self.spark = spark
        self.advertiser_path = advertiser_path
        self.creative_path = creative_path
        self.quality_path = quality_path

    def _overwrite(
        self,
        df: DataFrame,
        path: str,
    ) -> None:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )

    def write(
        self,
        advertiser_df: DataFrame,
        creative_df: DataFrame,
        quality_df: DataFrame,
    ) -> None:
        self._overwrite(
            advertiser_df,
            self.advertiser_path,
        )
        self._overwrite(
            creative_df,
            self.creative_path,
        )
        self._overwrite(
            quality_df,
            self.quality_path,
        )

    def read_advertiser_daily(self) -> DataFrame:
        return (
            self.spark.read
            .format("delta")
            .load(self.advertiser_path)
        )


ICEBERG_GOLD_NAMESPACE = "supabase.gold"

ICEBERG_ADVERTISER_TABLE = (
    "supabase.gold.advertiser_daily"
)
ICEBERG_CREATIVE_TABLE = (
    "supabase.gold.creative_daily"
)
ICEBERG_QUALITY_TABLE = (
    "supabase.gold.traffic_quality_daily"
)


class IcebergGoldBackend:
    """Rebuild Gold aggregate tables in Supabase Iceberg."""

    def __init__(
        self,
        spark: SparkSession,
        advertiser_table: str = ICEBERG_ADVERTISER_TABLE,
        creative_table: str = ICEBERG_CREATIVE_TABLE,
        quality_table: str = ICEBERG_QUALITY_TABLE,
    ):
        self.spark = spark
        self.advertiser_table = advertiser_table
        self.creative_table = creative_table
        self.quality_table = quality_table

    def _replace_table(
        self,
        df: DataFrame,
        table: str,
        view: str,
    ) -> None:
        columns_sql = ",\n    ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in df.schema.fields
        )

        self.spark.sql(
            f"DROP TABLE IF EXISTS {table}"
        )

        self.spark.sql(
            f"""
            CREATE TABLE {table} (
                {columns_sql}
            )
            USING iceberg
            """
        )

        df.createOrReplaceTempView(view)

        self.spark.sql(
            f"""
            INSERT INTO {table}
            SELECT *
            FROM {view}
            """
        )

    def write(
        self,
        advertiser_df: DataFrame,
        creative_df: DataFrame,
        quality_df: DataFrame,
    ) -> None:
        self.spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS
            {ICEBERG_GOLD_NAMESPACE}
            """
        )

        self._replace_table(
            advertiser_df,
            self.advertiser_table,
            "adstream_gold_advertiser",
        )

        self._replace_table(
            creative_df,
            self.creative_table,
            "adstream_gold_creative",
        )

        self._replace_table(
            quality_df,
            self.quality_table,
            "adstream_gold_quality",
        )
