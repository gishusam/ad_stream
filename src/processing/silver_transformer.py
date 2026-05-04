from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.utils.logger import get_logger

logger = get_logger("silver_transformer")

SILVER_LEGITIMATE_PATH = "data/silver/impressions/legitimate"
SILVER_FRAUD_PATH      = "data/silver/impressions/fraud"
SILVER_QUARANTINE_PATH = "data/silver/impressions/quarantine"

VALID_COUNTRIES  = ["US", "GB", "KE", "DE", "FR", "NG", "ZA", "IN", "BR", "CA"]
VALID_CURRENCIES = ["USD", "EUR", "GBP", "KES", "NGN", "ZAR", "INR", "BRL", "CAD"]
VALID_DEVICES    = ["mobile", "desktop", "tablet", "ctv"]
VALID_FORMATS    = ["banner", "video", "native", "audio"]

# Data quality thresholds
MAX_FRAUD_RATE   = 0.10   # above 10% suggests pipeline problem
MAX_NULL_RATE    = 0.05   # above 5% nulls is unacceptable


class SilverTransformer:
    """
    Transforms Bronze impression data into Silver quality.

    Four steps — always in this order:
    1. Deduplicate      — remove duplicate impression_ids
    2. Quality check    — identify and quarantine bad records
    3. Enrich           — join user profile context
    4. Split            — separate legitimate from fraud

    Why a class?
    The transformer holds configuration (thresholds, paths) and
    produces multiple outputs from one input. A class organises
    this cleanly. Each method is one responsibility.
    """

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate impression_ids.

        Why duplicates exist:
        Kafka delivers at-least-once. If a network retry happens,
        the same event arrives twice. Without deduplication, an
        advertiser gets billed twice for one impression.

        dropDuplicates keeps the first occurrence and drops the rest.
        We deduplicate on impression_id only — the unique business key.
        """
        before = df.count()
        df_deduped = df.dropDuplicates(["impression_id"])
        after = df_deduped.count()
        duplicates_removed = before - after

        logger.info(
            "deduplication_complete",
            before=before,
            after=after,
            duplicates_removed=duplicates_removed,
        )

        return df_deduped

    def check_quality(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Separate clean records from quarantine-worthy ones.

        Quarantine conditions:
        - null bid_price
        - null user_id
        - invalid country_code not in approved list
        - invalid currency not in approved list
        - bid_price above $50 (data entry error threshold)

        Returns:
            clean_df:      records that pass all checks
            quarantine_df: records that failed at least one check
        """
        # Build a quality flag column
        # Each condition adds a flag. Clean records have no flags.
        df = df.withColumn(
            "quality_issues",
            F.concat_ws(", ",
                F.when(F.col("bid_price").isNull(), F.lit("null_bid_price")),
                F.when(F.col("user_id").isNull(), F.lit("null_user_id")),
                F.when(
                    ~F.col("country_code").isin(VALID_COUNTRIES),
                    F.lit("invalid_country")
                ),
                F.when(
                    ~F.col("currency").isin(VALID_CURRENCIES),
                    F.lit("invalid_currency")
                ),
                F.when(
                    F.col("bid_price") > 50.0,
                    F.lit("bid_price_exceeds_threshold")
                ),
            ).cast(StringType())
        )

        # Split on whether quality_issues is empty or not
        clean_df      = df.filter(
            F.col("quality_issues").isNull() |
            (F.col("quality_issues") == "")
        ).drop("quality_issues")

        quarantine_df = df.filter(
            F.col("quality_issues").isNotNull() &
            (F.col("quality_issues") != "")
        )

        clean_count      = clean_df.count()
        quarantine_count = quarantine_df.count()

        logger.info(
            "quality_check_complete",
            clean=clean_count,
            quarantined=quarantine_count,
        )

        if quarantine_count > 0:
            logger.warning(
                "records_quarantined",
                count=quarantine_count,
                sample_issues=quarantine_df.select(
                    "impression_id", "quality_issues"
                ).limit(3).collect(),
            )

        return clean_df, quarantine_df

    def enrich(self, df: DataFrame, user_profiles: DataFrame) -> DataFrame:
        """
        Join impression data with user profile context.

        Why left join not inner join?
        Inner join drops impressions where no user profile exists.
        That means silent data loss — revenue events disappear.
        Left join keeps all impressions and fills missing profile
        fields with null. We know the impression happened even if
        we don't have the user context.

        Fields added from user profiles:
        - account_tier:         free/basic/premium/enterprise
        - registration_country: where the user signed up
        - preferred_currency:   user's billing currency
        - is_verified:          verified account flag
        """
        df_enriched = df.join(
            user_profiles.select(
                "user_id",
                "account_tier",
                "registration_country",
                "preferred_currency",
                "is_verified",
            ),
            on="user_id",
            how="left",
        )

        # Track how many impressions had no matching user profile
        no_profile_count = df_enriched.filter(
            F.col("account_tier").isNull()
        ).count()

        logger.info(
            "enrichment_complete",
            total=df_enriched.count(),
            missing_profiles=no_profile_count,
        )

        return df_enriched

    def split_fraud(
        self, df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """
        Split enriched data into legitimate and fraud tables.

        Why write fraud to Silver instead of dropping it?
        Fraud events are valuable data for the fraud team.
        They need to analyse patterns, train ML models, and
        investigate specific advertisers. Dropping fraud means
        losing that signal forever.

        Legitimate table: used for revenue reports and billing
        Fraud table:      used for fraud analysis and model training
        """
        legitimate_df = df.filter(F.col("is_fraud") == False)
        fraud_df      = df.filter(F.col("is_fraud") == True)

        logger.info(
            "fraud_split_complete",
            legitimate=legitimate_df.count(),
            fraud=fraud_df.count(),
            fraud_rate=round(
                fraud_df.count() / df.count(), 4
            ) if df.count() > 0 else 0,
        )

        return legitimate_df, fraud_df

    def transform(
        self,
        bronze_df: DataFrame,
        user_profiles: DataFrame,
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Run the full Silver transformation pipeline.

        Returns:
            legitimate_df:  clean, enriched, fraud-free impressions
            fraud_df:       fraud impressions for analysis
            quarantine_df:  records that failed quality checks
        """
        logger.info(
            "silver_transformation_started",
            input_rows=bronze_df.count(),
        )

        # Step 1 — deduplicate
        df = self.deduplicate(bronze_df)

        # Step 2 — quality check
        df, quarantine_df = self.check_quality(df)

        # Step 3 — enrich
        df = self.enrich(df, user_profiles)

        # Step 4 — split fraud
        legitimate_df, fraud_df = self.split_fraud(df)

        logger.info(
            "silver_transformation_complete",
            legitimate=legitimate_df.count(),
            fraud=fraud_df.count(),
            quarantined=quarantine_df.count(),
        )

        return legitimate_df, fraud_df, quarantine_df