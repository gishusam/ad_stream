from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.logger import get_logger

logger = get_logger("gold_aggregator")

GOLD_REVENUE_PATH     = "data/gold/revenue_by_advertiser"
GOLD_CONTENT_PATH     = "data/gold/content_performance"
GOLD_FRAUD_PATH       = "data/gold/fraud_summary"


class GoldAggregator:
    """
    Computes business-level aggregations from Silver data.

    Each method answers one specific business question.
    Gold tables are pre-computed so dashboards and analysts
    never scan raw event data — they read pre-aggregated results.

    Design principle: each method is pure — takes a DataFrame,
    returns a DataFrame. No side effects, no writing.
    Writing is handled by GoldIngestionPipeline.
    """

    def compute_revenue_by_advertiser(
        self, legitimate_df: DataFrame
    ) -> DataFrame:
        """
        Hourly revenue metrics per advertiser.

        Business question: how much did each advertiser spend
        this hour, and what did they get for it?

        Metrics computed:
        - total_spend:        sum of all bid prices
        - impression_count:   number of impressions delivered
        - avg_cpm:            cost per thousand impressions
        - mobile_impressions: impressions on mobile devices
        - video_impressions:  impressions in video format
        - unique_campaigns:   number of active campaigns

        Why hourly partitioning?
        Finance runs billing hourly. Advertisers get hourly
        spend alerts. Hourly granularity matches business cadence.
        """
        # Truncate timestamp to hour for grouping
        # e.g. 2026-05-04 14:37:22 → 2026-05-04 14:00:00
        df = legitimate_df.withColumn(
            "hour",
            F.date_trunc("hour", F.col("timestamp"))
        )

        # Window for ranking advertisers by spend within each hour
        hour_window = Window.partitionBy("hour").orderBy(
            F.col("total_spend").desc()
        )

        revenue_df = (
            df.groupBy("advertiser_id", "hour")
            .agg(
                F.round(F.sum("bid_price"), 4)
                 .alias("total_spend"),

                F.count("impression_id")
                 .alias("impression_count"),

                F.round(
                    (F.sum("bid_price") / F.count("impression_id")) * 1000, 4
                ).alias("avg_cpm"),

                F.sum(
                    F.when(F.col("device_type") == "mobile", 1).otherwise(0)
                ).alias("mobile_impressions"),

                F.sum(
                    F.when(F.col("ad_format") == "video", 1).otherwise(0)
                ).alias("video_impressions"),

                F.countDistinct("campaign_id")
                 .alias("unique_campaigns"),

                F.first("ingestion_date")
                 .alias("ingestion_date"),
            )
        )

        # Add rank — which advertiser spent most this hour?
        revenue_df = revenue_df.withColumn(
            "spend_rank_in_hour",
            F.rank().over(hour_window)
        )

        count = revenue_df.count()
        logger.info(
            "revenue_aggregation_complete",
            rows=count,
            metric="revenue_by_advertiser",
        )

        return revenue_df

    def compute_content_performance(
        self, legitimate_df: DataFrame
    ) -> DataFrame:
        """
        Daily content performance metrics.

        Business question: which content is driving the most
        value, and what makes it perform well?

        This is the table you identified as most valuable —
        it answers where to invest content production budget.

        Metrics computed:
        - total_impressions:    how many times was this content shown?
        - total_revenue:        how much did it earn?
        - avg_cpm:              revenue efficiency per 1000 impressions
        - top_device:           which device drove most impressions?
        - top_country:          which country engaged most?
        - top_ad_format:        which format performed best?
        - premium_user_rate:    what % of viewers are premium/enterprise?
        - verified_user_rate:   what % of viewers are verified?

        Why daily not hourly?
        Content performance needs enough data to be statistically
        meaningful. Hourly content metrics have too few impressions
        per content piece to draw conclusions. Daily gives signal.
        """
        # Window for finding the mode (most common value) per content
        device_window  = Window.partitionBy(
            "content_id", "ingestion_date"
        ).orderBy(F.col("device_count").desc())

        country_window = Window.partitionBy(
            "content_id", "ingestion_date"
        ).orderBy(F.col("country_count").desc())

        format_window  = Window.partitionBy(
            "content_id", "ingestion_date"
        ).orderBy(F.col("format_count").desc())

        # Step 1 — core metrics
        core_df = (
            legitimate_df.groupBy("content_id", "ingestion_date")
            .agg(
                F.count("impression_id")
                 .alias("total_impressions"),

                F.round(F.sum("bid_price"), 4)
                 .alias("total_revenue"),

                F.round(
                    (F.sum("bid_price") / F.count("impression_id")) * 1000, 4
                ).alias("avg_cpm"),

                F.round(
                    F.avg(
                        F.when(
                            F.col("account_tier").isin(
                                ["premium", "enterprise"]
                            ), 1
                        ).otherwise(0)
                    ), 4
                ).alias("premium_user_rate"),

                F.round(
                    F.avg(
                        F.when(F.col("is_verified") == True, 1)
                         .otherwise(0)
                    ), 4
                ).alias("verified_user_rate"),
            )
        )

        # Step 2 — top device per content per day
        device_df = (
            legitimate_df
            .groupBy("content_id", "ingestion_date", "device_type")
            .agg(F.count("*").alias("device_count"))
            .withColumn("rn", F.row_number().over(device_window))
            .filter(F.col("rn") == 1)
            .select("content_id", "ingestion_date",
                    F.col("device_type").alias("top_device"))
        )

        # Step 3 — top country per content per day
        country_df = (
            legitimate_df
            .groupBy("content_id", "ingestion_date", "country_code")
            .agg(F.count("*").alias("country_count"))
            .withColumn("rn", F.row_number().over(country_window))
            .filter(F.col("rn") == 1)
            .select("content_id", "ingestion_date",
                    F.col("country_code").alias("top_country"))
        )

        # Step 4 — top ad format per content per day
        format_df = (
            legitimate_df
            .groupBy("content_id", "ingestion_date", "ad_format")
            .agg(F.count("*").alias("format_count"))
            .withColumn("rn", F.row_number().over(format_window))
            .filter(F.col("rn") == 1)
            .select("content_id", "ingestion_date",
                    F.col("ad_format").alias("top_ad_format"))
        )

        # Step 5 — join everything together
        content_df = (
            core_df
            .join(device_df,  ["content_id", "ingestion_date"], "left")
            .join(country_df, ["content_id", "ingestion_date"], "left")
            .join(format_df,  ["content_id", "ingestion_date"], "left")
        )

        count = content_df.count()
        logger.info(
            "content_aggregation_complete",
            rows=count,
            metric="content_performance",
        )

        return content_df

    def compute_fraud_summary(
        self, fraud_df: DataFrame
    ) -> DataFrame:
        """
        Daily fraud summary metrics.

        Business question: how much fraud did we catch today,
        what did it cost, and where is it coming from?

        Metrics computed:
        - fraud_impression_count: how many fraud events caught
        - fraud_revenue_at_risk:  dollars that would have been paid out
        - avg_fraud_bid_price:    average fraudulent bid (higher = more sophisticated)
        - top_targeted_advertiser: which advertiser bots targeted most
        - fraud_by_country:       aggregated separately for drill-down

        Why this matters to the business:
        Each fraud impression prevented saves the advertiser money.
        fraud_revenue_at_risk is the dollar value your fraud
        detection system saved today. This is your system's ROI.
        """
        # Daily fraud totals
        daily_fraud_df = (
            fraud_df.groupBy("ingestion_date")
            .agg(
                F.count("impression_id")
                 .alias("fraud_impression_count"),

                F.round(F.sum("bid_price"), 4)
                 .alias("fraud_revenue_at_risk"),

                F.round(F.avg("bid_price"), 4)
                 .alias("avg_fraud_bid_price"),

                F.countDistinct("advertiser_id")
                 .alias("advertisers_targeted"),

                F.countDistinct("country_code")
                 .alias("countries_affected"),
            )
        )

        # Most targeted advertiser per day
        adv_window = Window.partitionBy("ingestion_date").orderBy(
            F.col("adv_fraud_count").desc()
        )

        top_adv_df = (
            fraud_df
            .groupBy("ingestion_date", "advertiser_id")
            .agg(F.count("*").alias("adv_fraud_count"))
            .withColumn("rn", F.row_number().over(adv_window))
            .filter(F.col("rn") == 1)
            .select(
                "ingestion_date",
                F.col("advertiser_id").alias("top_targeted_advertiser")
            )
        )

        fraud_summary_df = daily_fraud_df.join(
            top_adv_df, "ingestion_date", "left"
        )

        count = fraud_summary_df.count()
        logger.info(
            "fraud_aggregation_complete",
            rows=count,
            metric="fraud_summary",
        )

        return fraud_summary_df