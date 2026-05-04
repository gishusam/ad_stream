from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, BooleanType
)
from src.utils.logger import get_logger
from datetime import datetime, timezone
import random

logger = get_logger("user_profiles")

# In production this comes from PostgreSQL via Debezium CDC into Kafka.
# Locally we simulate a realistic user base that matches the user_ids
# our data generator creates — UUID format for real users, bot_ prefix
# for fraud users.

USER_TIERS    = ["free", "basic", "premium", "enterprise"]
COUNTRIES     = ["US", "GB", "KE", "DE", "FR", "NG", "ZA", "IN", "BR", "CA"]
CURRENCIES    = ["USD", "GBP", "KES", "EUR", "EUR", "NGN", "ZAR", "INR", "BRL", "CAD"]
COUNTRY_CURRENCY = dict(zip(COUNTRIES, CURRENCIES))

USER_PROFILE_SCHEMA = StructType([
    StructField("user_id",            StringType(),    nullable=False),
    StructField("account_tier",       StringType(),    nullable=False),
    StructField("registration_country", StringType(),  nullable=False),
    StructField("preferred_currency", StringType(),    nullable=False),
    StructField("is_verified",        BooleanType(),   nullable=False),
    StructField("created_at",         TimestampType(), nullable=False),
])


def generate_user_profiles(spark: SparkSession, n_users: int = 1000) -> DataFrame:
    """
    Generate a realistic user profiles reference table.

    This simulates what would come from your PostgreSQL users table
    via CDC in production. We generate n_users profiles with realistic
    tier distribution — most users are free, few are enterprise.

    Why a function not a class?
    Reference data generation is a pure function — same inputs, same
    outputs, no state. A class would be unnecessary complexity here.
    """
    random.seed(42)  # reproducible data — same profiles every run

    rows = []
    for i in range(n_users):
        user_id  = f"user_{str(i).zfill(6)}"
        country  = random.choice(COUNTRIES)
        currency = COUNTRY_CURRENCY[country]

        # Realistic tier distribution
        # 60% free, 25% basic, 12% premium, 3% enterprise
        tier = random.choices(
            USER_TIERS,
            weights=[60, 25, 12, 3]
        )[0]

        rows.append({
            "user_id":              user_id,
            "account_tier":         tier,
            "registration_country": country,
            "preferred_currency":   currency,
            "is_verified":          random.random() > 0.15,  # 85% verified
            "created_at":           datetime.now(timezone.utc),
        })

    df = spark.createDataFrame(rows, schema=USER_PROFILE_SCHEMA)

    logger.info(
        "user_profiles_generated",
        total=n_users,
        schema=str(df.schema),
    )

    return df