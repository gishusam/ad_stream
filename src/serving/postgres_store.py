"""Postgres serving store for AdStream Gold aggregates."""

import re

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Identifier, SQL


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresServingStore:
    def __init__(self, database_url: str, schema: str = "serving"):
        if not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"Invalid schema name: {schema!r}")

        self.database_url = database_url
        self.schema = schema

    def replace_advertiser_daily(self, rows: list[dict]) -> None:
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    Identifier(self.schema)
                )
            )

            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.advertiser_daily (
                        event_date DATE NOT NULL,
                        advertiser_id TEXT NOT NULL,
                        impressions BIGINT NOT NULL,
                        total_spend_cny DOUBLE PRECISION NOT NULL,
                        average_bid_cpm DOUBLE PRECISION,
                        average_clearing_cpm DOUBLE PRECISION,
                        total_auction_savings_cny DOUBLE PRECISION NOT NULL,
                        warning_events BIGINT NOT NULL
                    )
                    """
                ).format(Identifier(self.schema))
            )

            conn.execute(
                SQL("TRUNCATE TABLE {}.advertiser_daily").format(
                    Identifier(self.schema)
                )
            )

            cursor = conn.cursor()
            cursor.executemany(
                SQL(
                    """
                    INSERT INTO {}.advertiser_daily (
                        event_date,
                        advertiser_id,
                        impressions,
                        total_spend_cny,
                        average_bid_cpm,
                        average_clearing_cpm,
                        total_auction_savings_cny,
                        warning_events
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                ).format(Identifier(self.schema)),
                [
                    (
                        row["event_date"],
                        row["advertiser_id"],
                        row["impressions"],
                        row["total_spend_cny"],
                        row["average_bid_cpm"],
                        row["average_clearing_cpm"],
                        row["total_auction_savings_cny"],
                        row["warning_events"],
                    )
                    for row in rows
                ],
            )

    def list_advertiser_daily(
        self,
        event_date: str | None = None,
        advertiser_id: str | None = None,
    ) -> list[dict]:
        conditions = []
        params = []

        if event_date is not None:
            conditions.append(SQL("event_date = %s"))
            params.append(event_date)

        if advertiser_id is not None:
            conditions.append(SQL("advertiser_id = %s"))
            params.append(advertiser_id)

        where_clause = SQL("")
        if conditions:
            where_clause = (
                SQL(" WHERE ")
                + SQL(" AND ").join(conditions)
            )

        query = (
            SQL(
                """
                SELECT
                    event_date::text AS event_date,
                    advertiser_id,
                    impressions,
                    total_spend_cny,
                    average_bid_cpm,
                    average_clearing_cpm,
                    total_auction_savings_cny,
                    warning_events
                FROM {}.advertiser_daily
                """
            ).format(Identifier(self.schema))
            + where_clause
            + SQL(" ORDER BY event_date, advertiser_id")
        )

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(
                query,
                params,
            ).fetchall()

        return rows

    def drop_schema(self) -> None:
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    Identifier(self.schema)
                )
            )

    def replace_creative_daily(self, rows: list[dict]) -> None:
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    Identifier(self.schema)
                )
            )

            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.creative_daily (
                        event_date DATE NOT NULL,
                        advertiser_id TEXT NOT NULL,
                        creative_id TEXT NOT NULL,
                        impressions BIGINT NOT NULL,
                        total_spend_cny DOUBLE PRECISION NOT NULL,
                        average_clearing_cpm DOUBLE PRECISION,
                        clicks BIGINT NOT NULL
                    )
                    """
                ).format(Identifier(self.schema))
            )

            conn.execute(
                SQL("TRUNCATE TABLE {}.creative_daily").format(
                    Identifier(self.schema)
                )
            )

            cursor = conn.cursor()
            cursor.executemany(
                SQL(
                    """
                    INSERT INTO {}.creative_daily (
                        event_date,
                        advertiser_id,
                        creative_id,
                        impressions,
                        total_spend_cny,
                        average_clearing_cpm,
                        clicks
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                ).format(Identifier(self.schema)),
                [
                    (
                        row["event_date"],
                        row["advertiser_id"],
                        row["creative_id"],
                        row["impressions"],
                        row["total_spend_cny"],
                        row["average_clearing_cpm"],
                        row["clicks"],
                    )
                    for row in rows
                ],
            )

    def list_creative_daily(
        self,
        event_date: str | None = None,
        advertiser_id: str | None = None,
        creative_id: str | None = None,
    ) -> list[dict]:
        conditions = []
        params = []

        if event_date is not None:
            conditions.append(SQL("event_date = %s"))
            params.append(event_date)

        if advertiser_id is not None:
            conditions.append(SQL("advertiser_id = %s"))
            params.append(advertiser_id)

        if creative_id is not None:
            conditions.append(SQL("creative_id = %s"))
            params.append(creative_id)

        where_clause = SQL("")
        if conditions:
            where_clause = (
                SQL(" WHERE ")
                + SQL(" AND ").join(conditions)
            )

        query = (
            SQL(
                """
                SELECT
                    event_date::text AS event_date,
                    advertiser_id,
                    creative_id,
                    impressions,
                    total_spend_cny,
                    average_clearing_cpm,
                    clicks
                FROM {}.creative_daily
                """
            ).format(Identifier(self.schema))
            + where_clause
            + SQL(
                " ORDER BY event_date, advertiser_id, creative_id"
            )
        )

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(
                query,
                params,
            ).fetchall()

        return rows

    def replace_traffic_quality_daily(
        self,
        rows: list[dict],
    ) -> None:
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    Identifier(self.schema)
                )
            )

            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.traffic_quality_daily (
                        event_date DATE NOT NULL,
                        total_events BIGINT NOT NULL,
                        valid_events BIGINT NOT NULL,
                        warning_events BIGINT NOT NULL,
                        warning_rate DOUBLE PRECISION NOT NULL,
                        quarantined_events BIGINT NOT NULL
                    )
                    """
                ).format(Identifier(self.schema))
            )

            conn.execute(
                SQL("TRUNCATE TABLE {}.traffic_quality_daily").format(
                    Identifier(self.schema)
                )
            )

            cursor = conn.cursor()
            cursor.executemany(
                SQL(
                    """
                    INSERT INTO {}.traffic_quality_daily (
                        event_date,
                        total_events,
                        valid_events,
                        warning_events,
                        warning_rate,
                        quarantined_events
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                ).format(Identifier(self.schema)),
                [
                    (
                        row["event_date"],
                        row["total_events"],
                        row["valid_events"],
                        row["warning_events"],
                        row["warning_rate"],
                        row["quarantined_events"],
                    )
                    for row in rows
                ],
            )

    def list_traffic_quality_daily(
        self,
        event_date: str | None = None,
    ) -> list[dict]:
        conditions = []
        params = []

        if event_date is not None:
            conditions.append(SQL("event_date = %s"))
            params.append(event_date)

        where_clause = SQL("")
        if conditions:
            where_clause = (
                SQL(" WHERE ")
                + SQL(" AND ").join(conditions)
            )

        query = (
            SQL(
                """
                SELECT
                    event_date::text AS event_date,
                    total_events,
                    valid_events,
                    warning_events,
                    warning_rate,
                    quarantined_events
                FROM {}.traffic_quality_daily
                """
            ).format(Identifier(self.schema))
            + where_clause
            + SQL(" ORDER BY event_date")
        )

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(
                query,
                params,
            ).fetchall()

        return rows
