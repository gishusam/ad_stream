"""Postgres persistence for AdStream pipeline observability metrics."""

import json
import re

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import Identifier, SQL


_IDENTIFIER = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*$"
)


class PostgresPipelineMetricsStore:
    def __init__(
        self,
        database_url: str,
        schema: str = "serving",
    ):
        if not _IDENTIFIER.fullmatch(schema):
            raise ValueError(
                f"Invalid schema name: {schema!r}"
            )

        self.database_url = database_url
        self.schema = schema

    def _ensure_table(self, conn) -> None:
        conn.execute(
            SQL(
                "CREATE SCHEMA IF NOT EXISTS {}"
            ).format(
                Identifier(self.schema)
            )
        )

        conn.execute(
            SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.pipeline_stage_runs (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    duration_ms DOUBLE PRECISION NOT NULL,
                    result_json JSONB NOT NULL,
                    error_type TEXT,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(
                Identifier(self.schema)
            )
        )

    def record_stage(
        self,
        record: dict,
    ) -> None:
        with psycopg.connect(
            self.database_url
        ) as conn:
            self._ensure_table(conn)

            conn.execute(
                SQL(
                    """
                    INSERT INTO {}.pipeline_stage_runs (
                        run_id,
                        stage,
                        status,
                        duration_ms,
                        result_json,
                        error_type
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s::jsonb,
                        %s
                    )
                    """
                ).format(
                    Identifier(self.schema)
                ),
                (
                    record.get("run_id"),
                    record["stage"],
                    record["status"],
                    record["duration_ms"],
                    json.dumps(
                        record.get(
                            "result",
                            {},
                        )
                    ),
                    record.get(
                        "error_type"
                    ),
                ),
            )

    def list_stage_runs(
        self,
        run_id: str,
    ) -> list[dict]:
        """Return the latest observed state of each stage for a run."""

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as conn:
            self._ensure_table(conn)

            rows = conn.execute(
                SQL(
                    """
                    WITH ranked_stage_states AS (
                        SELECT
                            id,
                            run_id,
                            stage,
                            status,
                            duration_ms,
                            result_json,
                            error_type,
                            recorded_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY run_id, stage
                                ORDER BY recorded_at DESC, id DESC
                            ) AS stage_rank
                        FROM {}.pipeline_stage_runs
                        WHERE run_id = %s
                    )
                    SELECT
                        run_id,
                        stage,
                        status,
                        duration_ms,
                        result_json AS result,
                        error_type,
                        recorded_at
                    FROM ranked_stage_states
                    WHERE stage_rank = 1
                    ORDER BY
                        CASE stage
                            WHEN 'silver' THEN 1
                            WHEN 'gold' THEN 2
                            WHEN 'quality' THEN 3
                            WHEN 'serving' THEN 4
                            ELSE 99
                        END,
                        stage
                    """
                ).format(
                    Identifier(self.schema)
                ),
                (run_id,),
            ).fetchall()

        return rows

    def list_recent_runs(
        self,
        limit: int = 10,
    ) -> list[dict]:
        """Summarize runs using the latest state of each observed stage."""

        with psycopg.connect(
            self.database_url,
            row_factory=dict_row,
        ) as conn:
            self._ensure_table(conn)

            rows = conn.execute(
                SQL(
                    """
                    WITH ranked_stage_states AS (
                        SELECT
                            id,
                            run_id,
                            stage,
                            status,
                            duration_ms,
                            recorded_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY run_id, stage
                                ORDER BY recorded_at DESC, id DESC
                            ) AS stage_rank
                        FROM {}.pipeline_stage_runs
                        WHERE run_id IS NOT NULL
                    ),
                    latest_stage_states AS (
                        SELECT
                            run_id,
                            stage,
                            status,
                            duration_ms,
                            recorded_at
                        FROM ranked_stage_states
                        WHERE stage_rank = 1
                    ),
                    run_summary AS (
                        SELECT
                            run_id,
                            CASE
                                WHEN BOOL_OR(status = 'failed')
                                    THEN 'failed'
                                ELSE 'success'
                            END AS status,
                            SUM(duration_ms) AS duration_ms,
                            MAX(recorded_at) AS recorded_at
                        FROM latest_stage_states
                        GROUP BY run_id
                    )
                    SELECT
                        run_id,
                        status,
                        duration_ms,
                        recorded_at
                    FROM run_summary
                    ORDER BY recorded_at DESC
                    LIMIT %s
                    """
                ).format(
                    Identifier(self.schema)
                ),
                (limit,),
            ).fetchall()

        return rows
