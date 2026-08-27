"""Refresh Supabase Postgres serving tables from Gold aggregates."""


class ServingRefreshJob:
    def __init__(self, gold_backend, serving_store):
        self.gold_backend = gold_backend
        self.serving_store = serving_store

    def run(self) -> dict[str, int]:
        advertiser_rows = [
            row.asDict()
            for row in self.gold_backend.read_advertiser_daily().collect()
        ]

        creative_rows = [
            row.asDict()
            for row in self.gold_backend.read_creative_daily().collect()
        ]

        quality_rows = [
            row.asDict()
            for row in self.gold_backend.read_traffic_quality_daily().collect()
        ]

        self.serving_store.replace_advertiser_daily(advertiser_rows)
        self.serving_store.replace_creative_daily(creative_rows)
        self.serving_store.replace_traffic_quality_daily(quality_rows)

        return {
            "advertiser_daily": len(advertiser_rows),
            "creative_daily": len(creative_rows),
            "traffic_quality_daily": len(quality_rows),
        }
