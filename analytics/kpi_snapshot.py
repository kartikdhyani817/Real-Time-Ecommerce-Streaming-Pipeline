import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from analytics.kpi_engine import (
    PARQUET_PATTERN,
)


OUTPUT_FILE = Path(
    "data/processed/latest_kpis.json"
)


def create_kpi_snapshot():
    """
    Calculate and save current KPI state.
    """

    parquet_files = list(
        Path("data/parquet").rglob(
            "*.parquet"
        )
    )

    if not parquet_files:

        print(
            "No Parquet events available."
        )

        return None

    connection = duckdb.connect()

    result = connection.execute(
        f"""
        SELECT
            COUNT(*) AS total_orders,

            COALESCE(
                SUM(quantity),
                0
            ) AS total_items,

            COALESCE(
                ROUND(
                    SUM(total_amount),
                    2
                ),
                0
            ) AS total_revenue,

            COALESCE(
                ROUND(
                    AVG(total_amount),
                    2
                ),
                0
            ) AS average_order_value

        FROM read_parquet(
            '{PARQUET_PATTERN}',
            union_by_name = true
        )
        """
    ).fetchone()

    connection.close()

    snapshot = {
        "total_orders": int(
            result[0]
        ),

        "total_items": int(
            result[1]
        ),

        "total_revenue": float(
            result[2]
        ),

        "average_order_value": float(
            result[3]
        ),

        "updated_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT_FILE.write_text(
        json.dumps(
            snapshot,
            indent=4,
        ),
        encoding="utf-8",
    )

    print(
        "KPI snapshot updated successfully."
    )

    return snapshot


if __name__ == "__main__":

    result = create_kpi_snapshot()

    if result:

        print(
            json.dumps(
                result,
                indent=4,
            )
        )