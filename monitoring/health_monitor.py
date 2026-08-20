from datetime import datetime, timezone
from pathlib import Path

import duckdb

from utils.logger import setup_logger


logger = setup_logger(
    "HealthMonitor"
)


PARQUET_ROOT = Path(
    "data/parquet"
)

PROCESSED_ROOT = Path(
    "data/processed"
)

LOG_ROOT = Path(
    "logs"
)

PARQUET_PATTERN = (
    "data/parquet/**/*.parquet"
)


def check_parquet_storage():

    files = list(
        PARQUET_ROOT.rglob(
            "*.parquet"
        )
    )

    return {
        "status": (
            "HEALTHY"
            if files
            else "NO DATA"
        ),
        "files": len(files),
    }


def check_duckdb():

    files = list(
        PARQUET_ROOT.rglob(
            "*.parquet"
        )
    )

    if not files:

        return {
            "status": "NO DATA",
            "events": 0,
        }

    connection = duckdb.connect()

    try:

        result = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet(
                '{PARQUET_PATTERN}',
                union_by_name = true
            )
            """
        ).fetchone()

        return {
            "status": "HEALTHY",
            "events": int(
                result[0]
            ),
        }

    except Exception as error:

        logger.exception(
            f"DuckDB health check failed: "
            f"{error}"
        )

        return {
            "status": "ERROR",
            "events": 0,
        }

    finally:

        connection.close()


def check_logs():

    pipeline_log = (
        LOG_ROOT
        / "pipeline.log"
    )

    error_log = (
        LOG_ROOT
        / "errors.log"
    )

    return {
        "pipeline_log": (
            pipeline_log.exists()
        ),
        "error_log": (
            error_log.exists()
        ),
    }


def run_health_check():

    print("=" * 65)
    print("E-COMMERCE PIPELINE HEALTH MONITOR")
    print("=" * 65)

    timestamp = datetime.now(
        timezone.utc
    )

    print(
        f"\nCheck Time: "
        f"{timestamp.isoformat()}"
    )

    parquet = (
        check_parquet_storage()
    )

    analytics = (
        check_duckdb()
    )

    logs = check_logs()

    print("\nSTORAGE")
    print("-" * 65)

    print(
        f"Parquet Status : "
        f"{parquet['status']}"
    )

    print(
        f"Parquet Files  : "
        f"{parquet['files']}"
    )

    print("\nANALYTICS")
    print("-" * 65)

    print(
        f"DuckDB Status  : "
        f"{analytics['status']}"
    )

    print(
        f"Stored Events  : "
        f"{analytics['events']}"
    )

    print("\nLOGGING")
    print("-" * 65)

    print(
        f"Pipeline Log   : "
        f"{'AVAILABLE' if logs['pipeline_log'] else 'MISSING'}"
    )

    print(
        f"Error Log      : "
        f"{'AVAILABLE' if logs['error_log'] else 'MISSING'}"
    )

    print("\n" + "=" * 65)

    logger.info(
        f"Health check completed | "
        f"parquet={parquet['status']} | "
        f"duckdb={analytics['status']} | "
        f"events={analytics['events']}"
    )


if __name__ == "__main__":

    run_health_check()