from datetime import datetime
from pathlib import Path

import pandas as pd


PARQUET_ROOT = Path(
    "data/parquet"
)


def save_event_to_parquet(event):
    """
    Save one valid event to date-partitioned Parquet storage.
    """

    event_timestamp = event.get(
        "event_timestamp"
    )

    event_date = datetime.fromisoformat(
        event_timestamp
    ).date()

    partition_folder = (
        PARQUET_ROOT
        / f"year={event_date.year}"
        / f"month={event_date.month:02d}"
        / f"day={event_date.day:02d}"
    )

    partition_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    event_id = event.get(
        "event_id",
        "unknown",
    )

    output_file = (
        partition_folder
        / f"{event_id}.parquet"
    )

    dataframe = pd.DataFrame(
        [event]
    )

    dataframe.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
    )

    return output_file