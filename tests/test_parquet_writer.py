from pathlib import Path

import pandas as pd

from producer.event_generator import generate_event
import scripts.parquet_writer as parquet_writer


def test_parquet_file_created(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        parquet_writer,
        "PARQUET_ROOT",
        tmp_path,
    )

    event = generate_event()

    output_file = (
        parquet_writer
        .save_event_to_parquet(
            event
        )
    )

    assert output_file.exists()


def test_parquet_file_readable(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        parquet_writer,
        "PARQUET_ROOT",
        tmp_path,
    )

    event = generate_event()

    output_file = (
        parquet_writer
        .save_event_to_parquet(
            event
        )
    )

    df = pd.read_parquet(
        output_file
    )

    assert not df.empty

    assert (
        df.iloc[0]["order_id"]
        == event["order_id"]
    )


def test_date_partition_created(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        parquet_writer,
        "PARQUET_ROOT",
        tmp_path,
    )

    event = generate_event()

    output_file = (
        parquet_writer
        .save_event_to_parquet(
            event
        )
    )

    path_string = str(
        output_file
    )

    assert "year=" in path_string
    assert "month=" in path_string
    assert "day=" in path_string