from pathlib import Path

import pandas as pd


PARQUET_ROOT = Path(
    "data/parquet"
)


def load_all_events():
    """
    Read all stored Parquet events.
    """

    parquet_files = list(
        PARQUET_ROOT.rglob(
            "*.parquet"
        )
    )

    if not parquet_files:

        print(
            "No Parquet files found."
        )

        return pd.DataFrame()

    dataframes = []

    for file in parquet_files:

        dataframe = pd.read_parquet(
            file
        )

        dataframes.append(
            dataframe
        )

    combined_df = pd.concat(
        dataframes,
        ignore_index=True,
    )

    return combined_df


if __name__ == "__main__":

    df = load_all_events()

    if not df.empty:

        print("=" * 60)
        print("Stored Streaming Events")
        print("=" * 60)

        print(
            f"\nTotal Events: {len(df)}"
        )

        print(
            "\nColumns:"
        )

        print(
            df.columns.tolist()
        )

        print(
            "\nSample Events:\n"
        )

        print(
            df.head().to_string(
                index=False
            )
        )