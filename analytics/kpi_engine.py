import time
from pathlib import Path

import duckdb


PARQUET_PATTERN = "data/parquet/**/*.parquet"

REFRESH_SECONDS = 5


def parquet_data_exists():
    """
    Check whether streaming Parquet files exist.
    """

    parquet_files = list(
        Path("data/parquet").rglob(
            "*.parquet"
        )
    )

    return len(parquet_files) > 0


def get_connection():
    """
    Create an in-memory DuckDB connection.
    """

    return duckdb.connect()


def get_overall_kpis(connection):
    """
    Calculate overall real-time KPIs.
    """

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

    return {
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
    }


def get_top_product(connection):
    """
    Get highest revenue generating product.
    """

    result = connection.execute(
        f"""
        SELECT
            product_name,
            SUM(quantity) AS units,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue

        FROM read_parquet(
            '{PARQUET_PATTERN}',
            union_by_name = true
        )

        GROUP BY product_name

        ORDER BY revenue DESC

        LIMIT 1
        """
    ).fetchone()

    if result is None:

        return {
            "product_name": "N/A",
            "units": 0,
            "revenue": 0,
        }

    return {
        "product_name": result[0],
        "units": int(result[1]),
        "revenue": float(result[2]),
    }


def get_top_category(connection):
    """
    Get highest revenue generating category.
    """

    result = connection.execute(
        f"""
        SELECT
            category,
            COUNT(*) AS orders,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue

        FROM read_parquet(
            '{PARQUET_PATTERN}',
            union_by_name = true
        )

        GROUP BY category

        ORDER BY revenue DESC

        LIMIT 1
        """
    ).fetchone()

    if result is None:

        return {
            "category": "N/A",
            "orders": 0,
            "revenue": 0,
        }

    return {
        "category": result[0],
        "orders": int(result[1]),
        "revenue": float(result[2]),
    }


def get_payment_leader(connection):
    """
    Find most frequently used payment method.
    """

    result = connection.execute(
        f"""
        SELECT
            payment_method,
            COUNT(*) AS usage_count

        FROM read_parquet(
            '{PARQUET_PATTERN}',
            union_by_name = true
        )

        GROUP BY payment_method

        ORDER BY usage_count DESC

        LIMIT 1
        """
    ).fetchone()

    if result is None:

        return {
            "payment_method": "N/A",
            "usage_count": 0,
        }

    return {
        "payment_method": result[0],
        "usage_count": int(result[1]),
    }


def print_dashboard(
    kpis,
    top_product,
    top_category,
    payment_leader,
):
    """
    Display current KPI values in terminal.
    """

    print("\033c", end="")

    print("=" * 70)
    print("REAL-TIME E-COMMERCE KPI ENGINE")
    print("=" * 70)

    print(
        f"\nTotal Orders        : "
        f"{kpis['total_orders']:,}"
    )

    print(
        f"Total Items Sold    : "
        f"{kpis['total_items']:,}"
    )

    print(
        f"Total Revenue       : "
        f"₹{kpis['total_revenue']:,.2f}"
    )

    print(
        f"Average Order Value : "
        f"₹{kpis['average_order_value']:,.2f}"
    )

    print("\n" + "-" * 70)

    print(
        f"Top Product         : "
        f"{top_product['product_name']}"
    )

    print(
        f"Top Product Revenue : "
        f"₹{top_product['revenue']:,.2f}"
    )

    print(
        f"Top Category        : "
        f"{top_category['category']}"
    )

    print(
        f"Top Category Revenue: "
        f"₹{top_category['revenue']:,.2f}"
    )

    print(
        f"Most Used Payment   : "
        f"{payment_leader['payment_method']}"
    )

    print("\n" + "-" * 70)

    print(
        f"Refreshing every "
        f"{REFRESH_SECONDS} seconds..."
    )

    print(
        "Press Ctrl+C to stop."
    )


def start_kpi_engine():
    """
    Continuously refresh KPI values.
    """

    print(
        "Starting Real-Time KPI Engine..."
    )

    try:

        while True:

            if not parquet_data_exists():

                print(
                    "\nWaiting for Parquet "
                    "streaming data..."
                )

                time.sleep(
                    REFRESH_SECONDS
                )

                continue

            connection = get_connection()

            try:

                kpis = get_overall_kpis(
                    connection
                )

                top_product = (
                    get_top_product(
                        connection
                    )
                )

                top_category = (
                    get_top_category(
                        connection
                    )
                )

                payment_leader = (
                    get_payment_leader(
                        connection
                    )
                )

                print_dashboard(
                    kpis,
                    top_product,
                    top_category,
                    payment_leader,
                )

            finally:

                connection.close()

            time.sleep(
                REFRESH_SECONDS
            )

    except KeyboardInterrupt:

        print(
            "\n\nReal-Time KPI Engine stopped."
        )


if __name__ == "__main__":

    start_kpi_engine()