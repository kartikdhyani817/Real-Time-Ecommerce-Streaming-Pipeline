from pathlib import Path

import duckdb


DATABASE_PATH = Path(
    "data/ecommerce_analytics.duckdb"
)

PARQUET_PATTERN = (
    "data/parquet/**/*.parquet"
)


def create_database():
    """
    Create DuckDB database and analytics view
    over the streaming Parquet files.
    """

    DATABASE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = duckdb.connect(
        str(DATABASE_PATH)
    )

    parquet_files = list(
        Path("data/parquet").rglob(
            "*.parquet"
        )
    )

    if not parquet_files:
        print(
            "No Parquet files found."
        )

        connection.close()
        return False

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW ecommerce_events AS

        SELECT *
        FROM read_parquet(
            '{PARQUET_PATTERN}',
            union_by_name = true
        )
        """
    )

    connection.close()

    print(
        "DuckDB analytics database created successfully."
    )

    return True


def run_analytics():
    """
    Execute SQL analytics on e-commerce events.
    """

    connection = duckdb.connect(
        str(DATABASE_PATH)
    )

    print("\n" + "=" * 65)
    print("REAL-TIME E-COMMERCE ANALYTICS")
    print("=" * 65)

    # -----------------------------------------
    # Overall KPIs
    # -----------------------------------------

    print("\n1. OVERALL KPIs")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_items,
            ROUND(
                SUM(total_amount),
                2
            ) AS total_revenue,
            ROUND(
                AVG(total_amount),
                2
            ) AS average_order_value
        FROM ecommerce_events
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    # -----------------------------------------
    # Revenue by Category
    # -----------------------------------------

    print("\n2. REVENUE BY CATEGORY")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            category,
            COUNT(*) AS orders,
            SUM(quantity) AS items_sold,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue
        FROM ecommerce_events
        GROUP BY category
        ORDER BY revenue DESC
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    # -----------------------------------------
    # Top Products
    # -----------------------------------------

    print("\n3. TOP PRODUCTS")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            product_name,
            COUNT(*) AS orders,
            SUM(quantity) AS quantity_sold,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue
        FROM ecommerce_events
        GROUP BY product_name
        ORDER BY revenue DESC
        LIMIT 5
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    # -----------------------------------------
    # Payment Methods
    # -----------------------------------------

    print("\n4. PAYMENT METHOD ANALYSIS")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            payment_method,
            COUNT(*) AS orders,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue
        FROM ecommerce_events
        GROUP BY payment_method
        ORDER BY orders DESC
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    # -----------------------------------------
    # Order Status
    # -----------------------------------------

    print("\n5. ORDER STATUS")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            order_status,
            COUNT(*) AS orders,
            ROUND(
                SUM(total_amount),
                2
            ) AS order_value
        FROM ecommerce_events
        GROUP BY order_status
        ORDER BY orders DESC
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    # -----------------------------------------
    # Top Cities
    # -----------------------------------------

    print("\n6. TOP CITIES")
    print("-" * 65)

    result = connection.execute(
        """
        SELECT
            city,
            country,
            COUNT(*) AS orders,
            ROUND(
                SUM(total_amount),
                2
            ) AS revenue
        FROM ecommerce_events
        GROUP BY
            city,
            country
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).df()

    print(
        result.to_string(
            index=False
        )
    )

    connection.close()


if __name__ == "__main__":

    database_created = (
        create_database()
    )

    if database_created:

        run_analytics()