import duckdb
import pandas as pd


def create_test_dataframe():
    return pd.DataFrame(
        {
            "order_id": [
                "ORD-1",
                "ORD-2",
                "ORD-3",
            ],
            "product_name": [
                "Laptop",
                "Phone",
                "Laptop",
            ],
            "category": [
                "Electronics",
                "Electronics",
                "Electronics",
            ],
            "quantity": [
                1,
                2,
                1,
            ],
            "total_amount": [
                75000,
                70000,
                75000,
            ],
            "payment_method": [
                "UPI",
                "Card",
                "UPI",
            ],
        }
    )


def test_duckdb_total_orders():
    df = create_test_dataframe()

    connection = duckdb.connect()

    connection.register(
        "events",
        df,
    )

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM events
        """
    ).fetchone()

    connection.close()

    assert result[0] == 3


def test_duckdb_total_revenue():
    df = create_test_dataframe()

    connection = duckdb.connect()

    connection.register(
        "events",
        df,
    )

    result = connection.execute(
        """
        SELECT SUM(total_amount)
        FROM events
        """
    ).fetchone()

    connection.close()

    assert result[0] == 220000


def test_top_product():
    df = create_test_dataframe()

    connection = duckdb.connect()

    connection.register(
        "events",
        df,
    )

    result = connection.execute(
        """
        SELECT
            product_name,
            SUM(total_amount) AS revenue
        FROM events
        GROUP BY product_name
        ORDER BY revenue DESC
        LIMIT 1
        """
    ).fetchone()

    connection.close()

    assert result[0] == "Laptop"