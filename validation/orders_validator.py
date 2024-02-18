import pandas as pd
from great_expectations.dataset import PandasDataset
from great_expectations.expectations.core import ExpectColumnValuesToNotBeNull
from sqlalchemy import create_engine


def run():
    engine = create_engine("clickhouse+native://default@localhost:9000/ecommerce")

    try:
        df = pd.read_sql("SELECT price FROM orders", engine)
        dataset = PandasDataset(df)
        expectation = ExpectColumnValuesToNotBeNull(column="order_date")
        results = dataset.validate(expectations=[expectation])

        print(results)
    except Exception as e:
        print(f"Validation failed with error: {e}")

if __name__ == "__main__":
    run()
