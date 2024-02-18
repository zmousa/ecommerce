import pandas as pd
from great_expectations.dataset import PandasDataset
from great_expectations.expectations.core import ExpectColumnMedianToBeBetween
from sqlalchemy import create_engine


def run():
    engine = create_engine("clickhouse+native://default@localhost:9000/ecommerce")

    try:
        df = pd.read_sql("SELECT price FROM products", engine)
        dataset = PandasDataset(df)
        expectation = ExpectColumnMedianToBeBetween(column="price", min_value=0, max_value=1000)
        results = dataset.validate(expectations=[expectation])
        
        print(results)
    except Exception as e:
        print(f"Validation failed with error: {e}")

if __name__ == "__main__":
    run()
