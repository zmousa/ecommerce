from clickhouse_driver import Client

# Define ClickHouse connection parameters
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000

# Define schema and table creation queries
schema_queries = [
    """
    CREATE DATABASE IF NOT EXISTS ecommerce
    """,
    """
    CREATE TABLE IF NOT EXISTS ecommerce.products (
        id Int64,
        name String,
        price Int64
    ) ENGINE = MergeTree()
    ORDER BY id
    """,
    """
    CREATE TABLE IF NOT EXISTS ecommerce.customers (
        id Int64,
        name String,
        email String
    ) ENGINE = MergeTree()
    ORDER BY id
    """,
    """
    CREATE TABLE IF NOT EXISTS ecommerce.orders (
        id Int64,
        customer_id Int64,
        order_date String
    ) ENGINE = MergeTree()
    ORDER BY id
    """,
    """
    CREATE TABLE IF NOT EXISTS ecommerce.order_items (
        id Int64,
        order_id Int64,
        product_id Int64,
        quantity Int32
    ) ENGINE = MergeTree()
    ORDER BY order_id
    """
]

# Function to initialize ClickHouse database with schema and tables
def init_clickhouse_database():
    try:
        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        #client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DATABASE)
        for query in schema_queries:
            client.execute(query)
        print("ClickHouse database initialized successfully.")
    except Exception as e:
        print(f"Error initializing ClickHouse database: {str(e)}")

if __name__ == '__main__':
    init_clickhouse_database()
