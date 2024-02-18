import pandas as pd
from sqlalchemy import create_engine

# MySQL connection settings
db_user = 'root'
db_password = 'root'
db_host = 'localhost'
db_port = '3306'

# Create a MySQL database connection
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}')

# Define function to create schema and tables
def create_schema_and_tables():
    with engine.connect() as connection:
        # Create schema
        connection.execute("CREATE SCHEMA IF NOT EXISTS ecommerce")

        # Create tables within schema
        connection.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce.products (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                price INT
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce.customers (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255)
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce.orders (
                id INT PRIMARY KEY,
                customer_id INT,
                order_date DATE,
                FOREIGN KEY (customer_id) REFERENCES ecommerce.customers(id)
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce.order_items (
                id INT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                FOREIGN KEY (order_id) REFERENCES ecommerce.orders(id),
                FOREIGN KEY (product_id) REFERENCES ecommerce.products(id)
            )
        """)

if __name__ == "__main__":
    # Create schema and tables
    create_schema_and_tables()
    print("Schema created successfully.")
