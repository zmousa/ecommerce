import pandas as pd
from sqlalchemy import create_engine

# MySQL connection settings
db_user = 'root'
db_password = 'root'
db_host = 'localhost'
db_port = '3306'

# Create a MySQL database connection
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}')

# Define dummy data generation functions
def generate_products(num_records):
    return pd.DataFrame({
        'id': range(1, num_records + 1),
        'name': [f'Product_{i}' for i in range(1, num_records + 1)],
        'price': [(i + 1) * 10 for i in range(num_records)],
    })

def generate_customers(num_records):
    return pd.DataFrame({
        'id': range(1, num_records + 1),
        'name': [f'Customer_{i}' for i in range(1, num_records + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, num_records + 1)],
    })

def generate_orders(num_records, num_customers):
    return pd.DataFrame({
        'id': range(1, num_records + 1),
        'customer_id': [i % num_customers + 1 for i in range(num_records)],
        'order_date': pd.date_range(start='2022-01-01', periods=num_records),
    })

def generate_order_items(num_records, num_products, num_orders):
    return pd.DataFrame({
        'id': range(1, num_records + 1),
        'order_id': [i % num_orders + 1 for i in range(num_records)],
        'product_id': [(i % num_products) + 1 for i in range(num_records)],
        'quantity': [i + 1 for i in range(num_records)],
    })

# Define function to insert data into MySQL tables
def insert_data(dataframe, table_name):
    dataframe.to_sql(table_name, con=engine, schema='ecommerce', if_exists='append', index=False)
    
    
def main():
    # Generate dummy data
    num_products = 10
    num_customers = 5
    num_orders = 20
    num_order_items = 50

    products_df = generate_products(num_products)
    customers_df = generate_customers(num_customers)
    orders_df = generate_orders(num_orders, num_customers)
    order_items_df = generate_order_items(num_order_items, num_products, num_orders)

    # Insert data into MySQL tables
    insert_data(products_df, 'products')
    insert_data(customers_df, 'customers')
    insert_data(orders_df, 'orders')
    insert_data(order_items_df, 'order_items')

    print("Dummy data inserted successfully.")

if __name__ == "__main__":
    main()
