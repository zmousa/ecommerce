import faust
import pymysql
import json
from models import ProductEvent, CustomerEvent, OrderEvent, OrderItemEvent

app = faust.App('mysql_to_kafka', broker='kafka://localhost:9092')

mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'db': 'ecommerce'
}

globals()["products_last_processed_id"] = 0
globals()["customers_last_processed_id"] = 0
globals()["orders_last_processed_id"] = 0
globals()["order_items_last_processed_id"] = 0

async def get_and_send_new_rows(table_name, event_class, topic_name):
    last_processed_id = f"{table_name}_last_processed_id"
    
    connection = pymysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'], 
        password=mysql_config['password'],
        db=mysql_config['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        # Query for new rows
        sql = f"SELECT * FROM {table_name} WHERE `id` > %s ORDER BY `id` ASC"
        cursor.execute(sql, (globals()[last_processed_id],))
        new_rows = cursor.fetchall()

        for row in new_rows:
            # Prepare the event data
            event = event_class(**row)
            
            # Send the event to Kafka topic
            await app.send(topic_name, value=event.dumps(serializer='raw'))
            
            # Update the last processed ID
            globals()[last_processed_id] = row['id']

    connection.close()

@app.timer(interval=10.0)
async def produce_products():
    await get_and_send_new_rows('products', ProductEvent, 'products')

@app.timer(interval=10.0)
async def produce_customers():
    await get_and_send_new_rows('customers', CustomerEvent, 'customers')

@app.timer(interval=10.0)
async def produce_orders():
    await get_and_send_new_rows('orders', OrderEvent, 'orders')

@app.timer(interval=10.0)
async def produce_order_items():
    await get_and_send_new_rows('order_items', OrderItemEvent, 'order_items')

if __name__ == '__main__':
    app.main()
