import faust
from faust.types import ProcessingGuarantee
import json
from clickhouse_driver import Client
from models import ProductEvent, CustomerEvent, OrderEvent, OrderItemEvent


# Faust app configuration
app = faust.App(
    'kafka_to_clickhouse', 
    broker='kafka://localhost:9092',
    processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE
)

# ClickHouse configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
ch_client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

# Kafka topics to ClickHouse tables mapping
topics_to_ch_tables = {
    'products': 'products',
    'customers': 'customers',
    'orders': 'orders',
    'order_items': 'order_items',
}

# Kafka topic to Faust Record mapping
topic_to_record = {
    'products': ProductEvent,
    'customers': CustomerEvent,
    'orders': OrderEvent,
    'order_items': OrderItemEvent,
}

@app.agent(app.topic('products'))
async def process_products(stream):
    async for event in stream:
        await action(event, 'products', topics_to_ch_tables['products'])

@app.agent(app.topic('customers'))
async def process_customers(stream):
    async for event in stream:
        await action(event, 'customers', topics_to_ch_tables['customers'])

@app.agent(app.topic('orders'))
async def process_orders(stream):
    async for event in stream:
        await action(event, 'orders', topics_to_ch_tables['orders'])

@app.agent(app.topic('order_items'))
async def process_order_items(stream):
    async for event in stream:
        await action(event, 'order_items', topics_to_ch_tables['order_items'])

async def action(event, kafka_topic, ch_table):
    try:
        # Deserialize JSON event to a dictionary
        event_data = json.loads(event) if isinstance(event, (str, bytes)) else event.asdict()
        event_data.pop('__faust', None)  # Remove the __faust metadata if present
        print(f'event_data: {event_data}')
        
        # Get the Faust record class based on the topic
        record = topic_to_record[kafka_topic](**event_data)
        
        # Build ClickHouse select query to check if the record already exists
        select_query = f"SELECT * FROM ecommerce.{ch_table} WHERE id = {record.id}"
        
        # Execute the select query
        existing_record = ch_client.execute(select_query)
        
        if existing_record:
            # Record already exists, ignore
            pass
        else:
            # Build ClickHouse insert query
            query = f"INSERT INTO ecommerce.{ch_table} (*) VALUES"
            values = tuple(record.asdict().values())
            ch_client.execute(query, [values])
    except json.JSONDecodeError as e:
        print(f'ERROR: Could not decode JSON: {e}')
    except Exception as e:
        print(f'ERROR: An error occurred: {e}')

if __name__ == '__main__':
    app.main()
