from kafka_setup import create_topics as kafka_setup_main
from mysql_setup import create_schema_and_tables as mysql_setup_main
from clickhouse_setup import init_clickhouse_database as clickhouse_setup_main
from mysql_generate_data import main as mysql_generate_data_main

def run_all():
    kafka_setup_main()
    mysql_setup_main()
    clickhouse_setup_main()
    mysql_generate_data_main()

if __name__ == "__main__":
    run_all()
