# E-COMMERCE Data Pipeline
`MySQL` -> `Kafka` -> `ClickHouse`

### Setup Environment
Create new `Python` environment with `Conda`
```sh
brew install --cask miniforge
brew install pipx
conda create -n ecommerce python=3.9
conda activate ecommerce
```

### Poetry
Using [Poetry](https://python-poetry.org/), for packaging and dependency management
- command: `poetry install`

### Run Docker
This [docker-compose](docker-compose.yml) file, setup the needed environment in `Docker`
- command: `docker compose -f docker-compose.yml -p ecommerce up`
- Cluster contains:
    + `Zookeeper` node
    + 2 `Kafka` brokers nodes
    + `MySQL` node
    + `Clickhouse` node

### Setup Clusters and Data
- command: `poetry run python setup/setup.py`
- includes:
  + Create `MySQL` schema and tables.
  + Create `Kafka` topics.
  + Create `ClickHouse` schema and tables.
  + Generate dummy data into `MySQL` tables.

### Run Workers
[Faust](https://faust.readthedocs.io/en/latest/) stream processing library is used to build both pipelines
- Data Producers:
    + Scheduled process for each `MySQL` table to push new data into `Kafka` by selecting all new records with `id` greater than the last seen one
    + command: `poetry run python workers/kafka_producers.py worker -l info`
- Data Consumers: 
    + Agent subscribed to each `Kafka` topic, and push nre records into `ClickHouse`.
    + duplicates and updated data are not written to ClickHouse
    + command: `poetry run python workers/kafka_consumers.py worker -l info`

### Data De-normalization
Simple notebook to explain the concept, this could be added into an `Airflow` DAG that runs on daily basis
- [denormalize_orders](notebooks/denormalize_orders.ipynb)

### Data Validate
Using [great-expectations](https://legacy.docs.greatexpectations.io/en/0.13.8/intro.html) to connect into `Click House` data source and run suite of expectations
- commands: 
    + `poetry run python validation/orders_validator.py`
    + command: `poetry run python validation/orders_validator.py`
- This validation is conceptual to present the idea, properly not working.

### Local Test
- Verify data in `MySQL`: 
    + `mysql --host=127.0.0.1 --port=3306 --user root -p`
- Verify data in `Kafka` topic: 
    + `kafka-console-consumer --bootstrap-server localhost:9092 --topic customers --from-beginning`
- Verify data in `ClickHouse`: 
    + `curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20ecommerce.customers'`
