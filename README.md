# Implementation of the Wikimedia Project in Golang

This project provides a Golang implementation of the Wikimedia project as discussed by [Stephane Maarek](https://github.com/simplesteph) in his [Kafka beginners course](https://www.udemy.com/share/1013hc/). The original Java consumer and producer repositories can be found here:

- [Consumer](https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-consumer-opensearch)
- [Producer](https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-producer-wikimedia)

## Prerequisites

Before you begin, ensure the following software is installed on your system:

- [Docker](https://docs.docker.com/engine/install/)
- [Apache Kafka](https://kafka.apache.org/downloads)
- [Golang](https://go.dev/doc/install)

## Running the Project

### 1. Start Kafka in Kraft Mode

Start Kafka by executing the following command in your terminal:

```bash
docker-compose -f docker-compose-kafka-kraft.yml up --build
```

### 2. Start OpenSearch

Open a new terminal window and execute:

```bash
docker-compose -f docker-compose-opensearch.yml up --build
```

### 3. Create Kafka Topic

Once Kafka is up, create the Kafka topic with the following command:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange --create --partitions 3 --replication-factor 2
```

### 4. Start Wikimedia Kafka Producer

Navigate to the `producer-wikimedia` directory in a new terminal window and run:

```bash
cd producer-wikimedia
go run ./producer.go
```

### 5. Start OpenSearch Kafka Consumer

Navigate to the `consumer-opensearch` directory in another terminal window and run:

```bash
cd consumer-opensearch
go run ./consumer.go
```

## Viewing Consumed Data in OpenSearch Dashboard

To view the consumed data in the OpenSearch dashboard, go to [OpenSearch Dev Tools](http://localhost:5601/app/dev_tools#/console).

- **Username:** admin
- **Password:** Toor1234_

### Retrieve Consumed Data

Execute the following command to retrieve data consumed and sent to OpenSearch by the Kafka consumer:

```bash
GET /wikimedia/_search
```