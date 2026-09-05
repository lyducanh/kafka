# Spring Boot Kafka Streams Stock Statistics

A Spring Boot 3 implementation of the Kafka Streams stock statistics aggregation example.

## Features

1. **Automatic Topic Provisioning**: Automatically configures topics `stocks` and `stockstats-output` with 3 partitions and replication factor 3 for the local cluster defined in `docker-compose.yml`.
2. **Built-in Mock Trade Generator**: Background scheduler simulates ask orders for 10 tickers (`MMM`, `ABT`, `ABBV`, `ACN`, `ATVI`, `AYI`, `ADBE`, `AAP`, `AES`, `AET`) with Gaussian price fluctuations.
3. **Kafka Streams Aggregation**:
   - 5-second hopping window advancing every 1 second.
   - Calculates number of trades, sum, minimum, maximum, and average price per ticker.
   - Publishes aggregated results to `stockstats-output`.
4. **Spring Kafka Listener**: Automatically consumes from `stockstats-output` and logs live statistics to the console.
5. **REST API**:
   - `GET /api/stocks/status` - Check generator status and aggregation count
   - `POST /api/stocks/generator/toggle` - Start or pause the mock trade generator
   - `POST /api/stocks/trade` - Send a custom trade
   - `GET /api/stocks/stats` - View recent window aggregations

## Prerequisites

Start the Kafka cluster defined in `docker-compose.yml`:
```bash
docker compose up -d
```

## How to Run

### In IntelliJ IDEA:
1. Open or import the `spring-kafka-stockstats` folder as a Maven project.
2. Run `SpringKafkaStockStatsApplication` using the green Play button.

### Using Maven:
```bash
mvn spring-boot:run
```

## REST Endpoints Examples

### Check Status
```bash
curl http://localhost:8082/api/stocks/status
```

### Pause/Resume Generator
```bash
curl -X POST http://localhost:8082/api/stocks/generator/toggle
```

### Send a Custom Trade
```bash
curl -X POST http://localhost:8082/api/stocks/trade \
  -H "Content-Type: application/json" \
  -d '{"type":"ASK","ticker":"AAPL","price":175.50,"size":50}'
```

### View Recent Aggregated Stats
```bash
curl http://localhost:8082/api/stocks/stats
```

## Postman Collection

Import the collection into Postman:
- File location: `postman/Spring_Kafka_StockStats.postman_collection.json` (or in repository root `postman/Spring_Kafka_StockStats.postman_collection.json`)
- Base URL variable: `{{baseUrl}}` (defaults to `http://localhost:8082`)

