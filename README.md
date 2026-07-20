# Outbox Table Demo

A Spring Boot demo of the **Transactional Outbox Pattern** using **Debezium Embedded** to capture row changes from MySQL and **Micrometer** to measure end-to-end latency from database commit to consumer receive.

## Architecture

```
[App / LoadGenerator] --INSERT--> [MySQL: outbox_* table]
                                         |
                                  (Debezium CDC via binlog)
                                         |
                            [DebeziumEngine (embedded, in-process)]
                                         |
                            [OutboxEventListener (Consumer)]
                                         |
                            [Micrometer Timer + Counters]
                                         |
                            [Prometheus /actuator/prometheus]
```

The app embeds Debezium directly (no Kafka Connect cluster). Debezium reads MySQL binlog, emits change events as JSON to an in-process consumer, which records `created_at -> receive` latency per table.

## Components

| File | Role |
|------|------|
| `OutboxTableDemoApplication` | Spring Boot entrypoint, provides `Clock` bean. |
| `DebeziumProperties` | Binds `debezium.props[*]` from `application.properties`. |
| `DebeziumEngineConfig` | Builds and runs `DebeziumEngine` as a `SmartLifecycle` bean (auto-start/stop with Spring context, CompletionCallback/ConnectorCallback for visibility). |
| `OutboxEventListener` | `Consumer<ChangeEvent>` that parses Debezium JSON, computes latency, records Timer + counters. |
| `DebeziumHealthIndicator` | Reports `DOWN` at `/actuator/health` when the engine is not running (e.g. died unexpectedly) so orchestrators (K8s) can restart the pod. |
| `LoadGenerator` | Standalone `main()` for load testing. Reads DB credentials from `application.properties`. |

## Prerequisites

- JDK 17+
- MySQL 8.x with binlog enabled (`binlog_format=ROW`, `binlog_row_image=FULL`)
- MySQL `default-time-zone = "+00:00"` set in `mysqld.cnf` (UTC, industry standard for multi-region safety)
- Gradle 8.x (wrapper included)
- SSH access to the MySQL host (the project's `~/.ssh/config` uses `Host ducanh` with `LocalForward 3306 127.0.0.1:3306`)

## Database Setup

Create database `obt` and two outbox tables to compare CDC behaviour with and without row storage:

```sql
CREATE DATABASE IF NOT EXISTS obt;
USE obt;

-- Standard InnoDB outbox. Rows are stored until cleaned up.
-- The stored procedure below inserts and deletes in the same transaction
-- so the binlog captures the INSERT (op=c) without keeping the row.
CREATE TABLE outbox_innodb (
    id            CHAR(36)      NOT NULL PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id  VARCHAR(255) NOT NULL,
    event_type    VARCHAR(255) NOT NULL,
    payload       JSON          NOT NULL,
    created_at    TIMESTAMP(3)  NULL
) ENGINE=InnoDB;

-- BLACKHOLE engine: rows are never stored, but binlog still receives
-- the INSERT events. Ideal for benchmarking CDC throughput without
-- storage overhead. No cleanup needed (there is nothing to delete).
CREATE TABLE outbox_blackhole (
    id            CHAR(36)      NOT NULL PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id  VARCHAR(255) NOT NULL,
    event_type    VARCHAR(255) NOT NULL,
    payload       JSON          NOT NULL,
    created_at    TIMESTAMP(3)  NULL
) ENGINE=BLACKHOLE;
```

The stored procedure for `outbox_innodb` inserts a row and immediately deletes it within the same transaction. MySQL writes both operations to the binlog at commit, so Debezium still sees the INSERT (`op=c`) while the table stays empty. This mimics BLACKHOLE semantics on InnoDB. The trade-off: if Debezium falls behind the binlog retention window and the binlog rotates before the event is read, the event is lost permanently. Use a real cleanup job (e.g. `DELETE WHERE created_at < NOW() - INTERVAL 24 HOUR`) if you need durability guarantees.

```sql
DELIMITER //
CREATE PROCEDURE emit_outbox_innodb(
    IN p_aggregate_type VARCHAR(255),
    IN p_aggregate_id   VARCHAR(255),
    IN p_event_type     VARCHAR(255),
    IN p_payload        JSON,
    IN p_created_at     TIMESTAMP(3)
)
BEGIN
    DECLARE v_id CHAR(36);
    SET v_id = UUID();
    INSERT INTO outbox_innodb (id, aggregate_type, aggregate_id, event_type, payload, created_at)
    VALUES (v_id, p_aggregate_type, p_aggregate_id, p_event_type, p_payload, p_created_at);
    DELETE FROM outbox_innodb WHERE id = v_id;
END //
DELIMITER ;
```

Grant privileges to the `ducanh` user (adjust host as needed):

```sql
CREATE USER 'ducanh'@'localhost' IDENTIFIED BY '<your-password>';
GRANT SELECT, INSERT, UPDATE, DELETE ON obt.* TO 'ducanh'@'localhost';
-- Debezium also needs replication client + replication slave
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'ducanh'@'localhost';
FLUSH PRIVILEGES;
```

## Configuration

All Debezium connector properties live in `src/main/resources/application.properties` under the `debezium.props[*]` prefix. DB credentials are the single source of truth there; `LoadGenerator` reads them from the same file.

Key properties:

```properties
debezium.props[database.hostname]=localhost
debezium.props[database.port]=3306
debezium.props[database.user]=ducanh
debezium.props[database.password]=<your-password>
debezium.props[table.include.list]=obt.outbox_innodb,obt.outbox_blackhole
debezium.props[snapshot.mode]=schema_only
```

Debezium runtime state (offsets, schema history) is written to `.debezium/` (gitignored).

## Run the Application

Start the SSH tunnel (if MySQL is remote):

```bash
ssh ducanh -N
```

In another terminal, start the Spring Boot app:

```bash
./gradlew bootRun
```

The app listens on `:8081` and exposes:

| Endpoint | Description |
|----------|-------------|
| `http://localhost:8081/actuator/health` | Health check |
| `http://localhost:8081/actuator/prometheus` | Prometheus scrape target |
| `http://localhost:8081/actuator/metrics` | Micrometer metrics list |

## Run a Load Test

```bash
# Default: 10 threads x 1000 rows into outbox_blackhole via INSERT
./gradlew loadTest

# Custom: 20 threads, 5000 rows each, into outbox_innodb, use stored procedure
./gradlew loadTest -Pthreads=20 -PperThread=5000 -Ptable=outbox_innodb -PuseSp=true
```

While the load test runs, watch the Spring Boot log and Prometheus metrics to observe latency.

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `outbox.event.latency` | Timer | `connector`, `table` | Latency from `created_at` (app emit time, set by producer) to consumer receive. Publishes p50/p95/p99 and histogram. `connector` = `outbox.consumer.name` (consumer identity). |
| `outbox.event.received` | Counter | `connector`, `table` | Successfully processed `c` (create) events. |
| `outbox.event.skipped` | Counter | `connector`, `table`, `reason` | Skipped events (null value, missing source/after, non-create op, future timestamp, missing created_at). |
| `outbox.event.failed` | Counter | `connector`, `table`, `reason` | Failed events (invalid `created_at` format, exception). |

Example PromQL:

```promql
# p95 latency per table
histogram_quantile(0.95,
  sum by (table, le) (rate(outbox_event_latency_seconds_bucket[5m])))

# throughput per table
sum by (table) (rate(outbox_event_received_total[1m]))

# error ratio
sum by (table) (rate(outbox_event_failed_total[5m]))
  /
sum by (table) (rate(outbox_event_received_total[5m]))
```

## Project Structure

```
src/main/java/com/example/outboxtabledemo/
    OutboxTableDemoApplication.java   # @SpringBootApplication + Clock bean
    DebeziumProperties.java           # @ConfigurationProperties(prefix="debezium")
    DebeziumEngineConfig.java         # SmartLifecycle Debezium engine
    OutboxEventListener.java          # Consumer<ChangeEvent> + metrics
    LoadGenerator.java                # Standalone load test main()
src/main/resources/
    application.properties            # DB + Debezium config, Actuator endpoints
.debezium/                            # Offsets + schema history (gitignored)
```

## Security Notes

- DB credentials currently live in `application.properties` (single source of truth). For any non-private deployment, switch to env var placeholders (`${DB_PASSWORD}`) and inject via real OS env / Spring profile / secret manager.
- `.debezium/` and `.env*` are gitignored.
- If credentials are ever committed, rotate them on the DB and use `git-filter-repo` to purge history.

## Tech Stack

- Spring Boot 3.4.3
- Debezium Embedded 3.0.0.Final (MySQL connector)
- Micrometer + Prometheus registry
- Jackson (parsing Debezium JSON envelopes)
- Java 17
