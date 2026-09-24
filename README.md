# valve-stiction-ingestion

MQTT ingestion service for the valve stiction fault-detection pipeline. Subscribes to a simulator's PV/OP stream ([valve-stiction-simulator](https://github.com/maulanaiskak/valve-stiction-simulator)), buffers each sensor into fixed-size windows, sends completed windows to the detection/ML service ([valve-stiction-detection](https://github.com/maulanaiskak/valve-stiction-detection)), and persists the result to TimescaleDB.

Two modes, same windowing logic, swapped via `PUBLISH_MODE`:

- `grpc` (default) — calls the detection service directly and persists the response itself.
- `kafka` — publishes each window to Redpanda/Kafka instead (partitioned by `sensor_id`), for horizontal scaling across multiple detection-service replicas. Persistence in this mode happens on the detection side (`delivery/kafka/worker.py` in valve-stiction-detection), not here.

## Architecture

Layered: `domain` (plain types, no I/O) → `usecase` (windowing logic, transport-agnostic) → `repository` (TimescaleDB persistence) → `delivery` (transport adapters). `main.go` is just wiring.

```
domain/sample.go              Sample, WindowMessage, DetectionResult -- plain data
usecase/ingestor.go            Ingestor -- sliding-window buffering, transport-agnostic
repository/detection_result.go TimescaleDB writes (gRPC adapter only)
delivery/mqtt/subscriber.go    inbound: MQTT subscribe -> domain.Sample
delivery/grpc/publisher.go     outbound: gRPC call to detection service + persist (V1)
delivery/kafka/publisher.go    outbound: publish to Redpanda/Kafka (V2)
```

## Run

```bash
go build -o ingestion .
DETECTION_SERVICE_ADDR=localhost:50051 DATABASE_URL=postgresql://postgres:postgres@localhost:5432/valve_stiction ./ingestion
```

| Env var | Default |
|---|---|
| `MQTT_BROKER_URL` | `tcp://localhost:1883` |
| `MQTT_TOPIC` | `valve/data` |
| `WINDOW_STRIDE` | `100` (non-overlapping) |
| `PUBLISH_MODE` | `grpc` |
| `DETECTION_SERVICE_ADDR` (grpc mode) | `localhost:50051` |
| `DATABASE_URL` (grpc mode, persists here) | `postgresql://postgres:postgres@localhost:5432/valve_stiction` |
| `KAFKA_BROKERS`, `KAFKA_TOPIC` (kafka mode) | `localhost:9092`, `valve-windows` |

## Files worth knowing about

`proto/detection.proto`, `db/init.sql` — copies of the shared gRPC contract and TimescaleDB schema. This service and [valve-stiction-detection](https://github.com/maulanaiskak/valve-stiction-detection) each keep their own copy (no shared/orchestrator repo) — if you change one, change the other. `delivery/grpc/detectionpb/` is the generated Go client stub from `proto/detection.proto`.
