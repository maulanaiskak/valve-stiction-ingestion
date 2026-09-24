# valve-stiction-ingestion

[![CI](https://github.com/maulanaiskak/valve-stiction-ingestion/actions/workflows/ci.yml/badge.svg)](https://github.com/maulanaiskak/valve-stiction-ingestion/actions)

MQTT ingestion service for a distributed, real-time control-valve stiction detection pipeline. Subscribes to a simulator's PV/OP stream, buffers each sensor into fixed-size windows, sends completed windows to the detection/ML service, and persists the result to TimescaleDB.

**Part of a 5-repo system** — see [System Design (HLD)](https://github.com/maulanaiskak/valve-stiction-backend/blob/main/docs/HLD.md) and [Whitepaper](https://github.com/maulanaiskak/valve-stiction-backend/blob/main/docs/WHITEPAPER.md) for the full picture: a train/serve model-generalization failure found, fixed, and honestly bounded; a monolith split into 5 independently-deployable services; 99.6%/AUC 0.9998 live-streaming detection accuracy after the fix.

| Repo | Role |
|---|---|
| [simulator](https://github.com/maulanaiskak/valve-stiction-simulator) | Synthetic PV/OP signal generator |
| **ingestion** (this repo) | MQTT subscribe, windowing, forwards to detection |
| [detection](https://github.com/maulanaiskak/valve-stiction-detection) | Classic detector + trained RF model |
| [backend](https://github.com/maulanaiskak/valve-stiction-backend) | REST + WebSocket API |
| [frontend](https://github.com/maulanaiskak/valve-stiction-frontend) | React dashboard |

## Where this fits

```mermaid
flowchart LR
    SIM[Simulator] -->|MQTT publish| MQ[[Mosquitto]]
    MQ --> ING["Ingestion (this repo)"]
    ING -->|gRPC| DET[Detection]
    DET -->|result| ING
    ING -->|persist| DB[(TimescaleDB)]
    DB --> BE[Backend]
    BE --> FE[Frontend]
```

Two modes, same windowing logic, swapped via `PUBLISH_MODE`:

- `grpc` (default) — calls the detection service directly and persists the response itself.
- `kafka` — publishes each window to Redpanda/Kafka instead (partitioned by `sensor_id`), for horizontal scaling across multiple detection-service replicas. Persistence in this mode happens on the detection side (`delivery/kafka/worker.py` in valve-stiction-detection), not here.

## Request flow (gRPC mode)

```mermaid
sequenceDiagram
    autonumber
    participant MQ as Mosquitto
    participant Ing as Ingestion (this repo)
    participant Det as Detection service
    participant DB as TimescaleDB

    MQ->>Ing: deliver sample
    Ing->>Ing: buffer sample per sensor
    Note over Ing: window full, 100 samples
    Ing->>Det: gRPC DetectWindow
    Det-->>Ing: label, rf_label, rf_probability
    Ing->>DB: INSERT window_results
```

## Window lifecycle

Per-sensor windowing state, as `usecase.Ingestor` actually implements it — no timeout/eviction, a silent sensor's buffer just idles:

```mermaid
stateDiagram-v2
    [*] --> Buffering: first sample for this sensor_id
    Buffering --> Buffering: sample count < WindowSize
    Buffering --> Publishing: sample count reaches WindowSize
    Publishing --> Buffering: window handed to publisher,\nbuffer slides forward by WindowStride
```

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

```bash
docker build -t valve-stiction-ingestion .
docker run -e DETECTION_SERVICE_ADDR=detection:50051 -e DATABASE_URL=... valve-stiction-ingestion
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

## Testing

```bash
go test ./...
go vet ./...
```

Covers non-overlapping windows, sliding windows (smaller stride), and per-sensor buffer isolation (`usecase/ingestor_test.go`).

## Files worth knowing about

`proto/detection.proto`, `db/init.sql` — copies of the shared gRPC contract and TimescaleDB schema. This service and [valve-stiction-detection](https://github.com/maulanaiskak/valve-stiction-detection) each keep their own copy (no shared/orchestrator repo) — if you change one, change the other. `delivery/grpc/detectionpb/` is the generated Go client stub from `proto/detection.proto`.
