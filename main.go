// Ingestion service (PRD FR-2, and FR-8's bridge in V2): subscribes to
// MQTT, buffers each sensor's PV/OP into fixed-size windows, forwards full
// windows onward -- either directly to the detection service over gRPC
// (V1) or as a message on a Redpanda/Kafka topic keyed by sensor_id (V2).
// See docs/V1_PLAN.md, docs/V2_PLAN.md for why the same binary does both
// rather than forking: the windowing logic (buffering, stride, per-sensor
// isolation) is identical either way.
//
// This file is just wiring: env vars -> layered packages (domain,
// usecase, repository, delivery). All the actual logic lives there.
package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	grpcdelivery "github.com/maulanaiskak/valve-stiction-ingestion/delivery/grpc"
	kafkadelivery "github.com/maulanaiskak/valve-stiction-ingestion/delivery/kafka"
	mqttdelivery "github.com/maulanaiskak/valve-stiction-ingestion/delivery/mqtt"
	"github.com/maulanaiskak/valve-stiction-ingestion/repository"
	"github.com/maulanaiskak/valve-stiction-ingestion/usecase"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func buildPublisher() usecase.WindowPublisher {
	mode := getenv("PUBLISH_MODE", "grpc")
	switch mode {
	case "grpc":
		detectionAddr := getenv("DETECTION_SERVICE_ADDR", "localhost:50051")
		dbURL := getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/valve_stiction")
		db, err := pgxpool.New(context.Background(), dbURL)
		if err != nil {
			log.Fatalf("failed to create db pool for %s: %v", dbURL, err)
		}
		repo := repository.NewDetectionResultRepo(db)
		publisher, err := grpcdelivery.NewPublisher(detectionAddr, repo)
		if err != nil {
			log.Fatalf("failed to connect to detection service at %s: %v", detectionAddr, err)
		}
		return publisher
	case "kafka":
		brokers := getenv("KAFKA_BROKERS", "localhost:9092")
		topic := getenv("KAFKA_TOPIC", "valve-windows")
		log.Printf("publishing windows to kafka topic %q on %s", topic, brokers)
		return kafkadelivery.NewPublisher(brokers, topic)
	default:
		log.Fatalf("PUBLISH_MODE must be \"grpc\" or \"kafka\", got %q", mode)
		return nil
	}
}

func main() {
	brokerURL := getenv("MQTT_BROKER_URL", "tcp://localhost:1883")
	topic := getenv("MQTT_TOPIC", "valve/data")

	if strideStr := os.Getenv("WINDOW_STRIDE"); strideStr != "" {
		stride, err := strconv.Atoi(strideStr)
		if err != nil || stride <= 0 || stride > usecase.WindowSize {
			log.Fatalf("WINDOW_STRIDE must be an integer in (0, %d], got %q", usecase.WindowSize, strideStr)
		}
		usecase.WindowStride = stride
	}
	log.Printf("window_size=%d window_stride=%d", usecase.WindowSize, usecase.WindowStride)

	in := usecase.NewIngestor(buildPublisher())

	if err := mqttdelivery.Subscribe(brokerURL, topic, in.HandleSample); err != nil {
		log.Fatalf("failed to connect to MQTT broker: %v", err)
	}

	select {} // block forever
}
