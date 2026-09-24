// Package kafka is the Kafka/Redpanda delivery adapter (V2, FR-8):
// publishes each completed window to a topic partitioned by sensor_id,
// for horizontal scaling across multiple detection-worker replicas. No
// synchronous response, so no persistence happens here -- see
// delivery/grpc's doc comment.
package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/maulanaiskak/valve-stiction-ingestion/domain"
)

type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(brokers, topic string) *Publisher {
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(brokers),
			Topic:    topic,
			Balancer: &kafkago.Hash{}, // key-based partitioning, see Publish
		},
	}
}

func (p *Publisher) Publish(sensorID string, pv, op []float64, windowStart int64) {
	msg := domain.WindowMessage{SensorID: sensorID, PV: pv, OP: op, WindowStartUnixMs: windowStart}
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[%s] failed to marshal window message: %v", sensorID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Key = sensorID: Kafka/Redpanda guarantees same-key messages land on
	// the same partition, keeping each sensor's windows in order for
	// whichever consumer-group member ends up handling that partition --
	// this is exactly FR-8's "partitioned by sensor_id" requirement.
	err = p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(sensorID), Value: payload})
	if err != nil {
		log.Printf("[%s] failed to publish window to kafka: %v", sensorID, err)
		return
	}
	log.Printf("[%s] published window to kafka (window_start=%d)", sensorID, windowStart)
}
