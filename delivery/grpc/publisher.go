// Package grpc is the gRPC delivery adapter (V1): calls the detection/ML
// service and persists the result itself -- it's the only side with a
// synchronous round-trip, so it owns persistence (see docs/V3_PLAN.md;
// the detection service is a stateless predictor and doesn't touch the
// DB). The Kafka adapter (delivery/kafka) doesn't get a response back, so
// it has nothing to persist here -- kafka_worker.py persists on the
// Python side instead.
package grpc

import (
	"context"
	"log"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/maulanaiskak/valve-stiction-ingestion/delivery/grpc/detectionpb"
	"github.com/maulanaiskak/valve-stiction-ingestion/repository"
)

type Publisher struct {
	client pb.DetectionClient
	repo   *repository.DetectionResultRepo // nil is valid: skips persistence (e.g. tests)
}

func NewPublisher(detectionAddr string, repo *repository.DetectionResultRepo) (*Publisher, error) {
	conn, err := googlegrpc.NewClient(detectionAddr, googlegrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Publisher{client: pb.NewDetectionClient(conn), repo: repo}, nil
}

func (p *Publisher) Publish(sensorID string, pv, op []float64, windowStart int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := p.client.DetectWindow(ctx, &pb.WindowRequest{
		SensorId:          sensorID,
		Pv:                pv,
		Op:                op,
		WindowStartUnixMs: windowStart,
	})
	if err != nil {
		log.Printf("[%s] detection request failed: %v", sensorID, err)
		return
	}
	log.Printf(
		"[%s] label=%s ellipse_index=%.3f kano=%v has_activity=%v rf_label=%s rf_probability=%.3f",
		sensorID, resp.Label, resp.EllipseIndex, resp.KanoVerdict, resp.HasActivity,
		resp.RfLabel, resp.RfProbability,
	)

	if p.repo == nil {
		return
	}
	insertCtx, insertCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer insertCancel()
	if err := p.repo.Save(
		insertCtx, sensorID, windowStart, resp.Label, resp.EllipseIndex, resp.KanoVerdict,
		resp.RfLabel, resp.RfProbability, pv, op,
	); err != nil {
		log.Printf("[%s] failed to persist detection result: %v", sensorID, err)
	}
}
