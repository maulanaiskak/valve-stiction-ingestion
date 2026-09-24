// Package repository holds TimescaleDB persistence. Only used by the gRPC
// delivery adapter (V1) -- it's the side with the synchronous round-trip
// to the detection service, so it's the one that persists the result
// (see docs/V3_PLAN.md). The Kafka delivery adapter (V2) has no result to
// persist here; that happens on the Python detection side instead.
package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DetectionResultRepo struct {
	db *pgxpool.Pool
}

func NewDetectionResultRepo(db *pgxpool.Pool) *DetectionResultRepo {
	return &DetectionResultRepo{db: db}
}

func (r *DetectionResultRepo) Save(
	ctx context.Context,
	sensorID string,
	windowStartUnixMs int64,
	label string,
	ellipseIndex float64,
	kanoVerdict bool,
	rfLabel string,
	rfProbability float64,
	pv, op []float64,
) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO window_results
			(sensor_id, window_start, label, ellipse_index, kano_verdict, rf_label, rf_probability, pv, op)
		VALUES (
			$1, to_timestamp($2 / 1000.0), $3, $4, $5, $6, $7, $8, $9
		)`,
		sensorID, windowStartUnixMs, label, ellipseIndex, kanoVerdict, rfLabel, rfProbability, pv, op,
	)
	return err
}
