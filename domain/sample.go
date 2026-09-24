// Package domain holds plain data types shared across layers -- no I/O,
// no transport awareness.
package domain

// Sample is one MQTT-published PV/OP reading.
type Sample struct {
	SensorID string  `json:"sensor_id"`
	PV       float64 `json:"pv"`
	OP       float64 `json:"op"`
	TSUnixMs int64   `json:"ts"`
}

// WindowMessage is the Kafka/Redpanda wire format (V2) -- deliberately the
// same fields as the gRPC WindowRequest (V1), just JSON instead of
// protobuf. A second serialization path for the identical data isn't worth
// a shared schema registry at this scope; revisit if that changes.
type WindowMessage struct {
	SensorID          string    `json:"sensor_id"`
	PV                []float64 `json:"pv"`
	OP                []float64 `json:"op"`
	WindowStartUnixMs int64     `json:"window_start_unix_ms"`
}

// DetectionResult is what a window publisher gets back after a window is
// scored -- classic detector fields plus the RF model's.
type DetectionResult struct {
	Label         string
	EllipseIndex  float64
	KanoVerdict   bool
	HasActivity   bool
	RFLabel       string
	RFProbability float64
}
