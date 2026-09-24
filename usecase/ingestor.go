// Package usecase holds the windowing logic (PRD FR-2, FR-8's bridge in
// V2) -- transport-agnostic: it doesn't know or care whether a completed
// window ends up going out over gRPC or Kafka, only that it gets handed
// to a WindowPublisher.
package usecase

import (
	"sync"

	"github.com/maulanaiskak/valve-stiction-ingestion/domain"
)

// WindowSize matches what valve-stiction-ml validated the classic detector
// against (see its ML_PLAN.md §13) -- not an arbitrary choice, and not
// configurable for that reason: changing it would mean scoring windows the
// classic detector was never validated against.
const WindowSize = 100

// WindowStride controls how far the window slides after each emission.
// Defaults to WindowSize (non-overlapping, matching training exactly).
// A smaller stride gives more frequent detections at the cost of adjacent
// windows sharing samples -- each individual window is still exactly
// WindowSize samples, the same content shape training validated, just
// sampled more often. Configurable via WINDOW_STRIDE since this tradeoff
// (responsiveness vs. redundant detections) has no single right answer.
var WindowStride = WindowSize

// WindowPublisher is how a completed window reaches the detection side --
// a delivery adapter (gRPC client, Kafka producer) implements this.
// Windowing itself doesn't know or care which one is in use.
type WindowPublisher interface {
	Publish(sensorID string, pv, op []float64, windowStart int64)
}

// sensorBuffer accumulates samples for one sensor until a full window is
// ready. ts is tracked per-sample (not just a single start field) because
// with sliding windows the buffer never fully empties between emissions --
// the oldest remaining sample's timestamp is what each new window starts at.
type sensorBuffer struct {
	mu sync.Mutex
	pv []float64
	op []float64
	ts []int64
}

type Ingestor struct {
	mu        sync.Mutex
	buffers   map[string]*sensorBuffer
	publisher WindowPublisher
}

func NewIngestor(publisher WindowPublisher) *Ingestor {
	return &Ingestor{buffers: make(map[string]*sensorBuffer), publisher: publisher}
}

func (in *Ingestor) HandleSample(s domain.Sample) {
	in.mu.Lock()
	buf, ok := in.buffers[s.SensorID]
	if !ok {
		buf = &sensorBuffer{}
		in.buffers[s.SensorID] = buf
	}
	in.mu.Unlock()

	buf.mu.Lock()
	buf.pv = append(buf.pv, s.PV)
	buf.op = append(buf.op, s.OP)
	buf.ts = append(buf.ts, s.TSUnixMs)

	var pvWindow, opWindow []float64
	var windowStart int64
	full := len(buf.pv) >= WindowSize
	if full {
		pvWindow = append([]float64(nil), buf.pv[:WindowSize]...)
		opWindow = append([]float64(nil), buf.op[:WindowSize]...)
		windowStart = buf.ts[0]
		// slide forward by WindowStride; stride == WindowSize (the
		// default) reduces to the original non-overlapping behavior
		buf.pv = append([]float64(nil), buf.pv[WindowStride:]...)
		buf.op = append([]float64(nil), buf.op[WindowStride:]...)
		buf.ts = append([]int64(nil), buf.ts[WindowStride:]...)
	}
	buf.mu.Unlock()

	if full {
		in.publisher.Publish(s.SensorID, pvWindow, opWindow, windowStart)
	}
}
