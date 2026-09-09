package telemetry

import (
	"errors"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var auditWriteFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "agent_audit_write_failures_total",
	Help: "Audit sink failures by fixed sink role",
}, []string{"sink"})

type durableAuditSink struct {
	durable io.Writer
	mirror  io.Writer
}

// NewDurableAuditSink makes the database write authoritative and the process
// log a best-effort copy. Each sink is attempted independently: a broken log
// pipe cannot skip persistence or make a persisted tool decision fail.
func NewDurableAuditSink(durable, mirror io.Writer) io.Writer {
	return &durableAuditSink{durable: durable, mirror: mirror}
}

func (s *durableAuditSink) Write(data []byte) (int, error) {
	n, err := 0, error(nil)
	if s.durable == nil {
		err = errors.New("durable audit sink is required")
	} else {
		n, err = s.durable.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		auditWriteFailures.WithLabelValues("durable").Inc()
	}
	if s.mirror != nil {
		written, mirrorErr := s.mirror.Write(data)
		if mirrorErr != nil || written != len(data) {
			auditWriteFailures.WithLabelValues("mirror").Inc()
		}
	}
	return n, err
}
