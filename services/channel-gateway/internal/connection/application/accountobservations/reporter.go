// Package accountobservations owns bounded diagnostic reporting; it never grants
// account use or renews source freshness. The composition bridge supplies states.
package accountobservations

import (
	"context"
	"strconv"
	"time"

	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Source interface{ Observations() []c.Observation }
type Sink interface {
	Report(context.Context, []c.Observation) error
}
type Reporter struct {
	source Source
	sink   Sink
}

func New(source Source, sink Sink) *Reporter { return &Reporter{source, sink} }
func (r *Reporter) Run(ctx context.Context) error {
	sequence := int64(0)
	last := map[string]string{}
	sent := map[string]time.Time{}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		all := r.source.Observations()
		for start := 0; start < len(all) && shutdown.Err() == nil; start += 100 {
			end := min(start+100, len(all))
			batch := all[start:end]
			for i := range batch {
				sequence++
				batch[i].Sequence = sequence
				batch[i].State = "ERROR"
				batch[i].Reason = "SHUTDOWN"
				batch[i].At = time.Now().UTC()
			}
			_ = r.sink.Report(shutdown, batch)
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		now := time.Now().UTC()
		batch := []c.Observation{}
		keys, values := []string{}, []string{}
		flush := func() {
			if len(batch) == 0 {
				return
			}
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			e := r.sink.Report(op, batch)
			cancel()
			if e == nil {
				for i, k := range keys {
					last[k] = values[i]
					sent[k] = now
				}
			}
			batch = nil
			keys = nil
			values = nil
		}
		all := r.source.Observations()
		present := map[string]bool{}
		for _, o := range all {
			key := o.AccountID
			present[key] = true
			value := strconv.FormatInt(o.ConnectionRevision, 10) + o.ReceiveMode + o.State + o.Reason
			if o.OwnerEpoch != nil {
				value += strconv.FormatInt(*o.OwnerEpoch, 10)
			}
			if last[key] == value && now.Sub(sent[key]) < 30*time.Second {
				continue
			}
			sequence++
			o.Sequence = sequence
			o.At = now
			batch = append(batch, o)
			keys = append(keys, key)
			values = append(values, value)
			if len(batch) == 100 {
				flush()
			}
		}
		flush()
		for k := range last {
			if !present[k] {
				delete(last, k)
				delete(sent, k)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return ctx.Err()
}
