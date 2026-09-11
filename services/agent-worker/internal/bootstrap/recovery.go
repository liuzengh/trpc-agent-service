package bootstrap

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	"github.com/nats-io/nats.go/jetstream"
)

func (a *App) initialize(ctx context.Context) error {
	if _, err := a.exporter.Synchronize(ctx, a.projection, a.config.Limits.MaxManifests); err != nil {
		return err
	}
	// MaxAge=0, DiscardNew, DenyDelete/DenyPurge were checked by BindManifest.
	// A replacement stream/durable requires a new owner rebuild, never readiness.
	for {
		if ctx.Err() != nil {
			return errors.New("Worker Manifest initialization deadline exceeded")
		}
		if err := a.checkIncarnations(ctx); err != nil {
			return err
		}
		info, err := a.manifestBroker.Info(ctx)
		if err != nil {
			return natsadapter.ErrUnavailable
		}
		stream, err := a.manifestStream.Info(ctx)
		if err != nil {
			return natsadapter.ErrUnavailable
		}
		if info.NumPending == 0 && info.NumAckPending == 0 && (stream.State.Msgs == 0 || info.AckFloor.Stream >= stream.State.LastSeq) {
			return nil
		}
		if _, err = a.manifests.Poll(ctx); err != nil {
			return err
		}
	}
}
func (a *App) checkIncarnations(ctx context.Context) error {
	manifest, err := a.manifestStream.Info(ctx)
	if err != nil {
		return natsadapter.ErrUnavailable
	}
	mc, err := a.manifestBroker.Info(ctx)
	if err != nil {
		return natsadapter.ErrUnavailable
	}
	run, err := a.runStream.Info(ctx)
	if err != nil {
		return natsadapter.ErrUnavailable
	}
	rc, err := a.runBroker.Info(ctx)
	if err != nil {
		return natsadapter.ErrUnavailable
	}
	reply, err := a.replyStream.Info(ctx)
	if err != nil {
		return natsadapter.ErrUnavailable
	}
	if err = natsadapter.ValidateManifestSource(manifest, mc); err != nil {
		return err
	}
	if err = natsadapter.ValidateRunSource(run, rc); err != nil {
		return err
	}
	if !reply.Created.Equal(a.replyCreated) || !validReplyStream(reply) {
		return natsadapter.ErrTopology
	}
	if !manifest.Created.Equal(a.manifestCreated) || !mc.Created.Equal(a.manifestConsumerCreated) || !run.Created.Equal(a.runCreated) || !rc.Created.Equal(a.runConsumerCreated) {
		return natsadapter.ErrTopology
	}
	return nil
}
func (a *App) monitor(ctx context.Context, failures chan<- error) {
	for {
		if ctx.Err() != nil {
			return
		}
		op, cancel := context.WithTimeout(ctx, a.config.Timing.OperationTimeout.Value())
		err := a.pool.Ping(op)
		if err == nil {
			err = a.checkIncarnations(op)
		}
		cancel()
		a.storageHealthy.Store(err == nil && a.nc.IsConnected())
		if errors.Is(err, natsadapter.ErrTopology) {
			select {
			case failures <- err:
			default:
			}
			return
		}
		if !wait(ctx, a.config.Timing.HealthInterval.Value()) {
			return
		}
	}
}

func validReplyStream(info *jetstream.StreamInfo) bool {
	if info == nil {
		return false
	}
	s := info.Config
	return s.Name == natsadapter.ReplyStream && len(s.Subjects) == 1 && s.Subjects[0] == natsadapter.ReplySubject && s.Retention == jetstream.WorkQueuePolicy && s.Storage == jetstream.FileStorage && s.Discard == jetstream.DiscardNew && s.MaxBytes > 0 && (s.Replicas == 1 || s.Replicas == 3 || s.Replicas == 5) && s.Duplicates == 2*time.Minute && s.MaxMsgSize >= 1<<20 && s.MaxAge == 0 && s.MaxMsgs <= 0 && s.MaxMsgsPerSubject <= 0 && s.DenyDelete && s.DenyPurge && !s.NoAck && !s.Sealed && !s.AllowRollup && !s.AllowMsgTTL && s.SubjectTransform == nil && s.RePublish == nil && s.Mirror == nil && len(s.Sources) == 0 && !s.DiscardNewPerSubject
}
