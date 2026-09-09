package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func (s *FencedSessionService) AppendTrackEvent(ctx context.Context, sess *session.Session, evt *session.TrackEvent, opts ...session.Option) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSession(token, sess)
	}, func(ctx context.Context) error {
		writer, ok := s.inner.(session.TrackService)
		if !ok {
			return fmt.Errorf("session track writer is unavailable")
		}
		return writer.AppendTrackEvent(ctx, sess, evt, opts...)
	})
}

func (s *FencedSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, opts ...session.Option) (value *session.TrackEvents, err error) {
	err = s.runChecked(ctx, telemetry.OperationSessionRead, func(token fence.Token) error {
		return s.scope.validateSessionKey(token, key)
	}, func(ctx context.Context) error {
		reader, ok := s.inner.(interface {
			GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error)
		})
		if !ok {
			return fmt.Errorf("session track reader is unavailable")
		}
		var opErr error
		value, opErr = reader.GetTrackEvents(ctx, key, track, opts...)
		return opErr
	})
	return value, err
}

var _ session.TrackService = (*FencedSessionService)(nil)
