// Package clamav implements malware scanning through clamd's INSTREAM
// protocol. It never treats a malformed or unavailable response as clean.
package clamav

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	defaultMaxBytes = 16 << 20
	chunkSize       = 32 << 10
	maxResponse     = 4096
)

type DialContextFunc func(context.Context, string, string) (net.Conn, error)

type Scanner struct {
	Address        string
	DialContext    DialContextFunc
	DialTimeout    time.Duration
	CommandTimeout time.Duration
	ScanTimeout    time.Duration
	MaxBytes       int
}

func (s Scanner) Probe(ctx context.Context) error {
	_, err := s.version(ctx)
	return err
}

func (s Scanner) ScanMedia(ctx context.Context, content []byte, mediaType string) (preprocess.ScanResult, error) {
	if strings.TrimSpace(mediaType) == "" || len(content) == 0 || len(content) > s.maximum() {
		return preprocess.ScanResult{}, runtime.ErrInvalidEnvelope
	}
	version, err := s.version(ctx)
	if err != nil {
		return preprocess.ScanResult{}, err
	}
	conn, err := s.dial(ctx)
	if err != nil {
		return preprocess.ScanResult{}, err
	}
	defer conn.Close()
	if err := setDeadline(ctx, conn, s.scanTimeout()); err != nil {
		return preprocess.ScanResult{}, err
	}
	if err := writeAll(conn, []byte("zINSTREAM\x00")); err != nil {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	var size [4]byte
	for offset := 0; offset < len(content); {
		end := offset + chunkSize
		if end > len(content) {
			end = len(content)
		}
		binary.BigEndian.PutUint32(size[:], uint32(end-offset))
		if err := writeAll(conn, size[:]); err != nil {
			return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
		}
		if err := writeAll(conn, content[offset:end]); err != nil {
			return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
		}
		offset = end
	}
	if err := writeAll(conn, []byte{0, 0, 0, 0}); err != nil {
		return preprocess.ScanResult{}, runtime.ErrBackendUnavailable
	}
	response, err := readResponse(conn)
	if err != nil {
		return preprocess.ScanResult{}, err
	}
	result := preprocess.ScanResult{Version: "clamav:" + version}
	switch {
	case strings.HasSuffix(response, ": OK"):
		result.Verdict = preprocess.ScanClean
		return result, nil
	case strings.HasSuffix(response, " FOUND"):
		result.Verdict = preprocess.ScanRejected
		return result, nil
	default:
		result.Verdict = preprocess.ScanUnknown
		return result, runtime.ErrBackendUnavailable
	}
}

func (s Scanner) version(ctx context.Context) (string, error) {
	conn, err := s.dial(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := setDeadline(ctx, conn, s.commandTimeout()); err != nil {
		return "", err
	}
	if err := writeAll(conn, []byte("zVERSION\x00")); err != nil {
		return "", runtime.ErrBackendUnavailable
	}
	response, err := readResponse(conn)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(response, "ClamAV ") || strings.ContainsAny(response, "\r\n") {
		return "", runtime.ErrBackendUnavailable
	}
	version := strings.TrimSpace(strings.TrimPrefix(response, "ClamAV "))
	if version == "" {
		return "", runtime.ErrBackendUnavailable
	}
	return version, nil
}

func (s Scanner) dial(ctx context.Context) (net.Conn, error) {
	if strings.TrimSpace(s.Address) == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	timeout := s.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := s.DialContext
	if dial == nil {
		dialer := &net.Dialer{Timeout: timeout}
		dial = dialer.DialContext
	}
	conn, err := dial(dialCtx, "tcp", s.Address)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, runtime.ErrBackendUnavailable
	}
	return conn, nil
}

func (s Scanner) commandTimeout() time.Duration {
	if s.CommandTimeout > 0 {
		return s.CommandTimeout
	}
	return 10 * time.Second
}

func (s Scanner) scanTimeout() time.Duration {
	if s.ScanTimeout > 0 {
		return s.ScanTimeout
	}
	return 2 * time.Minute
}

func (s Scanner) maximum() int {
	if s.MaxBytes > 0 {
		return s.MaxBytes
	}
	return defaultMaxBytes
}

func readResponse(conn net.Conn) (string, error) {
	reader := bufio.NewReaderSize(io.LimitReader(conn, maxResponse+1), maxResponse)
	value, err := reader.ReadString(0)
	if err != nil {
		return "", runtime.ErrBackendUnavailable
	}
	if len(value) > maxResponse || len(value) < 2 {
		return "", runtime.ErrBackendUnavailable
	}
	value = strings.TrimSuffix(value, "\x00")
	if strings.ContainsRune(value, 0) || strings.TrimSpace(value) == "" {
		return "", runtime.ErrBackendUnavailable
	}
	return value, nil
}

func writeAll(conn net.Conn, value []byte) error {
	for len(value) > 0 {
		written, err := conn.Write(value)
		if err != nil {
			return err
		}
		if written <= 0 {
			return errors.New("zero-byte write")
		}
		value = value[written:]
	}
	return nil
}

func setDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return runtime.ErrBackendUnavailable
	}
	return nil
}

var _ preprocess.MalwareScanner = Scanner{}
