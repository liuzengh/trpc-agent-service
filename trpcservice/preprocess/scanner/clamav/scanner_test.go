package clamav

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestScannerCleanAndRejected(t *testing.T) {
	scanner := Scanner{Address: "clamd:3310", DialContext: fakeClamdDialer, CommandTimeout: time.Second, ScanTimeout: time.Second}
	clean, err := scanner.ScanMedia(context.Background(), []byte("safe"), "text/plain")
	if err != nil || clean.Verdict != preprocess.ScanClean || clean.Version != "clamav:1.4.3/test" {
		t.Fatalf("clean=%+v err=%v", clean, err)
	}
	rejected, err := scanner.ScanMedia(context.Background(), []byte("EICAR"), "text/plain")
	if err != nil || rejected.Verdict != preprocess.ScanRejected || rejected.Version == "" {
		t.Fatalf("rejected=%+v err=%v", rejected, err)
	}
	if err := scanner.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestScannerFailsClosed(t *testing.T) {
	scanner := Scanner{Address: "clamd:3310", DialContext: func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			command, _ := reader.ReadString(0)
			if command == "zVERSION\x00" {
				_, _ = server.Write([]byte("not-clamav\x00"))
			}
		}()
		return client, nil
	}, CommandTimeout: time.Second}
	if _, err := scanner.ScanMedia(context.Background(), []byte("safe"), "text/plain"); err != runtime.ErrBackendUnavailable {
		t.Fatalf("invalid version err=%v", err)
	}
	if _, err := (Scanner{Address: "clamd", DialContext: fakeClamdDialer, MaxBytes: 2}).
		ScanMedia(context.Background(), []byte("safe"), "text/plain"); err != runtime.ErrInvalidEnvelope {
		t.Fatalf("size err=%v", err)
	}
}

func TestScannerRejectsEmptyEngineVersion(t *testing.T) {
	scanner := Scanner{Address: "clamd:3310", DialContext: func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			command, _ := reader.ReadString(0)
			if command == "zVERSION\x00" {
				_, _ = server.Write([]byte("ClamAV \x00"))
			}
		}()
		return client, nil
	}, CommandTimeout: time.Second}
	result, err := scanner.ScanMedia(context.Background(), []byte("safe"), "text/plain")
	if err != runtime.ErrBackendUnavailable || result.Version != "" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func fakeClamdDialer(context.Context, string, string) (net.Conn, error) {
	client, server := net.Pipe()
	go serveFakeClamd(server)
	return client, nil
}

func serveFakeClamd(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	command, err := reader.ReadString(0)
	if err != nil {
		return
	}
	if command == "zVERSION\x00" {
		_, _ = conn.Write([]byte("ClamAV 1.4.3/test\x00"))
		return
	}
	if command != "zINSTREAM\x00" {
		_, _ = conn.Write([]byte("UNKNOWN COMMAND ERROR\x00"))
		return
	}
	var payload strings.Builder
	for {
		var size uint32
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			return
		}
		if size == 0 {
			break
		}
		if size > chunkSize {
			return
		}
		part := make([]byte, size)
		if _, err := io.ReadFull(reader, part); err != nil {
			return
		}
		payload.Write(part)
	}
	if strings.Contains(payload.String(), "EICAR") {
		_, _ = conn.Write([]byte("stream: Eicar-Signature FOUND\x00"))
		return
	}
	_, _ = conn.Write([]byte("stream: OK\x00"))
}
