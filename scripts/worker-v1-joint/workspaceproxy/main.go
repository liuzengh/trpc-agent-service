// workspaceproxy is a disposable joint-test transport adapter, not a service.
// It preserves host loopback endpoints inside the test Worker network namespace.
package main

import (
	"context"
	"flag"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func main() {
	ports := flag.String("ports", "", "owned host ports")
	service := flag.String("service", "/service", "real Worker binary")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var listeners []net.Listener
	var wg sync.WaitGroup
	for _, port := range strings.Split(*ports, ",") {
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
		if err != nil {
			os.Exit(2)
		}
		listeners = append(listeners, l)
		go func(port string, l net.Listener) {
			for {
				in, err := l.Accept()
				if err != nil {
					return
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer in.Close()
					out, err := net.Dial("tcp", net.JoinHostPort("host.docker.internal", port))
					if err != nil {
						return
					}
					defer out.Close()
					cancel := context.AfterFunc(ctx, func() { in.Close(); out.Close() })
					defer cancel()
					done := make(chan struct{})
					go func() { io.Copy(out, in); out.(*net.TCPConn).CloseWrite(); close(done) }()
					io.Copy(in, out)
					in.(*net.TCPConn).CloseWrite()
					<-done
				}()
			}
		}(port, l)
	}
	child := exec.Command(*service)
	child.Env = os.Environ()
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		child.Process.Signal(syscall.SIGTERM)
		err = <-done
	}
	stop()
	for _, l := range listeners {
		l.Close()
	}
	wg.Wait()
	if err != nil {
		os.Exit(1)
	}
}
