package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/bootstrap"
)

func main() {
	if err := run(); err != nil {
		log.Printf("control-api stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	return runCommand(os.Args[1:], os.Stdout)
}

func runCommand(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("control-api", flag.ContinueOnError)
	flags.SetOutput(stdout)
	printDigest := flags.Bool("print-deployment-contract-digest", false,
		"print the effective platform contract digest without opening the database or starting HTTP")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("control-api does not accept positional arguments")
	}
	if *printDigest {
		digest, err := bootstrap.DeploymentContractDigestFromEnvironment()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, digest)
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	cfg, err := bootstrap.LoadConfig()
	if err != nil {
		return err
	}

	app, err := bootstrap.New(ctx, cfg)
	if err != nil {
		return err
	}

	return app.Run(ctx)
}
