package main

import (
	"context"
	"github.com/itzg/go-flagsfiller"
	"log/slog"
	"os"
	"os/signal"
	"sync"
)

type Args struct {
	ServerAddress string
	ClientId      string
}

func main() {
	var args Args
	err := flagsfiller.Parse(&args)
	if err != nil {
		slog.Error("failed to parse args: ", "err", err)
		os.Exit(2)
	}

	if args.ServerAddress == "" {
		slog.Error("server address is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

	err = connectClient(err, args, &wg, ctx)
	if err != nil {
		slog.Error("failed to connect to server: ", "err", err)
		os.Exit(1)
	}
	wg.Add(1)

	slog.Info("connected to server")

	go func() {
		<-signals
		cancel()
	}()

	wg.Wait()
}
