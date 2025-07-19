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
	InboundBinding string `default:":0"`
	RsocketBinding string
}

func main() {
	var args Args
	err := flagsfiller.Parse(&args)
	if err != nil {
		slog.Error("failed to parse args: ", "err", err)
		os.Exit(2)
	}

	if args.RsocketBinding == "" {
		slog.Error("rsocket binding is required")
		os.Exit(1)
	}
	slog.Info("starting server", "rsocket", args.RsocketBinding)

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var rs rsocketServer
	go rs.serve(ctx, args.RsocketBinding)

	wg.Add(1)
	go startInboundListener(ctx, &wg, args.InboundBinding, &rs)

	go func() {
		<-signals
		cancel()
	}()

	wg.Wait()
	slog.Info("finished")
}
