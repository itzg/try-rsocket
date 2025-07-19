package main

import (
	"context"
	"github.com/itzg/go-flagsfiller"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"
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

	_, err = rsocket.Connect().
		SetupPayload(payload.NewString(args.ClientId, "")).
		OnClose(func(err error) {
			slog.Info("client closed", "err", err)
			wg.Done()
		}).
		Acceptor(clientSocketAcceptor).
		Transport(rsocket.TCPClient().SetAddr(args.ServerAddress).Build()).
		Start(ctx)
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

func clientSocketAcceptor(ctx context.Context, socket rsocket.RSocket) rsocket.RSocket {
	return rsocket.NewAbstractSocket(
		rsocket.RequestChannel(func(payloads flux.Flux) (responses flux.Flux) {
			payloadChan := make(chan payload.Payload)
			ticker := time.NewTicker(time.Second)
			go func() {
				slog.Info("sending ticks")
				for tick := range ticker.C {
					payloadChan <- payload.NewString(tick.String(), "")
				}
				slog.Info("finished sending ticks")
			}()

			payloads.
				DoOnNext(func(input payload.Payload) error {
					slog.Info("got message", "message", input.DataUTF8())
					return nil
				}).
				DoOnComplete(func() {
					slog.Info("payloads complete, closing channels and stopping ticker")
					close(payloadChan)
					ticker.Stop()
				}).
				SubscribeOn(scheduler.Parallel()).
				Subscribe(ctx)

			return flux.CreateFromChannel(payloadChan, nil)
		}))
}
