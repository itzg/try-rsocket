package main

import (
	"context"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"log/slog"
	"sync"
	"time"
)

func connectClient(err error, args Args, wg *sync.WaitGroup, ctx context.Context) error {
	_, err = rsocket.Connect().
		SetupPayload(payload.NewString(args.ClientId, "")).
		OnClose(func(err error) {
			slog.Info("client closed", "err", err)
			wg.Done()
		}).
		Acceptor(clientSocketAcceptor).
		Transport(rsocket.TCPClient().SetAddr(args.ServerAddress).Build()).
		Start(ctx)
	return err
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
