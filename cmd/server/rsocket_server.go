package main

import (
	"context"
	"fmt"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"github.com/rsocket/rsocket-go/rx/flux"
	"log/slog"
)

type rsocketServer struct {
	ctx           context.Context
	clientSockets map[string]rsocket.CloseableRSocket
}

func (s *rsocketServer) serve(ctx context.Context, binding string) {
	s.ctx = ctx
	s.clientSockets = make(map[string]rsocket.CloseableRSocket)

	err := rsocket.Receive().
		OnStart(func() {
			slog.Info("rsocket server started on ", "binding", binding)
		}).
		Acceptor(s.rsocketAcceptor).
		Transport(rsocket.TCPServer().SetAddr(binding).Build()).
		Serve(ctx)
	if err != nil {
		slog.Error("failed to start rsocket server: ", "err", err)
	}
}

func (s *rsocketServer) rsocketAcceptor(_ context.Context, setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) (rsocket.RSocket, error) {
	clientId := setup.DataUTF8()

	slog.Info("got setup", "client", clientId)
	s.clientSockets[clientId] = sendingSocket
	sendingSocket.OnClose(func(err error) {
		if err != nil {
			slog.Error("client socket closed with error: ", "err", err)
		} else {
			slog.Info("client socket closed", "client", clientId)
		}
		err = s.clientSockets[clientId].Close()
		if err != nil {
			slog.Error("failed to close client socket: ", "err", err, "client", clientId)
		}
		delete(s.clientSockets, clientId)
	})

	return rsocket.NewAbstractSocket(), nil
}

func (s *rsocketServer) startChannelToClient(remoteAddr string, clientId string, gen func(ctx context.Context, s flux.Sink), onNext func(string) error, onComplete rx.FnOnComplete) error {
	socket, ok := s.clientSockets[clientId]
	if !ok {
		return fmt.Errorf("no socket for client %s", clientId)
	}

	messages := flux.Create(gen)
	socket.RequestChannel(messages).
		DoOnNext(func(input payload.Payload) error {
			slog.Info("got message", "for", remoteAddr, "message", input.DataUTF8(), "client", clientId)
			return onNext(input.DataUTF8())
		}).
		DoOnComplete(onComplete).
		SubscribeOn(scheduler.Parallel()).
		Subscribe(s.ctx)
	slog.Info("subscribed to channel", "client", clientId)

	return nil
}
