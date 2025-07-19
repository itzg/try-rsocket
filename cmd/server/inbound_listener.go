package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"log/slog"
	"net"
	"os"
	"sync"
)

func startInboundListener(ctx context.Context, wg *sync.WaitGroup, binding string, rs *rsocketServer) {
	defer wg.Done()

	listener, err := net.Listen("tcp", binding)
	if err != nil {
		slog.Error("failed to listen on inbound binding: ", "err", err)
		os.Exit(1)
	}
	slog.Info("listening for inbound connections", "binding", listener.Addr())

	go acceptInboundConnections(listener, wg, rs)

	<-ctx.Done()

	slog.Info("closing inbound listener")
	err = listener.Close()
	if err != nil {
		slog.Error("failed to close listener: ", "err", err)
	}
}

func acceptInboundConnections(listener net.Listener, wg *sync.WaitGroup, rs *rsocketServer) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to accept inbound connection: ", "err", err)
			continue
		}

		wg.Add(1)
		go handleInboundConnection(conn, wg, rs)
	}
}

func handleInboundConnection(conn net.Conn, wg *sync.WaitGroup, rs *rsocketServer) {
	defer wg.Done()
	defer closeConnection(conn)

	slog.Info("accepted inbound connection", "from", conn.RemoteAddr())

	var clientId string

	scanner := bufio.NewScanner(conn)
	// wait for the first line, which will be request client ID
	if scanner.Scan() {
		clientId = scanner.Text()

		slog.Info("got client id, starting rsocket channel", "client", clientId)

		var inboundComplete sync.WaitGroup
		inboundComplete.Add(1)
		err := rs.startChannelToClient(conn.RemoteAddr().String(), clientId,
			sannerToSink(scanner, clientId),
			channelToInbound(conn),
			inboundComplete.Done,
		)
		if err != nil {
			slog.Error("failed to start rsocket channel: ", "err", err)
		}

		inboundComplete.Wait()
	}
	slog.Info("finished handling inbound", "client", clientId)
}

func channelToInbound(conn net.Conn) func(value string) error {
	return func(value string) error {
		_, err := conn.Write([]byte(value + "\n"))
		if err != nil {
			return fmt.Errorf("failed to write to inbound connection: %w", err)
		} else {
			return nil
		}
	}
}

func sannerToSink(scanner *bufio.Scanner, clientId string) func(ctx context.Context, sink flux.Sink) {
	return func(ctx context.Context, sink flux.Sink) {
		defer sink.Complete()
		for scanner.Scan() {
			sink.Next(payload.NewString(scanner.Text(), ""))
		}
		slog.Info("finished reading inbound", "client", clientId)
	}
}

func closeConnection(conn net.Conn) {
	slog.Info("closing inbound connection", "from", conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		slog.Error("failed to close connection: ", "err", err)
	}
}
