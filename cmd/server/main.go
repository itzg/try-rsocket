package main

import (
	"bufio"
	"context"
	"errors"
	"github.com/itzg/go-flagsfiller"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
)

type Args struct {
	TextBinding    string `default:":0"`
	RsocketBinding string
}

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

func (s *rsocketServer) channel(remoteAddr string, clientId string) chan payload.Payload {
	socket, ok := s.clientSockets[clientId]
	if !ok {
		slog.Error("no socket for client", "client", clientId)
		return nil
	}

	payloadChan := make(chan payload.Payload)

	messages := flux.CreateFromChannel(payloadChan, nil)
	socket.RequestChannel(messages).
		DoOnNext(func(input payload.Payload) error {
			slog.Info("got message", "for", remoteAddr, "message", input.DataUTF8(), "client", clientId)
			return nil
		}).
		SubscribeOn(scheduler.Parallel()).
		Subscribe(s.ctx)
	slog.Info("subscribed to channel", "client", clientId)

	return payloadChan
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
	go startTextListener(ctx, &wg, args.TextBinding, &rs)

	go func() {
		<-signals
		cancel()
	}()

	wg.Wait()
	slog.Info("finished")
}

func startTextListener(ctx context.Context, wg *sync.WaitGroup, binding string, rs *rsocketServer) {
	defer wg.Done()

	listener, err := net.Listen("tcp", binding)
	if err != nil {
		slog.Error("failed to listen on text binding: ", "err", err)
		os.Exit(1)
	}
	slog.Info("listening for text connections", "binding", listener.Addr())

	go acceptTextConnections(listener, wg, rs)

	<-ctx.Done()

	slog.Info("closing text listener")
	err = listener.Close()
	if err != nil {
		slog.Error("failed to close listener: ", "err", err)
	}
}

func acceptTextConnections(listener net.Listener, wg *sync.WaitGroup, rs *rsocketServer) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to accept connection: ", "err", err)
			continue
		}

		wg.Add(1)
		go handleTextConnection(conn, wg, rs)
	}
}

func handleTextConnection(conn net.Conn, wg *sync.WaitGroup, rs *rsocketServer) {
	defer wg.Done()

	defer closeConnection(conn)
	slog.Info("accepted connection", "from", conn.RemoteAddr())

	var clientId string
	var payloadChan chan payload.Payload
	defer func() {
		if payloadChan != nil {
			slog.Info("closing channels towards client", "client", clientId)
			close(payloadChan)
		}
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		if clientId == "" {
			clientId = scanner.Text()
			payloadChan = rs.channel(conn.RemoteAddr().String(), clientId)
		} else {
			slog.Info("sending text", "text", scanner.Text(), "client", clientId)
			payloadChan <- payload.NewString(scanner.Text(), "")
		}
	}
	slog.Info("finished reading text", "client", clientId)
}

func closeConnection(conn net.Conn) {
	slog.Info("closing connection", "from", conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		slog.Error("failed to close connection: ", "err", err)
	}
}
