This is a little experiment with [RSocket](https://rsocket.io/) in Go using [rsocket-go](https://pkg.go.dev/github.com/rsocket/rsocket-go)

## Try it out

Start the server
```shell
go run ./cmd/server/ --rsocket-binding=:8888 --inbound-binding=:9999
```

Start the client
```shell
go run ./cmd/client/ --client-id=one --server-address=localhost:8888
```

Start an inbound connection to server, such as with `nc`

```shell
nc localhost 9999
```

The line of input needs to be the client ID. For the example above it would be:

```
one
```

All subsequent inbound input lines are sent as messages from the server back to the client. The client will start a one second interval ticker to simulate bidirectional activity.