package nats_nw

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

type NatsConfig struct {
	Url    string
	Token  string
	Bucket string
}

func (cfg NatsConfig) Nats(options ...nats.Option) (*nats.Conn, error) {
	options = append(options, nats.Token(cfg.Token))
	conn, err := nats.Connect(cfg.Url, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %v", err)
	}

	return conn, nil
}

func jetStreamConnect(conn *nats.Conn) (nats.JetStreamContext, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to jetstream: %v", err)
	}

	return js, nil
}

func (cfg NatsConfig) JetStream(options ...nats.Option) (nats.JetStreamContext, error) {
	conn, err := cfg.Nats(options...)
	if err != nil {
		return nil, err
	}

	js, err := jetStreamConnect(conn)
	if err != nil {
		return nil, err
	}

	return js, nil
}
