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

func Nats(cfg NatsConfig) (*nats.Conn, error) {
	conn, err := nats.Connect(cfg.Url, nats.Token(cfg.Token))
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

func JetStream(cfg NatsConfig) (nats.JetStreamContext, error) {
	conn, err := Nats(cfg)
	if err != nil {
		return nil, err
	}

	js, err := jetStreamConnect(conn)
	if err != nil {
		return nil, err
	}

	return js, nil
}
