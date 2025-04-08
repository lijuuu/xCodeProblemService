package natsclient

import (
	"time"

	"github.com/nats-io/nats.go"
)

type NatsClient struct {
	Conn *nats.Conn
}

func NewNatsClient(natsURL string) (*NatsClient, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}
	return &NatsClient{Conn: nc}, nil
}

func (n *NatsClient) Close() {
	if n.Conn != nil {
		n.Conn.Close()
	}
}

func (n *NatsClient) Publish(subject string, data []byte) error {
	return n.Conn.Publish(subject, data)
}

func (n *NatsClient) Request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	return n.Conn.Request(subject, data, timeout)
}

func (n *NatsClient) Subscribe(subject string, handler func(*nats.Msg)) (*nats.Subscription, error) {
	return n.Conn.Subscribe(subject, handler)
}
