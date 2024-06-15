package natsprocessor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/retrier"
	"github.com/nats-io/nats.go"
)

type IJetstream interface {
	ChanQueueSubscribe(subj, queue string, ch chan *nats.Msg, opts ...nats.SubOpt) (*nats.Subscription, error)
	ConsumerInfo(stream, name string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error)
	AddConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error)
}

// NatsProcessor gets news from NATS and processes them using the provided function
type NatsProcessor struct {
	js           IJetstream
	bucket       string
	queueName    string
	retrier      retrier.Retrier
	subscription *nats.Subscription
	processFunc  func(*nwelastic.News) error
}

// NewNatsProcessor creates a new listener
func NewNatsProcessor(js IJetstream, bucket string, queueName string, retrier retrier.Retrier, processFunc func(*nwelastic.News) error) (NatsProcessor, error) {
	return NatsProcessor{
		js:          js,
		bucket:      bucket,
		retrier:     retrier,
		processFunc: processFunc,
		queueName:   queueName,
	}, nil
}

// Listen listens for new messages
func (l *NatsProcessor) listen(chanMsg chan *nats.Msg) error {
	queueName := l.queueName + "-" + l.bucket
	streamName := "KV_" + l.bucket

	_, err := l.js.ConsumerInfo(streamName, queueName)
	if err != nil {
		if errors.Is(err, nats.ErrConsumerNotFound) {
			_, err = l.js.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:           queueName,
				MaxAckPending:     -1,                     // No limit on the number of pending acks
				InactiveThreshold: time.Hour * 24 * 180,   // Queue will be removed after 180 days of inactivity
				AckPolicy:         nats.AckExplicitPolicy, // AckExplicit is required to prevent message loss
				MaxDeliver:        256,
				AckWait:           time.Minute,
				DeliverSubject:    queueName,
				DeliverGroup:      queueName,
			})
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	s, err := l.js.ChanQueueSubscribe(
		"$KV."+l.bucket+".*",
		queueName,
		chanMsg,
		nats.ManualAck(),
		nats.Bind(streamName, queueName),
	)
	if err != nil {
		return err
	}

	l.subscription = s

	return nil
}

func (l *NatsProcessor) Listen(ctx context.Context, onMsgProcessed ...func(*nats.Msg)) error {
	chanMsg := make(chan *nats.Msg, 1000)
	defer close(chanMsg)

	err := l.listen(chanMsg)
	if err != nil {
		return err
	}

	defer l.Unsubscribe()

	for {
		select {
		case msg := <-chanMsg:
			go func() {
				defer func() {
					if len(onMsgProcessed) > 0 {
						onMsgProcessed[0](msg)
					}
				}()

				if len(msg.Data) == 0 {
					msg.Ack()
					return
				}

				news := nwelastic.News{}
				err := json.Unmarshal(msg.Data, &news)
				if err != nil {
					msg.Nak()
					return
				}

				err = l.processFunc(&news)
				if err != nil {
					msg.Nak()
					return
				}

				msg.Ack()
			}()
		case <-ctx.Done():
			return nil
		}
	}
}

func (l *NatsProcessor) Unsubscribe() error {
	if l.subscription == nil {
		return nil
	}
	err := l.subscription.Unsubscribe()
	if err != nil {
		return err
	}
	l.subscription = nil
	return nil
}
