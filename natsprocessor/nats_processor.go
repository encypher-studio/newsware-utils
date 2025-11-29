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

type Opts struct {
	Js            IJetstream
	Bucket        string
	QueueName     string
	Retrier       retrier.Retrier
	ProcessFunc   func(*nwelastic.News) error
	DeliverPolicy *nats.DeliverPolicy // Defaults to DeliverNewPolicy
}

// NewNatsProcessor creates a new listener
func NewNatsProcessor(opts Opts) (NatsProcessor, error) {
	np := NatsProcessor{
		js:          opts.Js,
		bucket:      opts.Bucket,
		retrier:     opts.Retrier,
		processFunc: opts.ProcessFunc,
		queueName:   opts.QueueName,
	}

	var deliverPolicy nats.DeliverPolicy
	if opts.DeliverPolicy == nil {
		deliverPolicy = nats.DeliverNewPolicy
	} else {
		deliverPolicy = *opts.DeliverPolicy
	}

	_, err := np.js.ConsumerInfo(np.streamName(), np.queueName)
	if err != nil {
		if errors.Is(err, nats.ErrConsumerNotFound) {
			_, err = np.js.AddConsumer(np.streamName(), &nats.ConsumerConfig{
				Durable:           np.queueName,
				MaxAckPending:     -1,                     // No limit on the number of pending acks
				InactiveThreshold: time.Hour * 24 * 180,   // Queue will be removed after 180 days of inactivity
				AckPolicy:         nats.AckExplicitPolicy, // AckExplicit is required to prevent message loss
				MaxDeliver:        256,
				AckWait:           time.Minute,
				DeliverSubject:    np.queueName,
				DeliverGroup:      np.queueName,
				DeliverPolicy:     deliverPolicy,
			})
			if err != nil {
				return NatsProcessor{}, err
			}
		} else {
			return NatsProcessor{}, err
		}
	}

	return np, nil
}

// Gets the key value bucket stream name
func (l *NatsProcessor) streamName() string {
	return "KV_" + l.bucket
}

// Listen listens for new messages
func (l *NatsProcessor) listen(chanMsg chan *nats.Msg) error {
	s, err := l.js.ChanQueueSubscribe(
		"$KV."+l.bucket+".*",
		l.queueName,
		chanMsg,
		nats.ManualAck(),
		nats.Bind(l.streamName(), l.queueName),
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
