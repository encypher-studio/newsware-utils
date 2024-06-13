package natsprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/retrier"
	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

type integrationConfiguration struct {
	Nats natsConfig `yaml:"nats"`
}

var _integrationCfg *integrationConfiguration

func integrationCfg() natsConfig {
	if _integrationCfg == nil {
		_, filename, _, _ := runtime.Caller(0)
		configPath := path.Join(path.Dir(filename), "../config.test.yml")
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			panic(fmt.Errorf("failed to read config.test.yml for integration tests: %w", err))
		}

		err = yaml.Unmarshal(configBytes, &_integrationCfg)
		if err != nil {
			panic(fmt.Errorf("failed to unmarshal yaml: %w", err))
		}
	}

	return _integrationCfg.Nats
}

func TestNatsProcessor_listen(t *testing.T) {
	integration := os.Getenv("INTEGRATION")
	if integration == "" {
		t.Skip("skipping: set INTEGRATION env to run this test")
	}

	natsConn, err := nats.Connect(integrationCfg().Url, nats.Token(integrationCfg().Token))
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}

	js, err := natsConn.JetStream()
	if err != nil {
		t.Fatalf("failed to connect to jetstream: %v", err)
	}

	kv, err := js.KeyValue(integrationCfg().Bucket)
	if err != nil {
		t.Fatalf("failed to connect to kv store: %v", err)
	}

	l, err := NewNatsProcessor(
		js,
		integrationCfg().Bucket,
		"TestNatsProcessor_listen",
		retrier.Retrier{
			MaxRetries: 1,
			MaxDelay:   time.Millisecond,
			OnRetry:    func(n uint, err error, message string) {},
		},
		func(news *nwelastic.News) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	tests := []struct {
		name        string
		publishMsgs [][]byte
	}{
		{
			"should receive 1 news",
			[][]byte{
				marshalUnsafe(mockNews(1)),
			},
		},
		{
			"should receive 4 news",
			[][]byte{
				marshalUnsafe(mockNews(1)),
				marshalUnsafe(mockNews(2)),
				marshalUnsafe(mockNews(3)),
				marshalUnsafe(mockNews(4)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kl, err := kv.ListKeys()
			if err != nil {
				t.Fatalf("failed to list keys: %v", err)
			}

			for k := range kl.Keys() {
				err := kv.Delete(k)
				if err != nil {
					t.Fatalf("failed to delete key: %v", err)
				}
			}

			chanComplete := make(chan error)
			defer close(chanComplete)
			chanMsg := make(chan *nats.Msg, 1000)
			defer close(chanMsg)

			err = l.listen(chanMsg)
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}
			defer l.subscription.Unsubscribe()

			go func() {
				received := make([][]byte, 0)
				for msg := range chanMsg {
					msg.Ack()
					if len(msg.Data) == 0 {
						continue
					}
					received = append(received, msg.Data)

					if len(received) == len(tt.publishMsgs) {
						for i, expectedMsg := range tt.publishMsgs {
							if !bytes.Equal(expectedMsg, received[i]) {
								chanComplete <- fmt.Errorf("received msg is not equal to published msg, got: %s, expected: %s", received[i], expectedMsg)
							}
						}
						chanComplete <- nil
					}
				}
			}()

			for _, msg := range tt.publishMsgs {
				_, err := kv.Create(strconv.Itoa(rand.Int()), msg)
				if err != nil {
					t.Fatalf("failed to publish news: %v", err)
				}
			}

			for {
				select {
				case <-time.After(time.Second * 5):
					t.Fatalf("timeout")
				case err := <-chanComplete:
					if err != nil {
						t.Fatalf("%v", err)
					}
					return
				}
			}
		})
	}
}

func TestNatsProcessor_Listen(t *testing.T) {
	integration := os.Getenv("INTEGRATION")
	if integration == "" {
		t.Skip("skipping: set INTEGRATION env to run this test")
	}

	natsConn, err := nats.Connect(integrationCfg().Url, nats.Token(integrationCfg().Token))
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}

	js, err := natsConn.JetStream()
	if err != nil {
		t.Fatalf("failed to connect to jetstream: %v", err)
	}

	kv, err := js.KeyValue(integrationCfg().Bucket)
	if err != nil {
		t.Fatalf("failed to connect to kv store: %v", err)
	}

	tests := []struct {
		name                string
		publishMsgs         [][]byte
		expectedProcessNews []nwelastic.News
	}{
		{
			name: "process 1 news",
			publishMsgs: [][]byte{
				marshalUnsafe(mockNews(0)),
			},
			expectedProcessNews: []nwelastic.News{
				mockNews(0),
			},
		},
		{
			name: "process 3 news",
			publishMsgs: [][]byte{
				marshalUnsafe(mockNews(0)),
				marshalUnsafe(mockNews(1)),
				marshalUnsafe(mockNews(2)),
			},
			expectedProcessNews: []nwelastic.News{
				mockNews(0),
				mockNews(1),
				mockNews(2),
			},
		},
		{
			name: "receive 2 news and ignore 1 empty message",
			publishMsgs: [][]byte{
				marshalUnsafe(mockNews(0)),
				{},
				marshalUnsafe(mockNews(2)),
			},
			expectedProcessNews: []nwelastic.News{
				mockNews(0),
				mockNews(2),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			var actualProcessNews []nwelastic.News
			processFunc := func(n *nwelastic.News) error {
				actualProcessNews = append(actualProcessNews, *n)
				return nil
			}
			l, err := NewNatsProcessor(
				js,
				integrationCfg().Bucket,
				"TestListener_Listen_queue",
				retrier.Retrier{
					MaxRetries: 1,
					MaxDelay:   time.Millisecond,
					OnRetry:    func(n uint, err error, message string) {},
				},
				func(n *nwelastic.News) error {
					return nil
				},
			)
			if err != nil {
				t.Fatalf("failed to create listener: %v", err)
			}

			// Drain the subscription
			chanMsg := make(chan *nats.Msg, 1000)
			defer close(chanMsg)
			err = l.listen(chanMsg)
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}

			for l.subscription == nil {
				time.Sleep(time.Millisecond * 10)
			}

			for len(chanMsg) > 0 {
				msg := <-chanMsg
				err := msg.AckSync()
				if err != nil {
					t.Fatalf("failed to ack message: %v", err)
				}
			}

			err = l.Unsubscribe()
			if err != nil {
				t.Fatalf("failed to unsubscribe: %v", err)
			}

			l.processFunc = processFunc

			// Teardown
			defer func() {
				kl, err := kv.ListKeys()
				if err != nil {
					t.Fatalf("failed to list keys: %v", err)
				}

				for k := range kl.Keys() {
					err := kv.Delete(k)
					if err != nil {
						t.Fatalf("failed to delete key: %v", err)
					}
				}
			}()

			// Test
			ctx, ctxCancel := context.WithCancel(context.Background())
			defer ctxCancel()
			chanComplete := make(chan error)
			defer close(chanComplete)
			processedMsgs := 0
			go func() {
				err = l.Listen(ctx, func(msg *nats.Msg) {
					processedMsgs++
					if processedMsgs == len(tt.publishMsgs) {
						chanComplete <- nil
					}
				})
				if err != nil {
					chanComplete <- fmt.Errorf("failed to Listen: %v", err)
				}
			}()

			for l.subscription == nil {
				time.Sleep(time.Millisecond * 10)
			}

			for _, msg := range tt.publishMsgs {
				_, err := kv.Create(strconv.Itoa(rand.Int()), msg)
				if err != nil {
					t.Fatalf("failed to publish news: %v", err)
				}
			}

			for {
				select {
				case <-time.After(time.Second * 5):
					t.Fatalf("timeout")
				case err := <-chanComplete:
					if processedMsgs != len(tt.publishMsgs) {
						t.Fatalf("processed %d messages, expected %d", processedMsgs, len(tt.publishMsgs))
					}

					if err != nil {
						t.Fatalf("%v", err)
					}

					newsSortFunc := func(a, b nwelastic.News) int {
						if a.Id < b.Id {
							return -1
						}
						if a.Id > b.Id {
							return 1
						}
						return 0
					}

					slices.SortFunc(actualProcessNews, newsSortFunc)
					slices.SortFunc(tt.expectedProcessNews, newsSortFunc)

					if !reflect.DeepEqual(tt.expectedProcessNews, actualProcessNews) {
						t.Fatalf("expected:\n%+v\ngot:\n%+v", tt.expectedProcessNews, actualProcessNews)
					}

					return
				}
			}
		})
	}
}

func marshalUnsafe(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func mockNews(seed int) nwelastic.News {
	n := nwelastic.News{
		Id:              strconv.Itoa(seed),
		Headline:        fmt.Sprintf("headline%d", seed),
		Body:            fmt.Sprintf("body%d", seed),
		Tickers:         []string{fmt.Sprintf("ticker%d", seed), fmt.Sprintf("ticker%d%d", seed, seed)},
		Source:          fmt.Sprintf("source%d", seed),
		PublicationTime: time.Unix(int64(seed), 0),
		ReceivedTime:    time.Unix(int64(seed), 0),
		CreationTime:    time.Unix(int64(seed), 0),
		CategoryCodes:   []string{fmt.Sprintf("categoryCode%d", seed), fmt.Sprintf("categoryCode%d%d", seed, seed)},
		Ciks:            []int{seed},
		Link:            fmt.Sprintf("link%d", seed),
	}
	return n
}
