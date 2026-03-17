package rss

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/encypher-studio/newsware-utils/indexer"
	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/state"
	"github.com/mmcdole/gofeed"
	"go.uber.org/zap"
)

var ErrNoBody = errors.New("no body")

type record struct {
	id   string
	news *nwelastic.News
}

type RSSFeed struct {
	URL      string
	PollTime time.Duration
	source   string
	state    *state.StateString
	ticker   ITickerParser
	body     BodyConfig
	indexer  indexer.Indexer
	logger   ecslogger.ILogger
}

func NewRSSFeed(cfg RSSFeedConfig, s *state.State, indexer indexer.Indexer, logger ecslogger.ILogger) (RSSFeed, error) {
	ss, err := state.NewStateString(s, cfg.URL)
	if err != nil {
		return RSSFeed{}, err
	}

	return RSSFeed{
		URL:      cfg.URL,
		PollTime: cfg.PollInterval,
		source:   cfg.Source,
		state:    &ss,
		ticker:   NewTickerParserRegex(cfg.TickerRegex),
		body:     cfg.Body,
		indexer:  indexer,
		logger:   logger,
	}, nil
}

func (r RSSFeed) Poll(ctx context.Context) error {
	timer := time.NewTicker(r.PollTime)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
			news, err := r.newRecords()
			if err != nil {
				return err
			}

			for _, n := range news {
				err = r.indexer.Index(n.news)
				if err != nil {
					return err
				}

				r.logger.Debug(
					"indexed",
					zap.String("feed", r.URL),
					zap.String("id", n.id),
					zap.String("headline", n.news.Headline),
					zap.Time("publicationTime", n.news.PublicationTime),
				)

				err = r.state.SaveState(n.id)
				if err != nil {
					return err
				}
			}
		}
		<-timer.C
		timer.Reset(r.PollTime)
	}
}

func (r RSSFeed) newRecords() ([]record, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(r.URL)
	if err != nil {
		return nil, err
	}

	receivedTime := time.Now()

	var records []record

	lastIdIndex, err := r.lastIdIndex(feed.Items)
	if err != nil {
		return nil, err
	}

	if lastIdIndex == -1 {
		lastIdIndex = len(feed.Items)
	}

	for i := lastIdIndex - 1; i >= 0; i-- {
		item := feed.Items[i]

		r.logger.Debug("processing item", zap.String("feed", r.URL), zap.String("item", fmt.Sprintf("%+v", item)))

		id, err := getId(item)
		if err != nil {
			return nil, err
		}

		record, err := r.itemToRecord(item, receivedTime)
		if err != nil {
			return nil, err
		}

		records = append(records, record)

		err = r.state.SaveState(id)
		if err != nil {
			return nil, err
		}
	}

	return records, nil
}

// lastIdIndex returns the index of the last item with the stored lastId
// or -1 if no item with the lastId is found
// or -100 if an error occurred
func (r RSSFeed) lastIdIndex(items []*gofeed.Item) (int, error) {
	for i := 0; i < len(items); i++ {
		item := items[i]

		id, err := getId(item)
		if err != nil {
			return -100, err
		}

		if id == r.state.Get() {
			return i, nil
		}
	}

	return -1, nil
}

func getId(item *gofeed.Item) (string, error) {
	id := item.GUID

	if len(id) == 0 {
		postId, ok := item.Custom["post-id"]
		if !ok {
			return "", fmt.Errorf("no guid or post-id found")
		}
		id = postId
	}

	return id, nil
}

func (r RSSFeed) itemToRecord(item *gofeed.Item, receivedTime time.Time) (record, error) {
	id, err := getId(item)
	if err != nil {
		return record{}, err
	}

	var bodyParts []string
	if !r.body.ExcludeDescription && item.Description != "" {
		bodyParts = append(bodyParts, item.Description)
	}
	if !r.body.ExcludeContent && item.Content != "" {
		bodyParts = append(bodyParts, item.Content)
	}
	if len(bodyParts) == 0 {
		return record{}, ErrNoBody
	}

	return record{
		id: id,
		news: &nwelastic.News{
			Headline:        item.Title,
			Body:            strings.Join(bodyParts, "\n"),
			Source:          r.source,
			Tickers:         r.ticker.Parse(item.Categories),
			Link:            item.Link,
			PublicationTime: *item.PublishedParsed,
			ReceivedTime:    receivedTime,
		},
	}, nil
}
