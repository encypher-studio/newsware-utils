package rss

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/encypher-studio/newsware-utils/indexer"
	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/state"
	"github.com/mmcdole/gofeed"
	"github.com/rs/zerolog"
)

var ErrNoBody = errors.New("no body")

// defaultRetryAfter is used when a feed returns a 429 without a usable
// Retry-After header.
const defaultRetryAfter = 30 * time.Second

// httpClient is used to fetch feeds. The timeout prevents a hung request from
// stalling a feed's poll loop indefinitely (gofeed's default client has none).
var httpClient = &http.Client{Timeout: 30 * time.Second}

// RateLimitError is returned when a feed responds with HTTP 429 Too Many
// Requests. RetryAfter carries how long to wait before retrying, taken from the
// Retry-After header when present, otherwise defaultRetryAfter.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (429), retry after %s", e.RetryAfter)
}

type record struct {
	id   string
	news *nwelastic.News
}

type RSSFeed struct {
	URL          string
	PollTime     time.Duration
	source       string
	state        *state.StateString
	tickerParser ITickerParser
	body         BodyConfig
	indexer      indexer.Indexer
	logger       zerolog.Logger
}

func NewRSSFeed(cfg RSSFeedConfig, tickerParser ITickerParser, s *state.State, indexer indexer.Indexer, logger zerolog.Logger) (RSSFeed, error) {
	ss, err := state.NewStateString(s, cfg.URL)
	if err != nil {
		return RSSFeed{}, err
	}

	return RSSFeed{
		URL:          cfg.URL,
		PollTime:     cfg.PollInterval,
		source:       cfg.Source,
		state:        &ss,
		tickerParser: tickerParser,
		body:         cfg.Body,
		indexer:      indexer,
		logger:       logger,
	}, nil
}

func (r RSSFeed) Poll(ctx context.Context) error {
	timer := time.NewTicker(r.PollTime)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
			news, err := r.newRecords()
			if err != nil {
				// On a rate limit, back off for the duration the server asked
				// for and keep polling instead of crashing the feed.
				var rateLimitErr *RateLimitError
				if errors.As(err, &rateLimitErr) {
					r.logger.Warn().
						Str("feed", r.URL).
						Dur("retryAfter", rateLimitErr.RetryAfter).
						Msg("rate limited, backing off")
					if !sleepCtx(ctx, rateLimitErr.RetryAfter) {
						return fmt.Errorf("context canceled")
					}
					continue
				}
				return err
			}

			for _, n := range news {
				err = r.indexer.Index(n.news)
				if err != nil {
					return err
				}

				r.logger.Debug().
					Str("feed", r.URL).
					Str("id", n.id).
					Str("headline", n.news.Headline).
					Time("publicationTime", n.news.PublicationTime).
					Msg("indexed")

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
	feed, err := r.fetchFeed()
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

		r.logger.Debug().Str("feed", r.URL).Str("item", fmt.Sprintf("%+v", item)).Msg("processing item")

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

// fetchFeed retrieves and parses the feed using a timeout-bounded HTTP client.
// On HTTP 429 it returns a *RateLimitError carrying the Retry-After duration so
// the caller can back off instead of treating it as a fatal error.
func (r RSSFeed) fetchFeed() (*gofeed.Feed, error) {
	req, err := http.NewRequest(http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	// Mirror gofeed's default User-Agent so feeds that filter on it keep working.
	req.Header.Set("User-Agent", "Gofeed/1.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, &RateLimitError{RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("http error: %s", resp.Status)
	}

	return gofeed.NewParser().Parse(resp.Body)
}

// parseRetryAfter interprets a Retry-After header value, which may be either a
// number of seconds or an HTTP date. It falls back to defaultRetryAfter when the
// header is missing or unparseable.
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultRetryAfter
	}

	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return defaultRetryAfter
		}
		return time.Duration(secs) * time.Second
	}

	if t, err := http.ParseTime(value); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}

	return defaultRetryAfter
}

// sleepCtx sleeps for d or until ctx is canceled. It returns false if ctx was
// canceled before the duration elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
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
			Body:            strings.Join(bodyParts, "\n\n"),
			Source:          r.source,
			Tickers:         r.tickerParser.Parse(item.Categories),
			Link:            item.Link,
			PublicationTime: *item.PublishedParsed,
			ReceivedTime:    receivedTime,
		},
	}, nil
}
