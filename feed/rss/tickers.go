package rss

import (
	"regexp"
	"strings"
)

type ITickerParser interface {
	Parse(inputs []string) []string
}

// TickerParserRegex gets a list of tickers from a regex, all capture groups are treated as tickers
type TickerParserRegex struct {
	re *regexp.Regexp
}

func NewTickerParserRegex(re string) TickerParserRegex {
	t := TickerParserRegex{
		re: regexp.MustCompile(re),
	}

	return t
}

func (t TickerParserRegex) Parse(inputs []string) []string {
	seen := make(map[string]bool)
	var tickers []string

	for _, input := range inputs {
		results := t.re.FindAllStringSubmatch(input, -1)
		for _, result := range results {
			for _, ticker := range result[1:] {
				if len(ticker) == 0 {
					continue
				}
				ticker = strings.ToLower(ticker)
				ticker = strings.TrimSpace(ticker)
				if seen[ticker] {
					continue
				}
				seen[ticker] = true
				tickers = append(tickers, ticker)
			}
		}
	}

	return tickers
}
