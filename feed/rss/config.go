package rss

import "time"

type BodyConfig struct {
	ExcludeDescription bool `yaml:"excludeDescription"`
	ExcludeContent     bool `yaml:"excludeContent"`
}

type RSSFeedConfig struct {
	URL          string        `yaml:"url"`
	Source       string        `yaml:"source"`
	PollInterval time.Duration `yaml:"pollInterval"`
	Body         BodyConfig    `yaml:"body"`
}
