package nwelastic

import (
	"time"
)

// News describes a document that can be inserted to rethinkdb. Each field is commented with the sources it applies to.
type News struct {
	Id              string    `json:"id,omitempty"`
	Headline        string    `json:"headline"`
	Body            string    `json:"body,omitempty"`
	Tickers         []string  `json:"tickers,omitempty"`
	Source          string    `json:"source"`
	PublicationTime time.Time `json:"publicationTime"`
	ReceivedTime    time.Time `json:"receivedTime"`
	CreationTime    time.Time `json:"creationTime"` // Override by insert function
	// CategoryCodes represents a code which varies from provider to provider and represents a specific topic such as
	// acquisitions, mergers, etc.
	CategoryCodes []string `json:"categoryCodes"`

	// Ciks only applies to SEC
	Ciks []int `json:"ciks,omitempty"`
	// Link only applies to SEC
	Link string `json:"link,omitempty"`
}
