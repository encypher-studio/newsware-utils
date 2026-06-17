package nwelastic

type ElasticConfig struct {
	Addresses   []string
	Username    string
	Password    string
	NewsIndex   string `yaml:"newsIndex"`
	LogRequests bool   `yaml:"logRequests"`
	// MaxConnsPerHost caps total (active+idle) HTTP connections to the Elastic host. 0
	// means unlimited (Go's default). Without a cap, a burst of concurrent requests (e.g.
	// hundreds of simultaneous percolate calls) can open one connection per request, which
	// has been observed to overwhelm Docker's NAT/port-forwarding path and cause
	// "connection reset by peer" errors. Capping this forces excess requests to queue for
	// an existing connection instead.
	MaxConnsPerHost int `yaml:"maxConnsPerHost"`
}
