package nwelastic

type ElasticConfig struct {
	Addresses   []string
	Username    string
	Password    string
	NewsIndex   string `yaml:newsIndex`
	LogRequests bool   `yaml:"logRequests"`
}
