package nwelastic

type ElasticConfig struct {
	Addresses   []string
	Username    string
	Password    string
	NewsIndex   string
	LogRequests bool `yaml:"logRequests"`
}
