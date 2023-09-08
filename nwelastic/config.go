package nwelastic

type ElasticConfig struct {
	Addresses   []string
	Username    string
	Password    string
	LogRequests bool `yaml:"logRequests"`
}
