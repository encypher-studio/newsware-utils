package nwelastic

type ElasticConfig struct {
	Addresses   []string
	Username    string
	Password    string
	CertPath    string `yaml:"certPath"`
	LogRequests bool   `yaml:"logRequests"`
}
