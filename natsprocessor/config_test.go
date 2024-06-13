package natsprocessor

type config struct {
	Nats natsConfig `yaml:"nats"`
}

type natsConfig struct {
	Url    string `yaml:"url"`
	Token  string `yaml:"token"`
	Bucket string `yaml:"bucket"`
}
