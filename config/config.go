package config

import (
	"github.com/caarlos0/env"
)

type Config struct {
	ConfluentAPIKey string `env:"CONFLUENT_API_KEY"`
	ConfluentAPISecret string `env:"CONFLUENT_API_SECRET"`

	KafkaBootstrapServers string `env:"KAFKA_BOOTSTRAP_SERVERS"`
	KafkaTopic string `env:"KAFKA_TOPIC" envDefault:""`

	TwitterAPIKey string `env:"TWITTER_API_KEY"`
	TwitterAPISecret string `env:"TWITTER_API_SECRET"`
	TwitterTokenURL string `env:"TWITTER_TOKEN_URL" envDefault:"https://api.twitter.com/oauth2/token"`
}

func Parse() (Config, error) {
	confs := Config{}
	err := env.Parse(&confs)
	return confs, err
}
