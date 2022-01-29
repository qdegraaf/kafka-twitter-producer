package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	twitterstream "github.com/fallenstedt/twitter-stream"
	"github.com/rs/zerolog/log"

	"github.com/qdegraaf/kafka-twitter-producer/config"
	"github.com/qdegraaf/kafka-twitter-producer/pkg/twitter"
)

var reset bool
var rulesFile string

func Init() {
	flag.StringVar(&rulesFile, "rules", "", "Path to rules YAML file")
	flag.BoolVar(&reset, "reset", false, "If set will delete all rules present on streaming API before creating new ones")
	flag.Parse()
}

func main() {
	Init()

	conf, err := config.Parse()
	if err != nil {
		log.Fatal().Msgf("could not parse environment variables: " + err.Error())
	}

	// Create Producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": conf.KafkaBootstrapServers,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     conf.ConfluentAPIKey,
		"sasl.password":     conf.ConfluentAPISecret})
	if err != nil {
		log.Fatal().Msgf("failed to create Kafka producer: %s", err.Error())
	}

	// Create Admin Client
	client, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		log.Fatal().Msgf("failed to create Confluent admin client: %s", err.Error())
	}

	// Create topic if needed
	err = CreateTopic(client, conf.KafkaTopic)
	if err != nil {
		log.Fatal().Msgf("fatal error during creation of Kafka topic: %s", err.Error())
	}

	// Handle messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Warn().Msgf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					log.Warn().Msgf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)

	tok, err := twitterstream.NewTokenGenerator().SetApiKeyAndSecret(conf.TwitterAPIKey, conf.TwitterAPISecret).RequestBearerToken()
	if err != nil {
		log.Fatal().Msgf("could not acquire Twitter Bearer token: %s", err.Error())
	}

	api := twitterstream.NewTwitterStream(tok.AccessToken)

	// If reset flag passed, delete existing rules first
	if reset {
		log.Info().Msg("reset flag set, starting reset of rules")
		err = twitter.DeleteAllRules(api)
		if err != nil {
			log.Fatal().Msgf("an error occurred during rule reset: %s", err.Error())
		}
	}

	newRules, err := twitter.ReadRulesFromFile(rulesFile)
	if err != nil {
		log.Fatal().Msgf("unable to read rules from file at path %s: %s", rulesFile, err.Error())
	}

	log.Info().Msg("Adding rules....")

	err = twitter.AddRules(api, newRules, false)
	if err != nil {
		log.Fatal().Msgf("unable to create rules: %s", err.Error())
	}
	log.Info().Msg("Starting Stream...")

	api.Stream.SetUnmarshalHook(func(bytes []byte) (interface{}, error) {
		data := twitter.StreamData{}
		if err := json.Unmarshal(bytes, &data); err != nil {
			log.Error().Msgf("Failed to unmarshal bytes: %s", err.Error())
		}
		return data, err
	})

	err = api.Stream.StartStream("?expansions=author_id&tweet.fields=created_at")

	go func() {
		for message := range api.Stream.GetMessages() {
			if message.Err != nil {
				panic(message.Err)
			}

			tweet, ok := message.Data.(twitter.StreamData)
			if !ok {
				continue
			}
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &conf.KafkaTopic, Partition: kafka.PartitionAny},
				Value:          []byte(tweet.Data.Text),
				Key:            []byte("test"),
			}, nil)
			log.Info().Msg(tweet.Data.Text)
		}
	}()

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Print(<-ch)

	log.Info().Msg("Stopping Stream...")
	api.Stream.StopStream()
	log.Info().Msg("Stopping Producer...")
	p.Close()

}
