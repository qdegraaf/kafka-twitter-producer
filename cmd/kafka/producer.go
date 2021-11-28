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
	// TODO: Chop main into sensible chunks. Its getting rather large
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
		log.Fatal().Msgf("failed to create producer: %s", err)
	}

	// Create topic if needed
	err = CreateTopic(p, conf.KafkaTopic)
	if err != nil {
		log.Fatal().Msgf("fatal error during creation of Kafka topic: %w", err)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
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
		log.Fatal().Msgf("could not acquire Twitter Bearer token: %w", err)
	}

	api := twitterstream.NewTwitterStream(tok.AccessToken)

	// If reset flag passed, delete existing rules first
	if reset {
		log.Info().Msg("reset flag set, starting reset of rules")
		err = twitter.DeleteAllRules(api)
		if err != nil {
			log.Fatal().Msgf("an error occurred during rule reset: %w", err)
		}
	}

	newRules, err := twitter.ReadRulesFromFile(rulesFile)
	if err != nil {
		log.Fatal().Msgf("unable to read rules from file at path %s: %w", rulesFile, err)
	}

	log.Info().Msg("Adding rules....")

	err = twitter.AddRules(api, newRules, false)
	if err != nil {
		log.Fatal().Msgf("unable to create rules: %w", err)
	}
	log.Info().Msg("Starting Stream...")

	api.Stream.SetUnmarshalHook(func(bytes []byte) (interface{}, error) {
		data := twitter.StreamData{}
		if err := json.Unmarshal(bytes, &data); err != nil {
			log.Printf("Failed to unmarshal bytes: %v", err)
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
