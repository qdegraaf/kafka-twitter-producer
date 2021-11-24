package main

import (
	//baselog "log"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	twitterstream "github.com/fallenstedt/twitter-stream"
	"github.com/rs/zerolog/log"

	"github.com/qdegraaf/kafka-twitter-producer/config"
	"github.com/qdegraaf/kafka-twitter-producer/pkg/twitter"

)

// CreateTopic creates a topic using the Admin Client API
func CreateTopic(p *kafka.Producer, topic string) {
	a, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		os.Exit(1)
	}
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		fmt.Printf("ParseDuration(60s): %s", err)
		os.Exit(1)
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 3}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Admin Client request error: %v\n", err)
		os.Exit(1)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Failed to create topic: %v\n", result.Error)
			os.Exit(1)
		}
		fmt.Printf("%v\n", result)
	}
	a.Close()

}

func main() {
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
	CreateTopic(p, conf.KafkaTopic)

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
		log.Fatal().Msgf("could not acquire Twitter Bearer token " + err.Error())
	}

	log.Info().Msg("Adding rules....")
	rules := `{
		"add": [
				{"value": "cat has:images", "tag": "cat rule 2"}
			]
		}
`
	err = twitter.AddRules(conf, rules, false)
	if err != nil {
		log.Fatal().Msgf("unable to create rules: %w", err)
	}
	log.Info().Msg("Starting Stream...")

	api := twitterstream.NewTwitterStream(tok.AccessToken)

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