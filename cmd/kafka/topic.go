package main

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type KafkaClient interface {
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) (result []kafka.TopicResult, err error) {
}

// CreateTopic creates a topic using the Confluent Admin Client API
func CreateTopic(client *KafkaClient, topic string) error {
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		return fmt.Errorf("ParseDuration(60s): %w", err)
	}

	results, err := client.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 3}},
		kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		return fmt.Errorf("Admin Client request error: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("Failed to create topic: %v\n", result.Error)
		}
		log.Info().Msgf("%v\n", result)
	}
	return nil

}

// DeleteTopic deletes a topic using the Confluent Admin Client API
func DeleteTopic(client *kafka.AdminClient, topic string) error {
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		return fmt.Errorf("ParseDuration(60s): %w", err)
	}

	results, err := client.DeleteTopics(
		ctx,
		[]string{topic},
		kafka.SetAdminOperationTimeout(maxDur),
	)
	if err != nil {
		return fmt.Errorf("Admin Client request error: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("Failed to create topic: %v\n", result.Error)
		}
		log.Info().Msgf("%v\n", result)
	}
	client.Close()
	return nil
}
