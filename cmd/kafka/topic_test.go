package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/mock/gomock"
	"github.com/qdegraaf/kafka-twitter-producer/pkg/mock"
	"github.com/stretchr/testify/assert"
)

func TestCreateTopicRaisesErrorIfKafkaClientReturnsError(t *testing.T) {
	duration, _ := time.ParseDuration("60s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKafka := mock.NewMockKafkaClient(ctrl)
	mockKafka.EXPECT().CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             "test_topic",
		NumPartitions:     1,
		ReplicationFactor: 3}},
		kafka.SetAdminOperationTimeout(duration),
	).Return(nil, fmt.Errorf("oops"))

	result := CreateTopic(mockKafka, "test_topic")
	assert.Error(t, result)
	fmt.Println(result)
}


func TestCreateTopicIgnoresTopicAlreadyExistsErrors(t *testing.T) {
	duration, _ := time.ParseDuration("60s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKafka := mock.NewMockKafkaClient(ctrl)
	mockKafka.EXPECT().CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             "test_topic",
		NumPartitions:     1,
		ReplicationFactor: 3}},
		kafka.SetAdminOperationTimeout(duration),
	).Return([]kafka.TopicResult{
		{
			Topic: "",
			Error: kafka.NewError(kafka.ErrTopicAlreadyExists, "exists", false),
		},
	}, nil)

	result := CreateTopic(mockKafka, "test_topic")
	assert.Nil(t, result)
}
