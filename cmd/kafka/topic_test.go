package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/mock/gomock"
	"github.com/qdegraaf/kafka-twitter-producer/pkg/mock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateTopicRaisesErrorIfAdminClientFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := kafka.Producer{}
	mockKafka := mock.NewMockKafka(ctrl)
	mockKafka.EXPECT().NewAdminClientFromProducer(gomock.Eq(&mockProducer)).Return(nil, fmt.Errorf("oops"))

	result := CreateTopic(&mockProducer, "test_topic")
	assert.Error(t, result)
}
