// Code generated by MockGen. DO NOT EDIT.
// Source: cmd/kafka/topic.go

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	gomock "github.com/golang/mock/gomock"
)

// MockKafka is a mock of Kafka interface.
type MockKafka struct {
	ctrl     *gomock.Controller
	recorder *MockKafkaMockRecorder
}

// MockKafkaMockRecorder is the mock recorder for MockKafka.
type MockKafkaMockRecorder struct {
	mock *MockKafka
}

// NewMockKafka creates a new mock instance.
func NewMockKafka(ctrl *gomock.Controller) *MockKafka {
	mock := &MockKafka{ctrl: ctrl}
	mock.recorder = &MockKafkaMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockKafka) EXPECT() *MockKafkaMockRecorder {
	return m.recorder
}

// NewAdminClientFromProducer mocks base method.
func (m *MockKafka) NewAdminClientFromProducer(arg0 *kafka.Producer) (*kafka.AdminClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewAdminClientFromProducer", arg0)
	ret0, _ := ret[0].(*kafka.AdminClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewAdminClientFromProducer indicates an expected call of NewAdminClientFromProducer.
func (mr *MockKafkaMockRecorder) NewAdminClientFromProducer(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewAdminClientFromProducer", reflect.TypeOf((*MockKafka)(nil).NewAdminClientFromProducer), arg0)
}