.PHONY: build, mocks, test

build:
	go build cmd/kafka/*

mocks:
	mockgen -destination=pkg/mock/kafka.go -package=mock -source=cmd/kafka/topic.go Kafka

test:
	go test -v ./...
