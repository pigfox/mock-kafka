// main_test.go
package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestActualKafkaProducer(t *testing.T) {
	actualProducer, err := NewKafkaProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatal("Error creating actual Kafka producer:", err)
	}
	defer actualProducer.Close()

	err = actualProducer.ProduceMessage("test-topic", []byte("key"), []byte("Hello, Testing!"))
	if err != nil {
		t.Fatal("Error producing test message with actual Kafka producer:", err)
	}
	assert.NoError(t, err, "Error producing test message with actual Kafka producer")
}

func TestActualKafkaConsumer(t *testing.T) {
	actualConsumer, err := NewKafkaConsumer([]string{"localhost:9092"}, "test-group")
	if err != nil {
		t.Fatal("Error creating actual Kafka consumer:", err)
	}
	defer actualConsumer.Close()

	err = actualConsumer.ConsumeMessages("test-topic", func(key, value []byte) error {
		t.Logf("Received message: key=%s, value=%s\n", key, value)
		return nil
	})
	if err != nil {
		t.Fatal("Error consuming messages with actual Kafka consumer:", err)
	}
	assert.NoError(t, err, "Error consuming messages with actual Kafka consumer")
}
