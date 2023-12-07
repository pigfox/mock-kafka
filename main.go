// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

// KafkaProducer is an interface for Kafka producer functionality.
type KafkaProducer interface {
	ProduceMessage(topic string, key, value []byte) error
	Close() error
}

// KafkaConsumer is an interface for Kafka consumer functionality.
type KafkaConsumer interface {
	ConsumeMessages(topic string, handler func(key, value []byte) error) error
	Close() error
}

// MockKafkaProducer is a mock implementation of KafkaProducer for testing purposes.
type MockKafkaProducer struct{}

func (p *MockKafkaProducer) ProduceMessage(topic string, key, value []byte) error {
	// Implement your mock logic for producing a message
	fmt.Printf("Mock producer: Produced message to topic %s with key %s and value %s\n", topic, key, value)
	return nil
}

func (p *MockKafkaProducer) Close() error {
	// Implement your mock logic for closing the producer
	fmt.Println("Mock producer: Closed")
	return nil
}

// MockKafkaConsumer is a mock implementation of KafkaConsumer for testing purposes.
type MockKafkaConsumer struct{}

func (c *MockKafkaConsumer) ConsumeMessages(topic string, handler func(key, value []byte) error) error {
	// Implement your mock logic for consuming messages
	fmt.Printf("Mock consumer: Started consuming messages from topic %s\n", topic)
	return nil
}

func (c *MockKafkaConsumer) Close() error {
	// Implement your mock logic for closing the consumer
	fmt.Println("Mock consumer: Closed")
	return nil
}

// NewKafkaProducer creates a new KafkaProducer instance.
func NewKafkaProducer(brokers []string) (KafkaProducer, error) {
	// In this modified version, return an instance of MockKafkaProducer
	return &MockKafkaProducer{}, nil
}

// NewKafkaConsumer creates a new KafkaConsumer instance.
func NewKafkaConsumer(brokers []string, groupID string) (KafkaConsumer, error) {
	// In this modified version, return an instance of MockKafkaConsumer
	return &MockKafkaConsumer{}, nil
}

type myKafkaProducer struct {
	// actual Kafka producer instance
	producer KafkaProducer
}

func (p *myKafkaProducer) ProduceMessage(topic string, key, value []byte) error {
	return p.producer.ProduceMessage(topic, key, value)
}

func (p *myKafkaProducer) Close() error {
	return p.producer.Close()
}

type myKafkaConsumer struct {
	// actual Kafka consumer instance
	consumer KafkaConsumer
}

func (c *myKafkaConsumer) ConsumeMessages(topic string, handler func(key, value []byte) error) error {
	return c.consumer.ConsumeMessages(topic, handler)
}

func (c *myKafkaConsumer) Close() error {
	return c.consumer.Close()
}

type consumerHandler struct {
	handler func(key, value []byte) error
}

func (h *consumerHandler) Setup() error   { return nil }
func (h *consumerHandler) Cleanup() error { return nil }

func (h *consumerHandler) ConsumeMessages(topic string, handler func(key, value []byte) error) error {
	return nil
}

func setupSignalHandler() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		select {
		case <-sig:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func main() {
	brokers := []string{"localhost:9092"}
	groupID := "my-group"
	topic := "my-topic"

	// Actual Kafka producer and consumer
	actualProducer, err := NewKafkaProducer(brokers)
	if err != nil {
		log.Fatal("Error creating actual Kafka producer:", err)
	}
	defer actualProducer.Close()

	actualConsumer, err := NewKafkaConsumer(brokers, groupID)
	if err != nil {
		log.Fatal("Error creating actual Kafka consumer:", err)
	}
	defer actualConsumer.Close()

	// Produce and consume messages using actual Kafka
	err = actualProducer.ProduceMessage(topic, []byte("key"), []byte("Hello, Kafka!"))
	if err != nil {
		log.Fatal("Error producing message:", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := actualConsumer.ConsumeMessages(topic, func(key, value []byte) error {
			fmt.Printf("Received message: key=%s, value=%s\n", key, value)
			return nil
		})
		if err != nil {
			log.Fatal("Error consuming message:", err)
		}
	}()

	// Wait for a while to allow the consumer to process messages
	time.Sleep(5 * time.Second)

	// Mock Kafka producer and consumer for testing
	mockProducer := &MockKafkaProducer{}
	mockConsumer := &MockKafkaConsumer{}

	// Use mock Kafka in tests
	testProduceAndConsume(mockProducer, mockConsumer)

	// Wait for the actual consumer to finish processing
	wg.Wait()
}

func testProduceAndConsume(producer KafkaProducer, consumer KafkaConsumer) {
	err := producer.ProduceMessage("test-topic", []byte("key"), []byte("Hello, Testing!"))
	if err != nil {
		log.Fatal("Error producing test message:", err)
	}

	err = consumer.ConsumeMessages("test-topic", func(key, value []byte) error {
		fmt.Printf("Received test message: key=%s, value=%s\n", key, value)
		return nil
	})
	if err != nil {
		log.Fatal("Error consuming test message:", err)
	}
}
