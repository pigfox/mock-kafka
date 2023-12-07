// main_test.go
package main

import "testing"

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
	// No assertion is made in this simple example. You might want to use a Kafka testing library or verify the produced message in a real Kafka environment.
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
	// No assertion is made in this simple example. You might want to use a Kafka testing library or verify the consumed message in a real Kafka environment.
}
