package kafka

import (
	"fmt"
	"strconv"
	"time"
)

var failures []string

func consumer(stream Stream) {
	defer wg.Done()
	validatedStream := validateSteam(stream)
	for k, v := range validatedStream.Topics {
		fmt.Print("--Topic: ", k)
		fmt.Println(v)
	}
	errorChannel <- failures
}

func validateSteam(stream Stream) Stream {
	outStream := Stream{}
	outTopic := Topic{}
	for topicIndex, topic := range stream.Topics {
		if topic.Name != "" {
			for _, partition := range topic.Partitions {
				outTopic.Partitions = append(outTopic.Partitions, validatePartitionMessages(partition))
			}
		} else {
			failures = append(failures, "missing name at topic index "+strconv.Itoa(topicIndex))
		}
		outStream.Topics = append(outStream.Topics, outTopic)
	}
	return outStream
}

func validatePartitionMessages(partition Partition) Partition {
	validatedPartition := Partition{}
	for messageIndex, message := range partition.Messages {
		_ = messageIndex
		if -1 < message.Offset && isValidTimeDotTime(message.Created) && message.Value != "" {
			validatedPartition.Messages = append(validatedPartition.Messages, message)
		} else {
			failures = append(failures, "invalid message "+fmt.Sprintf("%s", message))
		}
	}
	return validatedPartition
}

func isValidTimeDotTime(t time.Time) bool {
	if !t.IsZero() {
		return true
	}
	return false
}
