package kafka

import (
	"strconv"
	"time"
)

func producer() {
	stream := Stream{}
	for i := 0; i < MaxTopics; i++ {
		topic := Topic{Name: "Topic" + strconv.Itoa(i)}
		for j := 0; j < MaxPartitions; j++ {
			topic.Partitions = append(topic.Partitions, generateMessages())
		}
		stream.Topics = append(stream.Topics, topic)
	}

	broker(stream)
}

func generateMessages() Partition {
	partition := Partition{}
	//maxMessages:=getRandomInt(max)
	for i := 0; i < MaxMessages; i++ {
		msg := Message{
			Offset:  0,
			Value:   "message" + strconv.Itoa(i),
			Created: time.Now(),
		}
		partition.Messages = append(partition.Messages, msg)
	}

	return partition
}
