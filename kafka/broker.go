package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
)

func broker(inStream Stream) {
	outStream := Stream{}
	outTopic := Topic{}
	for _, topic := range inStream.Topics {
		outTopic.Name = topic.Name
		for _, partition := range topic.Partitions {
			outTopic.Partitions = append(outTopic.Partitions, updateMessages(partition))
		}
		outStream.Topics = append(outStream.Topics, outTopic)
	}

	streamLength := len(inStream.Topics)
	outStreamLength := len(inStream.Topics)
	if streamLength != outStreamLength {
		e := "streamLength=" + strconv.Itoa(streamLength)
		e += "outStreamLength=" + strconv.Itoa(outStreamLength)
		panic(e)
	}

	streamBytes, _ := json.Marshal(inStream)
	streamJSON := string(streamBytes)
	bytes, err := writeFile(streamJSON, "streamJSON.txt")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(bytes, "written")
	outStreamBytes, _ := json.Marshal(outStream)
	outStreamJSON := string(outStreamBytes)
	bytes, err = writeFile(outStreamJSON, "outStreamJSON.txt")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(bytes, "written")

	consumer(outStream)
}

func updateMessages(partition Partition) Partition {
	updatedPartition := Partition{}
	i := 0
	for _, message := range partition.Messages {
		msg := message
		msg.Offset = i
		i++
		updatedPartition.Messages = append(updatedPartition.Messages, msg)
	}

	return updatedPartition
}
