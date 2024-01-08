package kafka

type SelectedMessage struct {
	Topic     string
	Partition int
	Offset    int
}

/*
func setSelectedMessage() SelectedMessage {
	return SelectedMessage{
		Topic:     "topic18",
		Partition: 4,
		Offset:    19,
	}
}

func getMessage(topic Topic) {
	targetMessage := setSelectedMessage()
	for k, v := range topic.Stream {
		if targetMessage.Topic == k {
			for _, v2 := range v {
				if targetMessage.Offset == v2.Offset && targetMessage.Partition == v2.Partition {
					fmt.Println("Target message found", targetMessage)
					return
				}
			}
		}
	}
	res2B, _ := json.Marshal(targetMessage)
	fmt.Println("Target message NOT found", string(res2B))
}
*/
