package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const MaxMessages int = 50
const MaxTopics int = 10
const MaxPartitions int = 30

var errorChannel chan []string
var wg sync.WaitGroup

type Message struct {
	Offset  int
	Value   string
	Created time.Time
}

type Partition struct {
	Messages []Message
}

type Topic struct {
	Name       string
	Partitions []Partition
}

type Stream struct {
	Topics []Topic
}

func getRandomInt(max int) int {
	return rand.Intn(max-1) + 1
}

func Mock() {
	errorChannel = make(chan []string)
	var errorsSlice []string

	wg.Add(1)
	go producer()
	select {
	case errorsSlice = <-errorChannel:
		break
	}

	wg.Wait()

	length := len(errorsSlice)
	if length == 0 {
		fmt.Println("no errors returned")
	} else {
		fmt.Println(strconv.Itoa(length)+" errors returned:", errorsSlice)
	}
}

/*
https://www.youtube.com/watch?v=udnX21__SuU
producer - Application that sends msgs to Topic
Message - Small to medium size data []byte
consumer - Application that receives msgs from Topic
broker - Topic server
Cluster - Group of computers, broker1 - brokerN
Topic - A name for a Topic stream
Partitions - Part of a topic
Offset - Unique id for a msg within a partition
consumer groups - A group of consumer acting as a single logical unit
To get message: Topic NAME -> Partition Number -> Offset
*/

func prettyString(str string) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(str), "", "    "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func writeFile(content string, fileName string) (int, error) {
	path := "/Users/vn56pgd/Desktop/" + fileName
	fmt.Println("path:", path)
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("Failed to create file:", err)
		return 0, err
	}
	defer file.Close()

	prettyStr, _ := prettyString(content)
	numBytes, err := file.WriteString(prettyStr)
	if err != nil {
		fmt.Println("Failed to write to file:", err) //print the failed message
		return numBytes, err
	}
	fmt.Println("Wrote to file " + fileName) //print the success message
	return numBytes, err
}
