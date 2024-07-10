package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

var (
	kafkaBootstrapServers = []string{"localhost:9092"}
	kafkaTopic            = "wikimedia.recentchange"
	wikimediaStreamURL    = "https://stream.wikimedia.org/v2/stream/recentchange"
)

// WikiData represents the structure of the data received from Wikimedia.
type WikiData struct {
	Schema           string `json:"$schema"`
	ID               int    `json:"id"`
	Type             string `json:"type"`
	Namespace        int    `json:"namespace"`
	Title            string `json:"title"`
	TitleURL         string `json:"title_url"`
	Comment          string `json:"comment"`
	Timestamp        int    `json:"timestamp"`
	User             string `json:"user"`
	Bot              bool   `json:"bot"`
	NotifyURL        string `json:"notify_url"`
	Minor            bool   `json:"minor"`
	Patrolled        bool   `json:"patrolled"`
	ServerURL        string `json:"server_url"`
	ServerName       string `json:"server_name"`
	ServerScriptPath string `json:"server_script_path"`
	Wiki             string `json:"wiki"`
	ParsedComment    string `json:"parsedcomment"`
	Meta             struct {
		URI       string `json:"uri"`
		RequestID string `json:"request_id"`
		ID        string `json:"id"`
		DT        string `json:"dt"`
		Domain    string `json:"domain"`
		Stream    string `json:"stream"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int    `json:"offset"`
	} `json:"meta"`
	Length struct {
		Old int `json:"old"`
		New int `json:"new"`
	} `json:"length"`
	Revision struct {
		Old int `json:"old"`
		New int `json:"new"`
	} `json:"revision"`
}

func (wikiData *WikiData) Marshal() []byte {
	data, _ := json.Marshal(wikiData)
	return data
}

// IMessageHandler is an interface that defines the methods for handling messages.
type IMessageHandler interface {
	// Setup is called to set up any necessary resources before starting message handling.
	Setup()

	// Cleanup is called to clean up any resources after message handling is complete.
	Cleanup()

	// OnMessage is called when a new message is received.
	// It takes a pointer to a WikiData object as a parameter.
	OnMessage(wikiData *WikiData)
}

type kafkaMessageHandler struct {
	producer sarama.SyncProducer
}

// Setup initializes the Kafka message handler by creating a new producer.
func (h *kafkaMessageHandler) Setup() {
	h.producer = newProducer()
}

// Cleanup closes the Kafka producer and performs any necessary cleanup operations.
func (h *kafkaMessageHandler) Cleanup() {
	if err := h.producer.Close(); err != nil {
		log.Fatalf("Failed to close Kafka producer: %v", err)
	}
}

// OnMessage is a method that handles incoming WikiData messages.
// It sends the message to a Kafka topic and logs the result.
func (h *kafkaMessageHandler) OnMessage(wikiData *WikiData) {
	message := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(wikiData.Marshal()),
	}
	partition, offset, err := h.producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	log.Printf("Message is stored in: topic(%s) - partition(%d) - offset(%d)\n", kafkaTopic, partition, offset)
}

// newProducer creates a new instance of a Kafka producer with the specified configuration.
// It returns a sarama.SyncProducer that can be used to send messages to Kafka topics.
func newProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // enable message delivery reports
	config.Producer.RequiredAcks = sarama.WaitForAll // require all in-sync replicas to acknowledge the message
	config.Producer.Retry.Max = 5 // number of retries before giving up on sending a message to a partition
	config.Producer.Retry.Backoff = time.Second * 60 // time to wait between retries
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner // walks through the available partitions one at a time
	config.Producer.Compression = sarama.CompressionSnappy // compress messages using Snappy
	config.Producer.Idempotent = true // producer will ensure that messages are successfully sent and acknowledged
	// linger.ms
	config.Producer.Flush.Frequency = time.Millisecond * 20 // time to wait before sending a batch of messages
	// batch.size
	config.Producer.Flush.Bytes = 32 * 1024 // number of bytes to trigger a batch of messages
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(kafkaBootstrapServers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	return producer
}

// WikimediaEventHandler connects to the Wikimedia stream and handles incoming events.
// It takes an IMessageHandler as a parameter, which is responsible for setting up and cleaning up the message handling logic.
// The function reads events from the stream, parses the JSON data, and passes it to the message handler.
// It also counts the number of messages processed and prints the total count at the end.
func WikimediaEventHandler(messageHandler IMessageHandler) {
	// Connect to the Wikimedia stream
	resp, err := http.Get(wikimediaStreamURL)
	if err != nil {
		log.Fatalf("Failed to connect to SSE endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to connect to SSE endpoint: %s", resp.Status)
	}

	reader := bufio.NewReader(resp.Body)
	// Read the initial response from the stream to confirm the connection
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read from SSE endpoint: %v", err)
	}
	line = strings.TrimSpace(line)
	if line != ":ok" {
		log.Fatalf("Failed to connect to SSE endpoint: %s", line)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM signals to gracefully shut down the producer
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		<-sigterm
		cancel()
	}()

	messageHandler.Setup()
	defer messageHandler.Cleanup()
	messageCnt := 0
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Producer is shutting down")
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil {
					log.Fatalf("Failed to read from SSE endpoint: %v", err)
				}
				// Trim leading and trailing whitespace from the line and ignore empty lines
				line = strings.TrimSpace(line)
				if len(line) == 0 {
					continue
				}

				wikiData := &WikiData{}
				switch {
				// Check for the event type and ignore any other events except "message"
				case strings.HasPrefix(line, "event: "):
					if line != "event: message" {
						log.Fatalf("Failed to read from SSE endpoint: %s", line)
					}
				// Parse the JSON data and pass it to the message handler
				case strings.HasPrefix(line, "data: "):
					err = json.Unmarshal([]byte(line[6:]), &wikiData)
					if err != nil {
						log.Fatalf("Failed to unmarshal JSON: %v", err)
					}
					messageHandler.OnMessage(wikiData)
					messageCnt++
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()
	wg.Wait()
	fmt.Printf("Total messages: %d\n", messageCnt)
}

func main() {
	WikimediaEventHandler(&kafkaMessageHandler{})
}
