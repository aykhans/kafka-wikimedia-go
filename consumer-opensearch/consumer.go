package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/opensearch-project/opensearch-go"
)

var (
	kafkaBootstrapServers = []string{"localhost:9092"}
	kafkaTopic            = "wikimedia.recentchange"
	opensearchAddresses   = []string{"https://localhost:9200"}
	opensearchUsername    = "admin"
	opensearchPassword    = "Toor1234_"
	opensearchIndex       = "wikimedia"
)

type OpensearchMessage struct {
	Message []byte
	ID      string
}

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

// IMessageHandler represents an interface for handling messages in the Kafka consumer.
type IMessageHandler interface {
	// Setup performs any necessary setup tasks before starting message processing to Opensearch.
	Setup()

	// Cleanup performs any necessary cleanup tasks after message consumption is complete.
	Cleanup()

	// OnMessage is called for each incoming message from Kafka.
	// It takes a single parameter, `messages`, which represents the Opensearch message to be processed.
	OnMessage(messages OpensearchMessage)
}

type opensearchHandler struct {
	client *opensearch.Client
}

// Setup initializes the OpenSearch client and creates the necessary index if it doesn't exist.
func (h *opensearchHandler) Setup() {
	log.Println("Setting up OpenSearch client")
	var err error
	client := newOpensearchClient()
	_, err = client.Info()
	if err != nil {
		log.Fatal(err)
	}

	response, err := client.Indices.Exists([]string{opensearchIndex})
	if err != nil {
		log.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode == 404 {
		response, err = client.Indices.Create(opensearchIndex)
		if err != nil {
			log.Fatal(err)
		}
		response.Body.Close()
	}
	h.client = client
}

func (h *opensearchHandler) Cleanup() {
	log.Println("Closing OpenSearch client")
}

// OnMessage is a method that handles incoming Opensearch messages.
// It indexes the message content into Opensearch and logs the response status code.
func (h *opensearchHandler) OnMessage(messages OpensearchMessage) {
	response, err := h.client.Index(
		opensearchIndex,
		strings.NewReader(string(messages.Message)),
		h.client.Index.WithDocumentID(messages.ID),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Index document:", response.StatusCode)
	response.Body.Close()
}

// newOpensearchClient creates a new instance of the opensearch.Client.
// It configures the client with the provided opensearch addresses, username, and password.
// It also sets up a custom transport with TLS configuration to skip certificate verification.
// If any error occurs during the creation of the client, it logs the error and exits the program.
func newOpensearchClient() *opensearch.Client {
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: opensearchAddresses,
		Username:  opensearchUsername,
		Password:  opensearchPassword,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

type kafkaConsumerGroupHandler struct {
	messageHandler IMessageHandler
}

// Setup initializes the consumer group handler.
// It sets up any necessary resources or configurations required for the handler to function properly.
// This method is called by the Sarama library when a new consumer group session is started.
// It returns an error if there was a problem setting up the handler.
func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// h.messageHandler.Setup()
	return nil
}

// Cleanup is called when the consumer group session is ending.
// It is responsible for cleaning up any resources used by the consumer group handler.
func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// h.messageHandler.Cleanup()
	return nil
}

// ConsumeClaim consumes messages from a Kafka consumer group claim.
// It processes each message by unmarshaling it into a WikiData struct,
// calling the message handler's OnMessage method with the OpensearchMessage,
// marking the message as processed, and committing the session.
// If there is an error during unmarshaling, it returns the error.
// It returns nil if all messages are consumed successfully.
func (h *kafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		wikiData := &WikiData{}
		err := json.Unmarshal(message.Value, wikiData)
		if err != nil {
			return err
		}
		h.messageHandler.OnMessage(OpensearchMessage{Message: message.Value, ID: wikiData.Meta.ID})
		sess.MarkMessage(message, "")
		sess.Commit()
	}
	return nil
}

// Consume consumes messages from a Kafka topic using a consumer group.
// It takes an IMessageHandler as a parameter to handle the consumed messages.
// The function creates a new consumer group with the specified group ID and configuration.
// It then starts consuming messages from the Kafka topic using the specified message handler.
// The function blocks until the consumer is closed or an error occurs.
func Consume(messageHandler IMessageHandler) {
	groupID := "consumer-opensearch-demo"

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	config.Consumer.Offsets.AutoCommit.Enable = false
	// config.Consumer.Offsets.AutoCommit.Interval = time.Millisecond * 5000
	// config.RackID = "rack1"

	consumer, err := sarama.NewConsumerGroup(kafkaBootstrapServers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Error closing consumer group: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM signals to gracefully shut down the consumer
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		<-sigterm
		cancel()
	}()

	handler := kafkaConsumerGroupHandler{messageHandler: messageHandler}
	handler.messageHandler.Setup()
	defer handler.messageHandler.Cleanup()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumer.Consume(ctx, []string{kafkaTopic}, &handler); err != nil {
				log.Fatalf("Error consuming messages: %v", err)
			}
			if ctx.Err() != nil {
				log.Println("Consumer closed")
				return
			}
		}
	}()

	wg.Wait()
}

func main() {
	Consume(&opensearchHandler{})
}
