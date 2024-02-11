package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/sing3demons/service-consumer/database"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

var logger *logrus.Logger

func init() {
	godotenv.Load(".env")
	logger = logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

const (
	assignor = "sticky"
	oldest   = true
	verbose  = false
	group    = "kafka-for-dev"
	version  = "1.0.0"
)

func main() {

	brokers := os.Getenv("KAFKA_BROKERS")
	topics := os.Getenv("KAFKA_TOPICS")

	keepRunning := true

	hostName, err := os.Hostname()
	logger.WithFields(logrus.Fields{
		"brokers":  brokers,
		"topics":   topics,
		"group":    group,
		"assignor": assignor,
		"oldest":   oldest,
		"verbose":  verbose,
		"version":  version,
		"hostName": hostName,
		"pid":      os.Getpid(),
		"ppid":     os.Getppid(),
		"uid":      os.Getuid(),
		"gid":      os.Getgid(),
		"error":    err,
	}).Info("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = logger
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		logger.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	default:
		logger.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		logger.Panicf("Error creating consumer group client: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				logger.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	logger.Info("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			logger.Info("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			logger.Info("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		logger.Panicf("Error closing client: %v", err)
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	db := database.New("products")
	productDb := db.Collection("products")
	productLanguageDb := db.Collection("product_languages")
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {

		sessionId := strings.TrimPrefix(session.MemberID(), "sarama-")
		id := session.GenerationID()
		headers := make(Header)
		for _, header := range message.Headers {
			key := string(header.Key)
			value := string(header.Value)
			if key != "" && value != "" {
				headers[key] = value
			}
		}

		data := Message{
			Partition: message.Partition,
			Offset:    message.Offset,
			Key:       string(message.Key),
			Value:     string(message.Value),
			Timestamp: message.Timestamp,
			Headers:   headers,
			Topic:     message.Topic,
			SessionID: sessionId,
			ID:        id,
		}

		var insertOneResult *mongo.InsertOneResult
		var err error
		switch message.Topic {
		case "create.products":
			result := Product{}
			json.Unmarshal([]byte(data.Value), &result)
			insertOneResult, err = productDb.InsertOne(context.Background(), result)
		case "create.productsLanguage":
			result := SupportingLanguage{}
			json.Unmarshal([]byte(data.Value), &result)
			insertOneResult, err = productLanguageDb.InsertOne(context.Background(), result)
		default:
			logger.Info("topic: "+message.Topic, " not found")
		}

		logger.WithFields(logrus.Fields{
			"partition":  data.Partition,
			"offset":     data.Offset,
			"key":        data.Key,
			"value":      data.Value,
			"timestamp":  data.Timestamp,
			"headers":    data.Headers,
			"topic":      data.Topic,
			"session_id": data.SessionID,
			"id":         data.ID,
			"result":     insertOneResult,
			"error":      err,
		}).Info("topic: ", message.Topic)

		session.MarkMessage(message, "")
	}

	return nil
}

type Header map[string]string
type Message struct {
	Partition int32     `json:"partition,omitempty"`
	Offset    int64     `json:"offset,omitempty"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
	Timestamp time.Time `json:"@timestamp,omitempty"`
	Headers   Header    `json:"headers,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	ID        int32     `json:"id,omitempty"`
}

type Price struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Tax                *Tax                  `json:"tax,omitempty"`
	PopRelationships   []*PopRelationship    `json:"popRelationship,omitempty"`
	UnitOfMeasure      *UnitOfMeasure        `json:"unitOfMeasure,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
}

type Tax struct {
	Type  string  `json:"type,omitempty"`
	Value float64 `json:"value,omitempty"`
}

type PopRelationship struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type UnitOfMeasure struct {
	Unit     string  `json:"unit,omitempty"`
	Amount   float64 `json:"amount,omitempty"`
	Currency string  `json:"currency,omitempty"`
}

type Category struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Description        string                `json:"description,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
}

type SupportingLanguage struct {
	ID            string         `json:"id,omitempty"`
	Name          string         `json:"name,omitempty"`
	Description   string         `json:"description,omitempty"`
	LanguageCode  string         `json:"languageCode,omitempty"`
	UnitOfMeasure *UnitOfMeasure `json:"unitOfMeasure,omitempty"`
	Attachment    []*Attachment  `json:"attachment,omitempty"`
	Status        string         `json:"status,omitempty"`
	CreatedAt     string         `json:"createdAt,omitempty"`
	UpdatedAt     string         `json:"updatedAt,omitempty"`
}

type Attachment struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	URL         string   `json:"url,omitempty"`
	Type        string   `json:"type,omitempty"`
	Status      string   `json:"status,omitempty"`
	CreatedAt   string   `json:"createdAt,omitempty"`
	UpdatedAt   string   `json:"updatedAt,omitempty"`
	Display     *Display `json:"display,omitempty"`
	RedirectURL string   `json:"redirectUrl,omitempty"`
}

type Display struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type Product struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Price              []*Price              `json:"price,omitempty"`
	Category           []*Category           `json:"category,omitempty"`
	Description        string                `json:"description,omitempty"`
	Stock              int                   `json:"stock,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
}
