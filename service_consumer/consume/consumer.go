package consume

import (
	"github.com/sing3demons/service-consumer/database"

	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	logrus "github.com/sirupsen/logrus"
)

type consumerHandler struct {
	db     database.IMongo
	logger *logrus.Logger
}

func NewConsumerHandler(db database.IMongo, logger *logrus.Logger) sarama.ConsumerGroupHandler {
	return consumerHandler{db: db, logger: logger}
}

func (obj consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		// timestamp := time.Now().Format("20060102150405")
		// obj.logger.WithFields(logrus.Fields{
		// 	"timestamp": timestamp,
		// 	"topic":     msg.Topic,
		// 	"partition": msg.Partition,
		// 	"offset":    msg.Offset,
		// 	"key":       string(msg.Key),
		// 	"value":     string(msg.Value),
		// }).Info("Consume message")

		switch msg.Topic {
		case "product.created":

		}

		session.MarkMessage(msg, "")
	}

	return nil
}

func (obj consumerHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (obj consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

type Event struct {
	Header map[string]any `json:"header"`
	Body   any            `json:"body"`
}

func Consume(servers []string, groupID string, topics []string, handler sarama.ConsumerGroupHandler) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	version, err := sarama.ParseKafkaVersion("1.0.0")
	if err != nil {
		logrus.Error("Error parsing Kafka version", err)
	}
	config.Version = version

	client, err := sarama.NewConsumerGroup(servers, groupID, config)
	if err != nil {
		return
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg *sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			logrus.Info("Kafka consumer has been started...")
			if err := client.Consume(ctx, topics, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logrus.Error(err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				logrus.Error(err)
				return
			}

		}
	}()

	// Handle graceful shutdown
	consumptionIsPaused := false
	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		logrus.Error("Received termination signal. Initiating shutdown...")

		cancel()
	case <-ctx.Done():
		logrus.Error("terminating: context cancelled")
	case <-sigusr1:
		toggleConsumptionFlow(client, &consumptionIsPaused)
	}
	// Wait for the consumer to finish processing
	wg.Wait()
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		logrus.Info("Resuming consumption")
	} else {
		client.PauseAll()
		logrus.Info("Pausing consumption")
	}

	*isPaused = !*isPaused
}
