package easynats

// With the library, the user should be able to connect to NATS server
//  create streams
//  get hold of already created streams
//  get hold of the consumer on the stream
//  consume messages from the stream using the consumer
//  cancel consumer context so that no more consumption happens

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/therealriteshkudalkar/easynats/nats_store"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NATSStore struct {
	natsClient             *nats.Conn
	natsJetstream          jetstream.JetStream
	streamNameConsumerMap  map[string]jetstream.Consumer
	streamNameStreamHandle map[string]jetstream.Stream
	context                context.Context
	contextCancelFunc      context.CancelFunc
}

func (natsStore *NATSStore) ConnectAndInitializeJetstream(natsURL string) {
	var err error
	natsStore.natsClient, err = nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ConnectHandler(func(conn *nats.Conn) {
			slog.Info("NATS Connection.", "Status", conn.Status(), "Is Connected", conn.IsConnected())
		}),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			slog.Info("Disconnected from NATS server.", "Error", err)
		}),
	)
	if err != nil {
		slog.Error("Could not establish a connection with NATS server.", "Error", err)
		return
	}
	if natsStore.natsClient == nil {
		slog.Error("NATS client is nil.")
		return
	}
	slog.Debug("Created NATS client.", "Statistics", natsStore.natsClient.Statistics)
	natsStore.natsJetstream, err = jetstream.New(natsStore.natsClient)
	if err != nil {
		slog.Error("Could not create NATS Jetstream object.", "Error", err)
		return
	}
	slog.Info("Jetstream connected.")
}

func (natsStore *NATSStore) CreateContext() {
	natsStore.context, natsStore.contextCancelFunc = context.WithCancel(context.Background())
}

func (natsStore *NATSStore) CancelContext() {
	natsStore.contextCancelFunc()
}

func (natsStore *NATSStore) CreateStreamIfNotPresent(streamName string, subjects []string) {
	_, ok := natsStore.streamNameStreamHandle[streamName]
	if ok {
		slog.Info("Stream handle is already present in memory.")
		return
	}
	slog.Info("Stream handle not present in memory. Fetching from NATS Server.", "Stream Name", streamName)

	// Check if jetstream and context are available
	if natsStore.natsJetstream == nil || natsStore.context == nil {
		slog.Error("Cannot get stream handle. Not connected to NATS Jetstream.")
		return
	}

	// Try to get the stream handle from the NATS Server
	var err error
	natsStore.streamNameStreamHandle[streamName], err = natsStore.natsJetstream.Stream(natsStore.context, streamName)
	if err == nil {
		slog.Debug("Fetched stream handle from NATS Server.", "Stream Name", streamName)
		return
	}
	slog.Error("Failed to fetch stream handle from NATS Server. Now creating new stream.",
		"Stream Name", streamName, "Error", err)

	// Create a new stream handle
	natsStore.streamNameStreamHandle[streamName], err = natsStore.natsJetstream.CreateStream(natsStore.context, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
	})
	if err != nil {
		slog.Error("Failed to create a new stream.", "Error", err)
		return
	}
	slog.Debug("Created a new stream.", "Stream Name", streamName)
}

func (natsStore *NATSStore) CreateDurableConsumerIfNotPresent(streamName string, consumerName string) {
	_, ok := natsStore.streamNameConsumerMap[streamName]
	if ok {
		slog.Info("Consumer is already present in memory.")
		return
	}

	streamHandle, ok := natsStore.streamNameStreamHandle[streamName]
	if !ok {
		slog.Error("Stream Handle not present. Initialize the stream before creating the consumer.")
		return
	}
	if natsStore.context == nil {
		slog.Error("Context is nil. Cannot create a new consumer.")
		return
	}
	var err error
	natsStore.streamNameConsumerMap[streamName], err = streamHandle.CreateConsumer(natsStore.context, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error("Error occurred while creating a consumer.", "Error", err)
	}
}

func (natsStore *NATSStore) CreateDurableWithConsumerWithConfigIfNotPresent(streamName string,
	config jetstream.ConsumerConfig) {
	_, ok := natsStore.streamNameConsumerMap[streamName]
	if ok {
		slog.Info("Consumer is already present in the memory.")
		return
	}

	streamHandle, ok := natsStore.streamNameStreamHandle[streamName]
	if !ok {
		slog.Error("Stream Handle not present. Initialize the stream.")
		return
	}
	if natsStore.context == nil {
		slog.Error("Context is nil. Cannot create a new consumer.")
		return
	}
	var err error
	natsStore.streamNameConsumerMap[streamName], err = streamHandle.CreateConsumer(natsStore.context, config)
	if err != nil {
		slog.Error("Error occurred while creating a consumer.", "Error", err)
		return
	}
	slog.Debug("Created new consumer.", "Consumer Name", config.Name)
}

func (natsStore *NATSStore) ReadNextMessage(streamName string, maxWaitTime time.Duration) (*jetstream.Msg, error) {
	// Read Message from the given stream
	consumerHandle, ok := natsStore.streamNameConsumerMap[streamName]
	if !ok {
		return nil, errors.New("no such nats consumer in memory")
	}
	msg, err := consumerHandle.Next(jetstream.FetchMaxWait(maxWaitTime))
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (natsStore *NATSStore) ReadMessageContinuouslyAndPostThemOnChannel(streamName string,
	messageChan chan *jetstream.Msg) {
	for {
		msg, err := natsStore.ReadNextMessage(streamName, time.Second*5)
		if err != nil {
			if !strings.Contains(err.Error(), "nats: timeout") {
				slog.Error("Error while reading new message!", "Error", err)
			}
			continue
		}
		slog.Debug("New message arrived on the channel.")

		messageChan <- msg

		select {
		case <-natsStore.context.Done():
			slog.Info("Completing ReadMessageContinuouslyAndPostThemOnChannel Go routine.")
			return
		default:
			continue
		}
	}
}

// PublishMessage publishes the message bytes on the subject
func (natsStore *NATSStore) PublishMessage(subject string, message []byte) {
	err := natsStore.natsClient.Publish(subject, message)
	if err != nil {
		slog.Debug("Failed to publish the message.", "Error", err)
		return
	}
	slog.Debug("Published message successfully.")
}

// CreatePublisherWithChannel to create a publisher with a channel. Publish messages to the channel to publish them to NATS server.
func (natsStore *NATSStore) CreatePublisherWithChannel(messageCh chan *nats_store.NATSMessage) {
	go func(natsStore *NATSStore, messageCh chan *nats_store.NATSMessage) {
		for message := range messageCh {
			err := natsStore.natsClient.Publish(message.Subject, message.Message)
			if err != nil {
				slog.Debug("Failed to publish the message.", "Error", err)
				continue
			}
			slog.Debug("Published message successfully.")
		}
	}(natsStore, messageCh)
}

func (natsStore *NATSStore) CloseConnection() {
	// Close the NATS connection using the stream
	err := natsStore.natsClient.Drain()
	if err != nil {
		slog.Error("Could not drain the NATS Connection.", "Error", err)
		return
	}
	slog.Info("NATS connection drained Successfully.")
}

func NewNATSStore() *NATSStore {
	return &NATSStore{
		natsClient:             nil,
		natsJetstream:          nil,
		streamNameConsumerMap:  make(map[string]jetstream.Consumer),
		streamNameStreamHandle: make(map[string]jetstream.Stream),
		context:                nil,
		contextCancelFunc:      nil,
	}
}
