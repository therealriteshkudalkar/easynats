package nats_store

type NATSMessage struct {
	Subject string
	Message []byte
}
