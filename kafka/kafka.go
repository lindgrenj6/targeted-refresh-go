package kafka

import "github.com/segmentio/kafka-go"

func Producer() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"10.0.0.15:9092"},
		Topic:    "platform.topological-inventory.collector-ansible-tower",
		Balancer: &kafka.LeastBytes{},
	})
}

func Message(key, value string) kafka.Message {
	return kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}
}
