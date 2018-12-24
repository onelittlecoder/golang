package kfk

import (
	"github.com/Shopify/sarama"
	"strings"
)

type KfkConsumer struct {
	Consumer  sarama.PartitionConsumer
	Topic     string
	Partition int32
}

func NewConsumer(address, topic string, partiton int32) (*KfkConsumer, error) {
	//"111.230.149.182:9092"

	consumer, err := sarama.NewConsumer(strings.Split(address, ","), nil)
	if err != nil {
		return nil, err
	}
	pc, err := consumer.ConsumePartition(topic, partiton, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}
	return &KfkConsumer{Consumer: pc, Topic: topic, Partition: partiton}, nil

}
