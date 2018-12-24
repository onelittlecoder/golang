package kfk

import (
	"github.com/Shopify/sarama"
	"strings"
)

type KfkProducer struct {
	Producer sarama.SyncProducer
	Topic    string
	Partition int32
}

func NewProducer(address,topic string,partition int32) (*KfkProducer, error) {
	//"111.230.149.182:9092"
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(strings.Split(address, ","), config)
	if err != nil {
		return nil, err
	}
	return &KfkProducer{Producer: producer,Topic:topic,Partition:partition}, nil
}

func (this *KfkProducer) Send(key, value string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{}
	msg.Key = sarama.StringEncoder(key)
	msg.Value = sarama.ByteEncoder(value)
	msg.Topic = this.Topic
	msg.Partition = this.Partition
	return this.Producer.SendMessage(msg)
}
