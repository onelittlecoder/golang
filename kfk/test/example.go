package test

import (
	"fmt"
	"fsuper/kfk"
	"log"
	"os"
)

var (
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

func createConsumer() {
	kfkConsumer, err := kfk.NewConsumer("111.230.149.182:9092", "hello", 0)
	if err != nil {
		panic(err)
	}
	defer kfkConsumer.Consumer.Close()
	for {
		select {
		case msg := <-kfkConsumer.Consumer.Messages():
			fmt.Println("msg offset: ", msg.Offset, " partition: ", msg.Partition, " timestrap: ", msg.Timestamp.Format("2006-Jan-02 15:04"), " value: ", string(msg.Value))
		case err := <-kfkConsumer.Consumer.Errors():
			fmt.Println(err.Err)
		}
	}
}
func createTopic() {

	kfkProducer, err := kfk.NewProducer("111.230.149.182:9092", "hello", 0)
	if err != nil {
		panic(err)
	}
	defer kfkProducer.Producer.Close()
	partition, offset, err := kfkProducer.Send("topic", "hello")
	if err != nil {
		logger.Println("Failed to produce message: ", err)
	}
	logger.Printf("partition=%d, offset=%d\n", partition, offset)
}

func run() {
	//createTopic()
	createConsumer()
}
