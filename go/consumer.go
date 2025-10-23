package main

import (
	"context"
	"log"
	"time"
	"go/amqp091-go-1.10.0"
	// "pkg.go.dev/github.com/rabbitmq/amqp091-go"
	// amqp "github.com/rabbitmq/amqp091-go"
)

// startRabbitMQConsumer 启动一个RabbitMQ消费者，将消息发送到channel
func startRabbitMQConsumer(ctx context.Context, consumerID int, msgChan chan<- amqp.Delivery) {
	// 连接RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Consumer %d: Failed to connect to RabbitMQ: %v", consumerID, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Consumer %d: Failed to open a channel: %v", consumerID, err)
	}
	defer ch.Close()

	// 确保队列存在
	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		log.Fatalf("Consumer %d: Failed to declare a queue: %v", consumerID, err)
	}

	// 设置QoS，限制未确认消息的数量，实现公平分发
	err = ch.Qos(
		batchSize, // prefetch count - 最多同时处理一个批量的消息
		0,         // prefetch size
		false,     // global
	)
	if err != nil {
		log.Fatalf("Consumer %d: Failed to set QoS: %v", consumerID, err)
	}

	// 开始消费
	deliveries, err := ch.Consume(
		queueName,
		fmt.Sprintf("tdengine_consumer_%d", consumerID),
		false, // autoAck - 必须设置为false，我们手动确认
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		log.Fatalf("Consumer %d: Failed to register a consumer: %v", consumerID, err)
	}

	log.Printf("Consumer %d: Started successfully.", consumerID)

	// 将获取到的消息发送到中央channel，供批处理器处理
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				log.Printf("Consumer %d: Delivery channel closed.", consumerID)
				return
			}
			// 将消息发送到处理channel。如果channel满，这里可能会阻塞。
			msgChan <- delivery

		case <-ctx.Done():
			log.Printf("Consumer %d: Shutting down.", consumerID)
			return
		}
	}
}