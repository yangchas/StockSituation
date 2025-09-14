// consumer.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	stream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func startStreamConsumer(ctx context.Context, consumerID int, msgChan chan<- StreamMessage) {
	// 配置连接选项
	envOptions := stream.NewEnvironmentOptions().
		SetHost(streamHost).
		SetPort(streamPort).
		SetUser(streamUser).
		SetPassword(streamPassword).
		SetMaxConsumersPerClient(10)

	// 创建 Stream 环境
	env, err := stream.NewEnvironment(envOptions)
	if err != nil {
		log.Printf("Consumer %d: Failed to create stream environment: %v", consumerID, err)
		return
	}

	// 创建消费者选项 - 使用手动提交模式
	consumerOptions := stream.NewConsumerOptions().
		SetConsumerName(fmt.Sprintf("tdengine_consumer_%d", consumerID)).
		SetCRCCheck(false). // 禁用CRC检查以提高性能
		SetOffset(stream.OffsetSpecification{}.First()). // 从开始消费
		SetManualCommit() // 设置手动提交模式

	// 创建处理函数
	handleMessage := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		// 复制消息数据 - 注意：message.Data 是 [][]byte 类型
		var messageData []byte
		if len(message.Data) > 0 {
			// 通常我们只取第一个数据段
			messageData = make([]byte, len(message.Data[0]))
			copy(messageData, message.Data[0])
		}

		// 创建消息结构
		msg := StreamMessage{
			Data:     messageData,
			Consumer: consumerContext.Consumer,
			Offset:   consumerContext.Consumer.GetOffset(), // 使用 Context 中的偏移量 consumerContext.Consumer.GetOffset()
		}

		// 发送到处理通道
		select {
		case msgChan <- msg:
			log.Printf("Consumer %d: Sent message offset %d, size: %d bytes", 
				consumerID, msg.Offset, len(messageData))
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			log.Printf("Consumer %d: Message channel timeout, offset %d", 
				consumerID, msg.Offset)
		}
	}

	// 创建消费者
	consumer, err := env.NewConsumer(
		streamName,
		handleMessage,
		consumerOptions,
	)

	if err != nil {
		log.Printf("Consumer %d: Failed to create consumer: %v", consumerID, err)
		env.Close()
		return
	}

	log.Printf("Stream Consumer %d: Started successfully", consumerID)

	// 等待关闭信号
	<-ctx.Done()
	
	// 优雅关闭
	if err := consumer.Close(); err != nil {
		log.Printf("Consumer %d: Error closing consumer: %v", consumerID, err)
	}
	
	if err := env.Close(); err != nil {
		log.Printf("Consumer %d: Error closing environment: %v", consumerID, err)
	}

	log.Printf("Stream Consumer %d: Shut down completed", consumerID)
}

// isStreamExistsError 检查是否是流已存在的错误
func isStreamExistsError(err error) bool {
	if err == nil {
		return false
	}
	
	errorMsg := err.Error()
	return errorMsg == "stream already exists" || 
	       errorMsg == "Stream is already available on the cluster" ||
	       errorMsg == "stream already exist" ||
	       errorMsg == "stream already exists with different properties"
}