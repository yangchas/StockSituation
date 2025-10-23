// consumer.go
package main

import (
	"context"
	"fmt"
	"log"
	// "encoding/binary"
	// "encoding/json"
	// "time"

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
		// SetCRCCheck(false). // 禁用CRC检查以提高性能
		SetOffset(stream.OffsetSpecification{}.Next()) // 从开始消费
		// SetManualCommit() // 设置手动提交模式
		// debugRawMessage:=func(consumerContext stream.ConsumerContext,data []byte) {
		// 	fmt.Println("=== 原始消息分析 ===")
		// 	fmt.Printf("总长度: %d 字节\n", len(data))
			
		// 	if len(data) >= 4 {
		// 		// 尝试解析头部长度
		// 		headerLen := binary.BigEndian.Uint32(data[:4])
		// 		fmt.Printf("头部长度字段: %d\n", headerLen)
				
		// 		if len(data) >= int(4+headerLen) {
		// 			// 尝试解析 JSON 头部
		// 			headerData := data[4 : 4+headerLen]
		// 			fmt.Printf("头部数据: %s\n", string(headerData))
					
		// 			var header map[string]interface{}
		// 			if err := json.Unmarshal(headerData, &header); err == nil {
		// 				fmt.Printf("解析的头部: %+v\n", header)
		// 			} else {
		// 				fmt.Printf("头部解析失败: %v\n", err)
		// 			}
					
		// 			// 显示 Protobuf 数据信息
		// 			protoData := data[4+headerLen:]
		// 			fmt.Printf("Protobuf 数据长度: %d 字节\n", len(protoData))
		// 			if len(protoData) > 0 && len(protoData) <= 50 {
		// 				fmt.Printf("Protobuf 数据 (hex): %x\n", protoData)
		// 			}
		// 		}
		// 	}
			
		// 	fmt.Printf("完整数据 (前100字节): %x\n", data[:min(100, len(data))])
		// 	fmt.Println("=== 分析结束 ===")
		// }
	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Println("\n=== RabbitMQ Stream Message ===")
		fmt.Printf("Stream: %s\n", consumerContext.Consumer.GetStreamName())
		fmt.Printf("Offset: %d\n", consumerContext.Consumer.GetOffset())
		
		if message == nil {
			fmt.Println("Message is nil")
			return
		}

		fmt.Printf("Data segments: %d\n", len(message.Data))

		if message == nil {
			log.Println("Received nil message")
			return
		}
		
		// 检查Data字段
		if message.Data == nil || len(message.Data) == 0 {
			log.Printf("Empty message data. Message properties: %+v\n", message.Properties)
			log.Printf("Message application properties: %+v\n", message.ApplicationProperties)
			log.Printf("Message annotations: %+v\n", message.Annotations)
			return
		}
		
		// 尝试以字符串形式打印消息
		log.Printf("Stream: %s - Received message: %s\n", 
			consumerContext.Consumer.GetStreamName(), message.Data)
		
		
		// 打印完整的消息结构用于调试
		log.Printf("Full message structure:\n")
		log.Printf("  Data: %v\n", message.Data)
		log.Printf("  Data (string): %s\n", message.Data)
		log.Printf("  Properties: %+v\n", message.Properties)
		log.Printf("  ApplicationProperties: %+v\n", message.ApplicationProperties)
	}
		// 检查消息是否为空
    
	// consumer, err := env.NewConsumer(streamName, messagesHandler, 
	// 		stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()))
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	// 创建处理函数
	// handleMessage := func(consumerContext stream.ConsumerContext, message *amqp.Message) {

	// 	log.Printf("%s",message.Data);
	// 	return
	// 	// 复制消息数据 - 注意：message.Data 是 [][]byte 类型
	// 	var messageData []byte
	// 	if len(message.Data) > 0 {
	// 		// 通常我们只取第一个数据段
	// 		messageData = make([]byte, len(message.Data[0]))
	// 		copy(messageData, message.Data[0])
	// 	}

	// 	// 创建消息结构
	// 	msg := StreamMessage{
	// 		Data:     messageData,
	// 		Consumer: consumerContext.Consumer,
	// 		Offset:   consumerContext.Consumer.GetOffset(), // 使用 Context 中的偏移量 consumerContext.Consumer.GetOffset()
	// 	}

	// 	// 发送到处理通道
	// 	select {
	// 	case msgChan <- msg:
	// 		log.Printf("Consumer %d: Sent message offset %d, size: %d bytes", 
	// 			consumerID, msg.Offset, len(messageData))
	// 	case <-ctx.Done():
	// 		return
	// 	case <-time.After(100 * time.Millisecond):
	// 		log.Printf("Consumer %d: Message channel timeout, offset %d", 
	// 			consumerID, msg.Offset)
	// 	}
	// }

	// 创建消费者
	consumer, err := env.NewConsumer(
		streamName,
		messagesHandler,// handleMessage,
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