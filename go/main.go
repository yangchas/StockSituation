package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// 全局配置
const (
	streamHost     = "localhost"
	streamPort     = 5552
	streamUser     = "admin"
	streamPassword = "admin"
	streamName     = "hello-nodejs-stream"

	targetDatabase = "test_db"
	superTableName = "sensors"
	batchSize      = 5000
	batchTimeout   = 1 * time.Second
	numConsumers   = 4
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 初始化 TDengine 连接
	tdConn, err := initTDengine()
	if err != nil {
		log.Fatalf("Failed to initialize TDengine: %v", err)
	}
	defer tdConn.Close()

	// 创建消息通道
	messageChannel := make(chan []byte, batchSize*2)

	var wg sync.WaitGroup

	// 启动批量处理器
	wg.Add(1)
	go func() {
		defer wg.Done()
		startBatchProcessor(ctx, messageChannel, tdConn)
	}()

	// 启动多个 Stream 消费者
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			startStreamConsumer(ctx, consumerID, messageChannel)
		}(i)
	}

	log.Println("Application started successfully")
	log.Printf("Started %d stream consumers", numConsumers)

	// 等待中断信号
	<-ctx.Done()
	log.Println("Shutdown signal received...")

	// 优雅关闭
	close(messageChannel)
	wg.Wait()

	log.Println("Application stopped gracefully.")
}