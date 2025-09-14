package main

import stream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

// StreamMessage 包含消息数据和确认信息
type StreamMessage struct {
    Data     []byte
    Consumer *stream.Consumer
    Offset   int64
}

// 全局配置
const (
    streamHost     = "chao"
    streamPort     = 5552
    streamUser     = "admin"
    streamPassword = "admin"
    streamName     = "hello-nodejs-stream"
	tdEngineDSN      = "root:taosdata@tcp(localhost:6030)/"
    targetDatabase = "test_db"
    superTableName = "sensors"
    batchSize      = 10
    batchTimeoutSec = 1
    numConsumers   = 4
)