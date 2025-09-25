// consumer_amqp.go
package main

import (
    "bytes"
    "compress/zlib"
    "encoding/binary"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "os/signal"
    "sync/atomic"
    "syscall"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    pb "rabtd-app/dataservice"
    "google.golang.org/protobuf/proto"
)

var (
    uri          = flag.String("uri", "amqp://admin:admin@localhost:5672/", "AMQP URI")
    queue        = flag.String("queue", "stream2", "AMQP queue name")
    consumerTag  = flag.String("consumer-tag", "data-consumer", "AMQP consumer tag")
    autoAck      = flag.Bool("auto_ack", false, "enable message auto-ack")
    verbose      = flag.Bool("verbose", true, "enable verbose output of message data")
    ErrLog       = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
    Log          = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
)

// MessageHeader 消息头部结构
type MessageHeader struct {
    ProtoVersion  string `json:"proto_version"`
    Compression   string `json:"compression"`
    BatchID       string `json:"batch_id"`
    RecordCount   int    `json:"record_count"`
    OriginalSize  int    `json:"original_size"`
    CompressedSize int   `json:"compressed_size"`
    Timestamp     int64  `json:"timestamp"`
}

type Consumer struct {
    uri         string
    queue       string
    tag         string
    conn        *amqp.Connection
    channel     *amqp.Channel
    deliveries  <-chan amqp.Delivery
    done        chan bool
    stopping    int32 // 原子操作，表示是否正在停止
    reconnectMu int32 // 重连锁，防止重复重连
}

func init() {
    flag.Parse()
}

func main() {
    Log.Printf("Starting consumer for queue: %s", *queue)
    
    consumer := &Consumer{
        uri:   *uri,
        queue: *queue,
        tag:   *consumerTag,
        done:  make(chan bool),
    }

    // 设置信号处理
    setupSignalHandler(consumer)
    
    // 启动消费者
    if err := consumer.Run(); err != nil {
        ErrLog.Fatalf("Failed to start consumer: %s", err)
    }

    // 等待完成信号
    <-consumer.done
    Log.Printf("Consumer stopped")
}

func setupSignalHandler(consumer *Consumer) {
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        sig := <-c
        Log.Printf("Received signal: %s", sig)
        consumer.Stop()
    }()
}

func (c *Consumer) Run() error {
    // 连接RabbitMQ
    if err := c.connect(); err != nil {
        return err
    }

    // 启动消息处理循环
    go c.handleMessages()

    return nil
}

func (c *Consumer) connect() error {
    var err error
    
    Log.Printf("Connecting to RabbitMQ...")
    c.conn, err = amqp.Dial(c.uri)
    if err != nil {
        return fmt.Errorf("Failed to connect: %s", err)
    }

    Log.Printf("Creating channel...")
    c.channel, err = c.conn.Channel()
    if err != nil {
        c.conn.Close()
        return fmt.Errorf("Failed to create channel: %s", err)
    }

    args := amqp.Table{
        "x-max-length": int64(100000),
    }
    
    // 声明队列
    Log.Printf("Declaring queue: %s", c.queue)
    _, err = c.channel.QueueDeclare(
        c.queue, // name
        true,    // durable
        false,   // autoDelete
        false,   // exclusive
        false,   // noWait
        args,    // arguments
    )
    if err != nil {
        c.channel.Close()
        c.conn.Close()
        return fmt.Errorf("Failed to declare queue: %s", err)
    }

    // 开始消费
    Log.Printf("Starting consumer with tag: %s", c.tag)
    c.deliveries, err = c.channel.Consume(
        c.queue, // queue
        c.tag,   // consumer
        *autoAck,// autoAck
        false,   // exclusive
        false,   // noLocal
        false,   // noWait
        nil,     // args - 这里应该是nil，不是args
    )
    if err != nil {
        c.channel.Close()
        c.conn.Close()
        return fmt.Errorf("Failed to start consumer: %s", err)
    }

    Log.Printf("Successfully connected and started consumer")
    return nil
}

func (c *Consumer) handleMessages() {
    defer func() {
        if r := recover(); r != nil {
            ErrLog.Printf("Recovered from panic in handleMessages: %v", r)
        }
        // 只有在非主动停止的情况下才触发重连
        if atomic.LoadInt32(&c.stopping) == 0 {
            c.reconnect()
        } else {
            c.done <- true
        }
    }()

    messageCount := 0
    lastReport := time.Now()

    for d := range c.deliveries {
        // 检查是否正在停止
        if atomic.LoadInt32(&c.stopping) == 1 {
            break
        }

        messageCount++
        
        // 定期报告处理进度
        if time.Since(lastReport) > 30*time.Second {
            Log.Printf("Processed %d messages", messageCount)
            lastReport = time.Now()
        }

        if *verbose {
            Log.Printf("Received message %d (%d bytes)", messageCount, len(d.Body))
        }

        // 处理消息
        if err := c.processMessage(d); err != nil {
            ErrLog.Printf("Failed to process message %d: %s", messageCount, err)
            if !*autoAck {
                // 处理失败，拒绝消息（不重新入队）
                if err := d.Nack(false, false); err != nil {
                    ErrLog.Printf("Failed to Nack message: %s", err)
                    // Nack失败，连接可能已断开，退出循环触发重连
                    break
                }
            }
            continue
        }

        // 确认消息
        if !*autoAck {
            if err := d.Ack(false); err != nil {
                ErrLog.Printf("Failed to Ack message: %s", err)
                // Ack失败，连接可能已断开，退出循环触发重连
                break
            }
        }

        if *verbose && messageCount%100 == 0 {
            Log.Printf("Successfully processed %d messages", messageCount)
        }
    }

    Log.Printf("Message channel closed, processed total %d messages", messageCount)
}

func (c *Consumer) processMessage(d amqp.Delivery) error {
    // 1. 解析消息格式
    header, protoData, err := parseMessageFormat(d.Body)
    if err != nil {
        return fmt.Errorf("Parse message format failed: %w", err)
    }

    // 2. 反序列化 DataRequest
    var dr pb.DataRequest  
    if err := proto.Unmarshal(protoData, &dr); err != nil {  
        return fmt.Errorf("Unmarshal DataRequest failed: %w", err)  
    }  

    // 3. 解压数据
    var batchBytes []byte  
    switch header.Compression {
    case "GZIP":
        batchBytes, err = decompressZlib(dr.CompressedData)
        if err != nil {
            return fmt.Errorf("Decompress failed: %w", err)
        }
    default:
        batchBytes = dr.CompressedData
    }

    // 4. 反序列化 DataBatch
    var db pb.DataBatch
    if err := proto.Unmarshal(batchBytes, &db); err != nil {
        return fmt.Errorf("Unmarshal DataBatch failed: %w", err)
    }
    
    // 5. 记录处理结果
    if *verbose {
        if len(db.Records) > 0 {
            Log.Printf("Batch %s: %d records, first: %s @ %.2f", 
                db.BatchId, len(db.Records), db.Records[0].Symbol, db.Records[0].Lp)
        } else {
            Log.Printf("Batch %s: %d records (empty)", db.BatchId, len(db.Records))
        }
    }

    return nil
}

func (c *Consumer) reconnect() {
    // 使用原子操作防止重复重连
    if !atomic.CompareAndSwapInt32(&c.reconnectMu, 0, 1) {
        Log.Printf("Reconnection already in progress")
        return
    }
    defer atomic.StoreInt32(&c.reconnectMu, 0)

    Log.Printf("Attempting to reconnect...")
    
    // 关闭现有连接
    c.cleanup()
    
    // 重连循环
    for i := 0; i < 5; i++ {
        Log.Printf("Reconnection attempt %d/5", i+1)
        
        if err := c.connect(); err != nil {
            ErrLog.Printf("Reconnection failed: %s", err)
            if i < 4 {
                waitTime := time.Duration(i+1) * 5 * time.Second
                Log.Printf("Waiting %v before next attempt", waitTime)
                time.Sleep(waitTime)
                continue
            }
            ErrLog.Printf("All reconnection attempts failed")
            c.done <- true
            return
        }
        
        // 重新启动消息处理
        go c.handleMessages()
        Log.Printf("Reconnected successfully")
        return
    }
}

func (c *Consumer) cleanup() {
    if c.channel != nil {
        if err := c.channel.Close(); err != nil {
            Log.Printf("Error closing channel: %s", err)
        }
        c.channel = nil
    }
    
    if c.conn != nil {
        if err := c.conn.Close(); err != nil {
            Log.Printf("Error closing connection: %s", err)
        }
        c.conn = nil
    }
}

func (c *Consumer) Stop() {
    Log.Printf("Stopping consumer...")
    atomic.StoreInt32(&c.stopping, 1)
    c.cleanup()
    c.done <- true
}

// 辅助函数保持不变
func decompressZlib(data []byte) ([]byte, error) {
    reader := bytes.NewReader(data)
    zr, err := zlib.NewReader(reader)
    if err != nil {
        return nil, err
    }
    defer zr.Close()
    
    var out bytes.Buffer
    _, err = io.Copy(&out, zr)
    if err != nil {
        return nil, err
    }
    return out.Bytes(), nil
}

func parseMessageFormat(body []byte) (*MessageHeader, []byte, error) {
    if len(body) < 4 {
        return nil, nil, fmt.Errorf("Message body too short")
    }

    headerLen := binary.BigEndian.Uint32(body[:4])
    
    if len(body) < int(4 + headerLen) {
        return nil, nil, fmt.Errorf("Message body too short for header")
    }

    headerJSON := body[4 : 4+headerLen]
    var header MessageHeader
    if err := json.Unmarshal(headerJSON, &header); err != nil {
        return nil, nil, fmt.Errorf("JSON header parse failed: %w", err)
    }

    protoData := body[4+headerLen:]
    return &header, protoData, nil
}