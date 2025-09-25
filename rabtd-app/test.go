// consumer_streadway_fixed.go
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

    amqp "github.com/streadway/amqp"
    pb "rabtd-app/dataservice"
    "google.golang.org/protobuf/proto"
)

var (
    uri          = flag.String("uri", "amqp://admin:admin@localhost:5672/", "AMQP URI")
    queue        = flag.String("queue", "stream2", "AMQP queue name")
    consumerTag  = flag.String("consumer-tag", "streadway-consumer", "AMQP consumer tag")
    verbose      = flag.Bool("verbose", true, "enable verbose output of message data")
    ErrLog       = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
    Log          = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
)

type Consumer struct {
    uri         string
    queue       string
    tag         string
    conn        *amqp.Connection
    channel     *amqp.Channel
    deliveries  <-chan amqp.Delivery
    done        chan bool
    stopping    int32
    reconnectMu int32 // 重连锁，防止重复重连
}

func main() {
    flag.Parse()
    
    Log.Printf("Starting consumer with streadway/amqp for queue: %s", *queue)
    
    consumer := &Consumer{
        uri:   *uri,
        queue: *queue,
        tag:   *consumerTag,
        done:  make(chan bool),
    }

    setupSignalHandler(consumer)
    
    if err := consumer.Run(); err != nil {
        ErrLog.Fatalf("Failed to start consumer: %s", err)
    }

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
    var err error
    
    Log.Printf("Connecting to RabbitMQ...")
    c.conn, err = amqp.Dial(c.uri)
    if err != nil {
        return fmt.Errorf("Failed to connect: %s", err)
    }

    Log.Printf("Creating channel...")
    c.channel, err = c.conn.Channel()
    if err != nil {
        if c.conn != nil {
            c.conn.Close()
        }
        return fmt.Errorf("Failed to create channel: %s", err)
    }

    // 设置QoS
    err = c.channel.Qos(10, 0, false)
    if err != nil {
        c.cleanup()
        return fmt.Errorf("Failed to set QoS: %s", err)
    }
args := amqp.Table{
        "x-max-length": int64(100000),
    }
    Log.Printf("Declaring queue: %s", c.queue)
    _, err = c.channel.QueueDeclare(c.queue, true, false, false, false, args)
    if err != nil {
        c.cleanup()
        return fmt.Errorf("Failed to declare queue: %s", err)
    }

    // 生成唯一的消费者标签
    uniqueTag := fmt.Sprintf("%s-%d", c.tag, time.Now().UnixNano())
    
    Log.Printf("Starting consumer with tag: %s", uniqueTag)
    c.deliveries, err = c.channel.Consume(
        c.queue,    // queue
        uniqueTag,  // consumer - 使用唯一标签
        true,       // autoAck
        false,      // exclusive
        false,      // noLocal
        false,      // noWait
        nil,        // args
    )
    if err != nil {
        c.cleanup()
        return fmt.Errorf("Failed to start consumer: %s", err)
    }

    // 启动连接监控（只启动一次）
    go c.monitorConnection()
    
    // 启动消息处理
    go c.handleMessages()
    
    Log.Printf("Successfully started consumer")
    return nil
}

func (c *Consumer) monitorConnection() {
    if c.conn == nil {
        return
    }
    
    closeChan := c.conn.NotifyClose(make(chan *amqp.Error, 1))
    
    select {
    case err := <-closeChan:
        if atomic.LoadInt32(&c.stopping) == 0 && err != nil {
            ErrLog.Printf("Connection closed: %v", err)
            Log.Printf("Connection lost, will attempt to reconnect")
            c.safeReconnect()
        }
    }
}

func (c *Consumer) safeReconnect() {
    // 使用原子操作防止重复重连
    if !atomic.CompareAndSwapInt32(&c.reconnectMu, 0, 1) {
        Log.Printf("Reconnection already in progress, skipping...")
        return
    }
    defer atomic.StoreInt32(&c.reconnectMu, 0)

    Log.Printf("Attempting to reconnect...")
    
    // 先清理资源
    c.cleanup()
    
    // 重连循环
    for i := 0; i < 5; i++ {
        Log.Printf("Reconnection attempt %d/5", i+1)
        
        if err := c.Run(); err != nil {
            ErrLog.Printf("Reconnection failed: %s", err)
            if i < 4 {
                waitTime := time.Duration(i+1) * 3 * time.Second
                Log.Printf("Waiting %v before next attempt", waitTime)
                time.Sleep(waitTime)
                continue
            }
            ErrLog.Printf("All reconnection attempts failed")
            c.done <- true
            return
        }
        
        Log.Printf("Reconnected successfully")
        return
    }
}

func (c *Consumer) handleMessages() {
    defer func() {
        if r := recover(); r != nil {
            ErrLog.Printf("Recovered from panic in handleMessages: %v", r)
        }
        
        // 只有在非主动停止的情况下才触发重连
        if atomic.LoadInt32(&c.stopping) == 0 {
            Log.Printf("Message channel closed, will attempt to reconnect")
            c.safeReconnect()
        } else {
            c.done <- true
        }
    }()

    messageCount := 0
    startTime := time.Now()
    lastReport := time.Now()

    for d := range c.deliveries {
        if atomic.LoadInt32(&c.stopping) == 1 {
            break
        }

        messageCount++

        // 定期报告处理进度
        if time.Since(lastReport) > 10*time.Second {
            rate := float64(messageCount) / time.Since(startTime).Seconds()
            Log.Printf("Processed %d messages (%.2f msg/sec)", messageCount, rate)
            lastReport = time.Now()
        }

        if *verbose && messageCount%100 == 0 {
            Log.Printf("Received message %d (%d bytes)", messageCount, len(d.Body))
        }

        // 处理消息
        if err := c.processMessage(d.Body); err != nil {
            ErrLog.Printf("Failed to process message %d: %s", messageCount, err)
            continue
        }

        if *verbose && messageCount%1000 == 0 {
            Log.Printf("Successfully processed %d messages", messageCount)
        }
    }

    Log.Printf("Deliveries channel closed, processed total %d messages", messageCount)
}

func (c *Consumer) processMessage(body []byte) error {
    // 1. 解析消息格式
    header, protoData, err := parseMessageFormat(body)
    if err != nil {
        return fmt.Errorf("Parse message format failed: %w", err)
    }

    if *verbose {
        Log.Printf("Message header: %+v", header)
    }

    // 2. 反序列化 DataRequest
    var dr pb.DataRequest  
    if err := proto.Unmarshal(protoData, &dr); err != nil {  
        return fmt.Errorf("Unmarshal DataRequest failed: %w", err)  
    }  

    // 3. 解压数据
    var batchBytes []byte  
    switch header.Compression {
    case "GZIP", "ZLIB":
        batchBytes, err = decompressZlib(dr.CompressedData)
        if err != nil {
            return fmt.Errorf("Decompress failed: %w", err)
        }
    case "NONE", "":
        batchBytes = dr.CompressedData
    default:
        return fmt.Errorf("Unsupported compression: %s", header.Compression)
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
            
            // 偶尔显示更多详细信息
            if len(db.Records) > 5{// && messageCount%1000 == 0 {
                Log.Printf("Sample records from batch %s:", db.BatchId)
                for i := 0; i < len(db.Records); i++ {//i < 3 && 
                    Log.Printf("  Record %d: %s @ %.2f", i+1, db.Records[i].Symbol, db.Records[i].Lp)
                }
            }
        } else {
            Log.Printf("Batch %s: %d records (empty)", db.BatchId, len(db.Records))
        }
    }

    return nil
}

func (c *Consumer) cleanup() {
    if c.channel != nil {
        c.channel.Close()
        c.channel = nil
    }
    
    if c.conn != nil {
        c.conn.Close()
        c.conn = nil
    }
}

func (c *Consumer) Stop() {
    Log.Printf("Stopping consumer...")
    atomic.StoreInt32(&c.stopping, 1)
    c.cleanup()
    c.done <- true
}

func decompressZlib(data []byte) ([]byte, error) {
    if len(data) == 0 {
        return nil, fmt.Errorf("empty data")
    }
    
    reader := bytes.NewReader(data)
    zr, err := zlib.NewReader(reader)
    if err != nil {
        return nil, fmt.Errorf("create zlib reader failed: %w", err)
    }
    defer zr.Close()
    
    var out bytes.Buffer
    if _, err := io.Copy(&out, zr); err != nil {
        return nil, fmt.Errorf("decompress copy failed: %w", err)
    }
    
    return out.Bytes(), nil
}

func parseMessageFormat(body []byte) (*MessageHeader, []byte, error) {
    if len(body) < 4 {
        return nil, nil, fmt.Errorf("message body too short: %d bytes", len(body))
    }

    headerLen := binary.BigEndian.Uint32(body[:4])
    
    if len(body) < int(4 + headerLen) {
        return nil, nil, fmt.Errorf("message body too short for header: have %d, need %d", 
            len(body), 4+int(headerLen))
    }

 
    headerJSON := body[4 : 4+headerLen]
    var header MessageHeader

    if err := json.Unmarshal(headerJSON, &header); err != nil {
        return nil, nil, fmt.Errorf("JSON header parse failed: %w, data: %s", err, string(headerJSON))
    }

    protoData := body[4+headerLen:]
    // Log.Printf("header:%s",header)
	// Log.Printf("protoData:%s",protoData)
    return &header, protoData, nil
}

type MessageHeader struct {
    ProtoVersion  string `json:"proto_version"`
    Compression   string `json:"compression"`
    BatchID       string `json:"batch_id"`
    RecordCount   int    `json:"record_count"`
    OriginalSize  int    `json:"original_size"`
    CompressedSize int   `json:"compressed_size"`
    Timestamp     int64  `json:"timestamp"`
}