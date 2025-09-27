// consumer_tdengine_optimized.go
package main

import (
    "bytes"
    "compress/zlib"
    "database/sql"
    "encoding/binary"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "os/signal"
    // "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "syscall"
    "time"

    amqp "github.com/streadway/amqp"
    pb "rabtd-app/dataservice"
    "google.golang.org/protobuf/proto"
    _ "github.com/taosdata/driver-go/v3/taosSql"
)

var (
    uri          = flag.String("uri", "amqp://admin:admin@localhost:5672/", "AMQP URI")
    queue        = flag.String("queue", "stream2", "AMQP queue name")
    consumerTag  = flag.String("consumer-tag", "tdengine-consumer", "AMQP consumer tag")
    tdengineDSN  = flag.String("tdengine-dsn", "root:taosdata@tcp(localhost:6030)/", "TDengine DSN")
    batchSize    = flag.Int("batch-size", 500, "Batch size for TDengine insertion") // 减小批次大小
    workerCount  = flag.Int("workers", 4, "Number of TDengine insertion workers")   // 增加工作线程
    bufferSize   = flag.Int("buffer-size", 50000, "Record channel buffer size")     // 增大缓冲区
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
    reconnectMu int32
    
    // TDengine 相关
    db          *sql.DB
    batchSize   int
    workerCount int
    recordChan  chan *pb.DataRecord
    wg          sync.WaitGroup
}

func main() {
    flag.Parse()
    
    Log.Printf("Starting optimized TDengine consumer for queue: %s", *queue)
    Log.Printf("Configuration: batch-size=%d, workers=%d, buffer-size=%d", 
        *batchSize, *workerCount, *bufferSize)
    
    consumer := &Consumer{
        uri:        *uri,
        queue:      *queue,
        tag:        *consumerTag,
        done:       make(chan bool),
        batchSize:  *batchSize,
        workerCount: *workerCount,
        recordChan: make(chan *pb.DataRecord, *bufferSize), // 使用配置的缓冲区大小
    }

    // 初始化 TDengine
    if err := consumer.initTDengine(); err != nil {
        ErrLog.Fatalf("Failed to initialize TDengine: %s", err)
    }

    setupSignalHandler(consumer)
    
    if err := consumer.Run(); err != nil {
        ErrLog.Fatalf("Failed to start consumer: %s", err)
    }

    <-consumer.done
    Log.Printf("Consumer stopped")
}

func (c *Consumer) initTDengine() error {
    var err error
    
    Log.Printf("Connecting to TDengine: %s", *tdengineDSN)
    c.db, err = sql.Open("taosSql", *tdengineDSN)
    if err != nil {
        return fmt.Errorf("Failed to connect to TDengine: %s", err)
    }

    // 设置连接池参数
    c.db.SetMaxOpenConns(20)
    c.db.SetMaxIdleConns(10)
    c.db.SetConnMaxLifetime(5 * time.Minute)

    // 测试连接
    if err := c.db.Ping(); err != nil {
        return fmt.Errorf("Failed to ping TDengine: %s", err)
    }

    // 创建数据库和表
    if err := c.createDatabaseAndTable(); err != nil {
        return fmt.Errorf("Failed to create database and table: %s", err)
    }

    // 启动多个插入工作线程
    for i := 0; i < c.workerCount; i++ {
        c.wg.Add(1)
        go c.batchInsertWorker(i)
    }

    Log.Printf("TDengine initialized with %d workers", c.workerCount)
    return nil
}

func (c *Consumer) createDatabaseAndTable() error {
    // 创建数据库
    _, err := c.db.Exec("CREATE DATABASE IF NOT EXISTS market_data")
    if err != nil {
        return fmt.Errorf("Failed to create database: %s", err)
    }

    // 使用数据库
    _, err = c.db.Exec("USE market_data")
    if err != nil {
        return fmt.Errorf("Failed to use database: %s", err)
    }

    // 创建超级表
    createStableSQL := `
    CREATE STABLE IF NOT EXISTS stock_data (
        tss TIMESTAMP,
        lp FLOAT,
        o FLOAT,
        h FLOAT,
        l FLOAT,
        lc FLOAT,
        a FLOAT,
        v BIGINT,
        p BIGINT,
        ap1 FLOAT,
        ap2 FLOAT,
        ap3 FLOAT,
        ap4 FLOAT,
        ap5 FLOAT,
        bp1 FLOAT,
        bp2 FLOAT,
        bp3 FLOAT,
        bp4 FLOAT,
        bp5 FLOAT,
        av1 BIGINT,
        av2 BIGINT,
        av3 BIGINT,
        av4 BIGINT,
        av5 BIGINT,
        bv1 BIGINT,
        bv2 BIGINT,
        bv3 BIGINT,
        bv4 BIGINT,
        bv5 BIGINT
    ) TAGS (symbol BINARY(20), exchange BINARY(10), market BINARY(10))
    `
    _, err = c.db.Exec(createStableSQL)
    if err != nil {
        return fmt.Errorf("Failed to create super table: %s", err)
    }

    Log.Printf("Database and table created successfully")
    return nil
}

func (c *Consumer) batchInsertWorker(workerID int) {
    defer c.wg.Done()
    
    batchBuffer := make([]*pb.DataRecord, 0, c.batchSize)
    ticker := time.NewTicker(100 * time.Millisecond) // 更频繁的刷新
    lastFlush := time.Now()
    
    defer ticker.Stop()

    for {
        select {
        case record, ok := <-c.recordChan:
            if !ok {
                // 通道关闭，插入剩余数据
                if len(batchBuffer) > 0 {
                    c.insertBatch(batchBuffer, workerID)
                }
                Log.Printf("Worker %d stopped", workerID)
                return
            }
            
            batchBuffer = append(batchBuffer, record)
            
            // 达到批次大小时立即插入
            if len(batchBuffer) >= c.batchSize {
                c.insertBatch(batchBuffer, workerID)
                batchBuffer = batchBuffer[:0]
                lastFlush = time.Now()
            }
            
        case <-ticker.C:
            // 定时插入，避免数据积压
            if len(batchBuffer) > 0 && time.Since(lastFlush) > 50*time.Millisecond {
                c.insertBatch(batchBuffer, workerID)
                batchBuffer = batchBuffer[:0]
                lastFlush = time.Now()
            }
        }
    }
}

func (c *Consumer) insertBatch(records []*pb.DataRecord, workerID int) {
    if len(records) == 0 {
        return
    }

    startTime := time.Now()
    
    // 按 symbol 分组
    recordsBySymbol := make(map[string][]*pb.DataRecord)
    for _, record := range records {
        symbol := record.GetSymbol()
        recordsBySymbol[symbol] = append(recordsBySymbol[symbol], record)
    }

    totalInserted := 0
    var wg sync.WaitGroup
    var mu sync.Mutex
    
    // 并行插入不同symbol的数据
    for symbol, symbolRecords := range recordsBySymbol {
        if len(symbolRecords) == 0 {
            continue
        }

        wg.Add(1)
        go func(symbol string, records []*pb.DataRecord) {
            defer wg.Done()
            
            inserted := c.insertSymbolRecords(symbol, records)
            mu.Lock()
            totalInserted += inserted
            mu.Unlock()
        }(symbol, symbolRecords)
    }
    
    wg.Wait()

    if *verbose && totalInserted > 0 {
        duration := time.Since(startTime)
        Log.Printf("Worker %d: Inserted %d records in %v (%.2f records/sec)", 
            workerID, totalInserted, duration, float64(totalInserted)/duration.Seconds())
    }
}

func (c *Consumer) insertSymbolRecords(symbol string, records []*pb.DataRecord) int {
    if len(records) == 0 {
        return 0
    }

    // 使用第一个记录获取 exchange 和 market
    firstRecord := records[0]
    exchange := firstRecord.GetExchange()
    market := firstRecord.GetMarket()
    
    // 创建子表（如果不存在）
    tableName := "market_data.t_" + sanitizeSymbol(symbol)
    createTableSQL := fmt.Sprintf(
        "CREATE TABLE IF NOT EXISTS %s USING market_data.stock_data TAGS ('%s', '%s', '%s')",
        tableName, symbol, exchange, market,
    )
    
    _, err := c.db.Exec(createTableSQL)
    if err != nil {
        ErrLog.Printf("Failed to create table for symbol %s: %s", symbol, err)
        return 0
    }

    // 构建批量插入 SQL
    var valueStrings []string
    var valueArgs []interface{}
    
    validRecordCount := 0
    for _, record := range records {
        // 验证和处理时间戳
        timestamp := record.GetXTs()
        valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        

            valueArgs = append(valueArgs,
                timestamp, // 转换为 time.Time
                record.GetLp(),
                record.GetO(),
                record.GetH(),
                record.GetL(),
                record.GetLc(),
                record.GetA(),
                record.GetV(),
                record.GetP(),
                record.GetAp1(),
                record.GetAp2(),
                record.GetAp3(),
                record.GetAp4(),
                record.GetAp5(),
                record.GetBp1(),
                record.GetBp2(),
                record.GetBp3(),
                record.GetBp4(),
                record.GetBp5(),
                record.GetAv1(),
                record.GetAv2(),
                record.GetAv3(),
                record.GetAv4(),
                record.GetAv5(),
                record.GetBv1(),
                record.GetBv2(),
                record.GetBv3(),
                record.GetBv4(),
                record.GetBv5(),
            )
        validRecordCount++
    }

    if validRecordCount == 0 {
        return 0
    }

    insertSQL := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueStrings, ","))
    // fmt.Println(valueArgs)
    _, err = c.db.Exec(insertSQL, valueArgs...)
    if err != nil {
        ErrLog.Printf("Failed to insert %d records for symbol %s: %s", validRecordCount, symbol, err)
        return 0
    }

    return validRecordCount
}

// 其他函数保持不变，但添加背压机制到消息处理
func (c *Consumer) handleMessages(deliveries <-chan amqp.Delivery) {
    defer func() {
        if r := recover(); r != nil {
            ErrLog.Printf("Recovered from panic in handleMessages: %v", r)
        }
        
        if atomic.LoadInt32(&c.stopping) == 0 {
            Log.Printf("Message channel closed, will attempt to reconnect")
            c.safeReconnect()
        } else {
            c.done <- true
        }
    }()

    messageCount := 0
    recordCount := 0
    startTime := time.Now()
    lastReport := time.Now()
    droppedCount := 0

    for d := range deliveries {
        if atomic.LoadInt32(&c.stopping) == 1 {
            break
        }

        messageCount++

        // 处理消息
        recordsProcessed, dropped, err := c.processMessage(d.Body)
        if err != nil {
            ErrLog.Printf("Failed to process message %d: %s", messageCount, err)
            continue
        }

        recordCount += recordsProcessed
        droppedCount += dropped

        // 定期报告处理进度和背压情况
        if time.Since(lastReport) > 5*time.Second { // 更频繁的报告
            rate := float64(messageCount) / time.Since(startTime).Seconds()
            recordRate := float64(recordCount) / time.Since(startTime).Seconds()
            channelUsage := float64(len(c.recordChan)) / float64(cap(c.recordChan)) * 100
            
            Log.Printf("Processed %d messages, %d records, dropped %d (%.2f msg/sec, %.2f records/sec, channel: %.1f%%)", 
                messageCount, recordCount, droppedCount, rate, recordRate, channelUsage)
            
            // 如果通道使用率过高，动态调整插入策略
            if channelUsage > 80 {
                Log.Printf("High channel usage detected, consider increasing workers or buffer size")
            }
            
            lastReport = time.Now()
        }

        if *verbose && messageCount%100 == 0 {
            Log.Printf("Received message %d (%d bytes)", messageCount, len(d.Body))
        }
    }

    Log.Printf("Deliveries channel closed, processed total %d messages, %d records, dropped %d", 
        messageCount, recordCount, droppedCount)
}

func (c *Consumer) processMessage(body []byte) (int, int, error) {
    // 1. 解析消息格式
    header, protoData, err := parseMessageFormat(body)
    if err != nil {
        return 0, 0, fmt.Errorf("Parse message format failed: %w", err)
    }

    // 2. 反序列化 DataRequest
    var dr pb.DataRequest  
    if err := proto.Unmarshal(protoData, &dr); err != nil {  
        return 0, 0, fmt.Errorf("Unmarshal DataRequest failed: %w", err)  
    }  

    // 3. 解压数据
    var batchBytes []byte  
    switch header.Compression {
    case "GZIP", "ZLIB":
        batchBytes, err = decompressZlib(dr.CompressedData)
        if err != nil {
            return 0, 0, fmt.Errorf("Decompress failed: %w", err)
        }
    case "NONE", "":
        batchBytes = dr.CompressedData
    default:
        return 0, 0, fmt.Errorf("Unsupported compression: %s", header.Compression)
    }

    // 4. 反序列化 DataBatch
    var db pb.DataBatch
    if err := proto.Unmarshal(batchBytes, &db); err != nil {
        return 0, 0, fmt.Errorf("Unmarshal DataBatch failed: %w", err)
    }
    
    // 5. 发送记录到 TDengine 插入队列（带背压控制）
    recordsProcessed := 0
    dropped := 0
    
    for _, record := range db.Records {
        // 使用带超时的非阻塞发送，避免长时间阻塞
        select {
        case c.recordChan <- record:
            recordsProcessed++
        default:
            // 通道已满，尝试等待一小段时间
            select {
            case c.recordChan <- record:
                recordsProcessed++
            case <-time.After(10 * time.Millisecond): // 更短的超时
                // 仍然无法发送，记录丢弃
                dropped++
                if dropped%100 == 0 { // 每丢弃100条记录才记录一次，避免日志过多
                    ErrLog.Printf("Dropped %d records due to full buffer", dropped)
                }
            }
        }
    }
    
    // 6. 记录处理结果
    if *verbose && len(db.Records) > 0 {
        Log.Printf("Batch %s: %d records, inserted %d, dropped %d, first: %s @ %.2f", 
            db.BatchId, len(db.Records), recordsProcessed, dropped, 
            db.Records[0].Symbol, db.Records[0].Lp,db.Records[0].XTs)
    }

    return recordsProcessed, dropped, nil
}

// 优雅停止，确保所有数据都被处理
func (c *Consumer) Stop() {
    Log.Printf("Stopping consumer gracefully...")
    atomic.StoreInt32(&c.stopping, 1)
    
    // 等待一段时间让剩余消息被处理
    time.Sleep(2 * time.Second)
    
    // 关闭记录通道
    close(c.recordChan)
    
    // 等待所有工作线程完成
    Log.Printf("Waiting for insertion workers to finish...")
    c.wg.Wait()
    
    Log.Printf("All insertion workers finished")
    
    c.cleanup()
    
    // 关闭数据库连接
    if c.db != nil {
        c.db.Close()
    }
    
    c.done <- true
    Log.Printf("Consumer stopped gracefully")
}
// 清理 symbol，确保可以作为表名
func sanitizeSymbol(symbol string) string {
    // 移除或替换无效字符
    sanitized := strings.Map(func(r rune) rune {
        if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || 
           (r >= '0' && r <= '9') || r == '_' {
            return r
        }
        return '_'
    }, symbol)
    
    // 确保不以数字开头
    if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
        sanitized = "s_" + sanitized
    }
    
    return sanitized
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

// func (c *Consumer) Run() error {
//     var err error
    
//     Log.Printf("Connecting to RabbitMQ...")
//     c.conn, err = amqp.Dial(c.uri)
//     if err != nil {
//         return fmt.Errorf("Failed to connect: %s", err)
//     }

//     Log.Printf("Creating channel...")
//     c.channel, err = c.conn.Channel()
//     if err != nil {
//         if c.conn != nil {
//             c.conn.Close()
//         }
//         return fmt.Errorf("Failed to create channel: %s", err)
//     }

//     // 设置QoS
//     err = c.channel.Qos(10, 0, false)
//     if err != nil {
//         c.cleanup()
//         return fmt.Errorf("Failed to set QoS: %s", err)
//     }
// 	args := amqp.Table{
//         "x-max-length": int64(100000),
//     }
//     Log.Printf("Declaring queue: %s", c.queue)
//     _, err = c.channel.QueueDeclare(c.queue, true, false, false, false, args)
//     if err != nil {
//         c.cleanup()
//         return fmt.Errorf("Failed to declare queue: %s", err)
//     }

//     // 生成唯一的消费者标签
//     uniqueTag := fmt.Sprintf("%s-%d", c.tag, time.Now().UnixNano())
    
//     Log.Printf("Starting consumer with tag: %s", uniqueTag)
//     c.deliveries, err = c.channel.Consume(
//         c.queue,    // queue
//         uniqueTag,  // consumer
//         true,       // autoAck
//         false,      // exclusive
//         false,      // noLocal
//         false,      // noWait
//         nil,        // args
//     )
//     if err != nil {
//         c.cleanup()
//         return fmt.Errorf("Failed to start consumer: %s", err)
//     }

//     // 启动连接监控
//     go c.monitorConnection()
    
//     // 启动消息处理
//     go c.handleMessages()
    
//     Log.Printf("Successfully started consumer")
//     return nil
// }

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


// 修改重连逻辑，避免在重连时丢失数据
func (c *Consumer) safeReconnect() {
    if !atomic.CompareAndSwapInt32(&c.reconnectMu, 0, 1) {
        Log.Printf("Reconnection already in progress, skipping...")
        return
    }
    defer atomic.StoreInt32(&c.reconnectMu, 0)

    Log.Printf("Attempting to reconnect...")
    
    // 先停止当前的消息处理，但保持插入工作线程运行
    c.cleanupAMQP()
    
    for i := 0; i < 5; i++ {
        Log.Printf("Reconnection attempt %d/5", i+1)
        
        if err := c.connectAMQP(); err != nil {
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

// 分离AMQP连接清理逻辑
func (c *Consumer) cleanupAMQP() {
    if c.channel != nil {
        c.channel.Close()
        c.channel = nil
    }
    
    if c.conn != nil {
        c.conn.Close()
        c.conn = nil
    }
}

// 分离AMQP连接逻辑
func (c *Consumer) connectAMQP() error {
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

    // 设置QoS，限制预取数量以避免内存溢出
    err = c.channel.Qos(5, 0, false) // 减少预取数量
    if err != nil {
        c.cleanupAMQP()
        return fmt.Errorf("Failed to set QoS: %s", err)
    }

    Log.Printf("Declaring queue: %s", c.queue)
        	args := amqp.Table{
        "x-max-length": int64(100000),
    }
    _, err = c.channel.QueueDeclare(c.queue, true, false, false, false, args)
    if err != nil {
        c.cleanupAMQP()
        return fmt.Errorf("Failed to declare queue: %s", err)
    }

    // 生成唯一的消费者标签
    uniqueTag := fmt.Sprintf("%s-%d", c.tag, time.Now().UnixNano())

    Log.Printf("Starting consumer with tag: %s", uniqueTag)
    c.deliveries, err = c.channel.Consume(
        c.queue,    // queue
        uniqueTag,  // consumer
        true,       // autoAck
        false,      // exclusive
        false,      // noLocal
        false,      // noWait
        nil,        // args
    )
    if err != nil {
        c.cleanupAMQP()
        return fmt.Errorf("Failed to start consumer: %s", err)
    }

    // 启动连接监控
    go c.monitorConnection()
    // 启动消息处理
    go c.handleMessages(c.deliveries)
    Log.Printf("AMQP connection established successfully")
    return nil
}

func (c *Consumer) Run() error {
    return c.connectAMQP()
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