// consumer_tdengine_fixed.go
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
    batchSize    = flag.Int("batch-size", 500, "Batch size for TDengine insertion")
    workerCount  = flag.Int("workers", 4, "Number of TDengine insertion workers")
    bufferSize   = flag.Int("buffer-size", 50000, "Record channel buffer size")
    flushTimeout = flag.Duration("flush-timeout", 1*time.Second, "Timeout for flushing remaining data")
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
    done        chan error
    stopping    int32
    
    // TDengine 相关
    db          *sql.DB
    batchSize   int
    workerCount int
    recordChan  chan *pb.DataRecord
    wg          sync.WaitGroup
    
    // 连接管理
    reconnectMu    sync.Mutex
    shouldReconnect bool
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

func main() {
    flag.Parse()
    
    Log.Printf("Starting TDengine consumer for queue: %s", *queue)
    
    consumer := &Consumer{
        uri:        *uri,
        queue:      *queue,
        tag:        *consumerTag,
        done:       make(chan error),
        batchSize:  *batchSize,
        workerCount: *workerCount,
        recordChan: make(chan *pb.DataRecord, *bufferSize),
        shouldReconnect: true,
    }

    if err := consumer.initTDengine(); err != nil {
        ErrLog.Fatalf("Failed to initialize TDengine: %s", err)
    }

    setupSignalHandler(consumer)
    
    // 启动主循环
    go consumer.mainLoop()

    // 等待退出信号
    err := <-consumer.done
    if err != nil {
        ErrLog.Printf("Consumer exited with error: %s", err)
    } else {
        Log.Printf("Consumer stopped gracefully")
    }
}

func (c *Consumer) mainLoop() {
    defer close(c.done)
    
    reconnectDelay := time.Second
    maxReconnectDelay := 30 * time.Second
    
    for {
        if atomic.LoadInt32(&c.stopping) == 1 {
            return
        }
        
        err := c.connectAndConsume()
        if err != nil {
            ErrLog.Printf("Connection failed: %s", err)
            
            if !c.shouldReconnect {
                c.done <- err
                return
            }
            
            Log.Printf("Reconnecting in %v...", reconnectDelay)
            time.Sleep(reconnectDelay)
            
            // 指数退避
            reconnectDelay *= 2
            if reconnectDelay > maxReconnectDelay {
                reconnectDelay = maxReconnectDelay
            }
            continue
        }
        
        // 连接成功，重置重连延迟
        reconnectDelay = time.Second
    }
}

func (c *Consumer) connectAndConsume() error {
    c.reconnectMu.Lock()
    defer c.reconnectMu.Unlock()
    
    // 清理旧连接
    c.cleanupAMQP()
    
    Log.Printf("Connecting to RabbitMQ...")
    conn, err := amqp.Dial(c.uri)
    if err != nil {
        return fmt.Errorf("Failed to connect: %s", err)
    }
    c.conn = conn

    Log.Printf("Creating channel...")
    channel, err := conn.Channel()
    if err != nil {
        conn.Close()
        return fmt.Errorf("Failed to create channel: %s", err)
    }
    c.channel = channel

    err = channel.Qos(10, 0, false)
    if err != nil {
        return fmt.Errorf("Failed to set QoS: %s", err)
    }

    Log.Printf("Declaring queue: %s", c.queue)
    args := amqp.Table{
        "x-max-length": int64(100000),
    }
    _, err = channel.QueueDeclare(c.queue, true, false, false, false, args)
    if err != nil {
        return fmt.Errorf("Failed to declare queue: %s", err)
    }

    uniqueTag := fmt.Sprintf("%s-%d", c.tag, time.Now().UnixNano())
    Log.Printf("Starting consumer with tag: %s", uniqueTag)
    
    deliveries, err := channel.Consume(
        c.queue,
        uniqueTag,
        true, // autoAck
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        return fmt.Errorf("Failed to start consumer: %s", err)
    }
    c.deliveries = deliveries

    // 启动连接监控
    go c.monitorConnection(conn)
    
    Log.Printf("AMQP connection established successfully")
    
    // 处理消息
    return c.handleMessages(deliveries)
}

func (c *Consumer) monitorConnection(conn *amqp.Connection) {
    closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))
    
    select {
    case err := <-closeChan:
        if atomic.LoadInt32(&c.stopping) == 0 && err != nil {
            ErrLog.Printf("Connection closed: %v", err)
            // 不直接触发重连，让主循环处理
        }
    case <-time.After(100 * time.Millisecond):
        // 避免在连接正常时阻塞
    }
}

func (c *Consumer) handleMessages(deliveries <-chan amqp.Delivery) error {
    messageCount := 0
    recordCount := 0
    startTime := time.Now()
    lastReport := time.Now()

    for {
        select {
        case d, ok := <-deliveries:
            if !ok {
                Log.Printf("Deliveries channel closed, processed %d messages, %d records", 
                    messageCount, recordCount)
                return fmt.Errorf("deliveries channel closed")
            }
            
            if atomic.LoadInt32(&c.stopping) == 1 {
                return nil
            }

            messageCount++
            recordsProcessed, _, err := c.processMessage(d.Body)
            if err != nil {
                ErrLog.Printf("Failed to process message %d: %s", messageCount, err)
                continue
            }

            recordCount += recordsProcessed

            if time.Since(lastReport) > 10*time.Second {
                rate := float64(messageCount) / time.Since(startTime).Seconds()
                recordRate := float64(recordCount) / time.Since(startTime).Seconds()
                Log.Printf("Processed %d messages, %d records (%.2f msg/sec, %.2f records/sec)", 
                    messageCount, recordCount, rate, recordRate)
                lastReport = time.Now()
            }

            if *verbose && messageCount%1000 == 0 {
                Log.Printf("Received message %d", messageCount)
            }
        }
    }
}

// 以下函数保持不变，只修改上面的核心逻辑
func (c *Consumer) initTDengine() error {
    var err error
    
    Log.Printf("Connecting to TDengine: %s", *tdengineDSN)
    c.db, err = sql.Open("taosSql", *tdengineDSN)
    if err != nil {
        return fmt.Errorf("Failed to connect to TDengine: %s", err)
    }

    c.db.SetMaxOpenConns(20)
    c.db.SetMaxIdleConns(10)
    c.db.SetConnMaxLifetime(5 * time.Minute)

    if err := c.db.Ping(); err != nil {
        return fmt.Errorf("Failed to ping TDengine: %s", err)
    }

    if err := c.createDatabaseAndTable(); err != nil {
        return fmt.Errorf("Failed to create database and table: %s", err)
    }

    for i := 0; i < c.workerCount; i++ {
        c.wg.Add(1)
        go c.batchInsertWorker(i)
    }

    Log.Printf("TDengine initialized with %d workers", c.workerCount)
    return nil
}

func (c *Consumer) createDatabaseAndTable() error {
    _, err := c.db.Exec("CREATE DATABASE IF NOT EXISTS market_data")
    if err != nil {
        return fmt.Errorf("Failed to create database: %s", err)
    }

    _, err = c.db.Exec("USE market_data")
    if err != nil {
        return fmt.Errorf("Failed to use database: %s", err)
    }

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
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case record, ok := <-c.recordChan:
            if !ok {
                if len(batchBuffer) > 0 {
                    c.insertBatch(batchBuffer, workerID)
                }
                return
            }
            
            batchBuffer = append(batchBuffer, record)
            
            if len(batchBuffer) >= c.batchSize {
                c.insertBatch(batchBuffer, workerID)
                batchBuffer = batchBuffer[:0]
            }
            
        case <-ticker.C:
            if len(batchBuffer) > 0 {
                c.insertBatch(batchBuffer, workerID)
                batchBuffer = batchBuffer[:0]
            }
        }
    }
}

func (c *Consumer) insertBatch(records []*pb.DataRecord, workerID int) {
    if len(records) == 0 {
        return
    }

    startTime := time.Now()
    
    recordsBySymbol := make(map[string][]*pb.DataRecord)
    for _, record := range records {
        symbol := record.GetSymbol()
        recordsBySymbol[symbol] = append(recordsBySymbol[symbol], record)
    }

    totalInserted := 0
    var wg sync.WaitGroup
    var mu sync.Mutex
    
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

    if totalInserted > 0 && *verbose {
        duration := time.Since(startTime)
        Log.Printf("Worker %d: Inserted %d records in %v", 
            workerID, totalInserted, duration)
    }
}

func (c *Consumer) insertSymbolRecords(symbol string, records []*pb.DataRecord) int {
    if len(records) == 0 {
        return 0
    }

    firstRecord := records[0]
    exchange := firstRecord.GetExchange()
    market := firstRecord.GetMarket()
    
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

    var valueStrings []string
    var valueArgs []interface{}
    
    for _, record := range records {
        valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        
        valueArgs = append(valueArgs,
            record.GetXTs(),
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
    }

    insertSQL := fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(valueStrings, ","))
    _, err = c.db.Exec(insertSQL, valueArgs...)
    if err != nil {
        ErrLog.Printf("Failed to insert %d records for symbol %s: %s", len(records), symbol, err)
        return 0
    }

    return len(records)
}

func (c *Consumer) processMessage(body []byte) (int, int, error) {
    header, protoData, err := parseMessageFormat(body)
    if err != nil {
        return 0, 0, fmt.Errorf("Parse message format failed: %w", err)
    }

    var dr pb.DataRequest  
    if err := proto.Unmarshal(protoData, &dr); err != nil {  
        return 0, 0, fmt.Errorf("Unmarshal DataRequest failed: %w", err)  
    }  

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

    var db pb.DataBatch
    if err := proto.Unmarshal(batchBytes, &db); err != nil {
        return 0, 0, fmt.Errorf("Unmarshal DataBatch failed: %w", err)
    }
    
    recordsProcessed := 0
    dropped := 0
    
    for _, record := range db.Records {
        select {
        case c.recordChan <- record:
            recordsProcessed++
        default:
            dropped++
            if dropped%1000 == 0 {
                ErrLog.Printf("Dropped %d records due to full buffer", dropped)
            }
        }
    }
    
    if *verbose && len(db.Records) > 0 {
        Log.Printf("Batch %s: %d records, inserted %d, dropped %d", 
            db.BatchId, len(db.Records), recordsProcessed, dropped)
    }

    return recordsProcessed, dropped, nil
}

func (c *Consumer) Stop() {
    Log.Printf("Stopping consumer gracefully...")
    atomic.StoreInt32(&c.stopping, 1)
    c.shouldReconnect = false
    
    time.Sleep(1 * time.Second)
    
    close(c.recordChan)
    c.wg.Wait()
    
    c.cleanupAMQP()
    
    if c.db != nil {
        c.db.Close()
    }
    
    c.done <- nil
}

func sanitizeSymbol(symbol string) string {
    sanitized := strings.Map(func(r rune) rune {
        if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || 
           (r >= '0' && r <= '9') || r == '_' {
            return r
        }
        return '_'
    }, symbol)
    
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