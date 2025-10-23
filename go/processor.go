package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
	// "go/amqp091-go-1.10.0"
	// "pkg.go.dev/github.com/rabbitmq/amqp091-go"
	"github.com/rabbitmq/amqp091-go"
)

// startBatchProcessor 从channel中批量获取消息，并使用流式插入写入TDengine
func startBatchProcessor(ctx context.Context, msgChan <-chan amqp091.Delivery, db *sql.DB) {
	batch := make([]amqp091.Delivery, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				// Channel 被关闭，处理最后一批数据
				if len(batch) > 0 {
					processBatch(batch, db)
				}
				log.Println("Processor exiting: message channel closed.")
				return
			}
			batch = append(batch, msg)

			// 如果达到批量大小，立即处理
			if len(batch) >= batchSize {
				processBatch(batch, db)
				batch = make([]amqp091.Delivery, 0, batchSize) // 重置batch
				ticker.Reset(batchTimeout)                     // 重置超时计时器
			}

		case <-ticker.C:
			// 超时时间到，处理当前批次（防止数据量小，一直达不到batchSize而积压）
			if len(batch) > 0 {
				processBatch(batch, db)
				batch = make([]amqp091.Delivery, 0, batchSize)
			}

		case <-ctx.Done():
			// 收到关闭信号，处理最后一批数据
			if len(batch) > 0 {
				processBatch(batch, db)
			}
			log.Println("Processor exiting: context cancelled.")
			return
		}
	}
}

// processBatch 处理一个批量的消息，使用流式插入写入TDengine，并确认RabbitMQ消息
func processBatch(messages []amqp091.Delivery, db *sql.DB) {
	startTime := time.Now()
	defer func() {
		log.Printf("Processed batch of %d messages in %v\n", len(messages), time.Since(startTime))
	}()

	// 1. 准备流式插入SQL语句
	// 假设消息体是CSV格式: "timestamp,temperature,humidity,location,sensor_id,device_id"
	// 示例: "1689349205000,25.6,45.2,Beijing,sensor_1,device_abc"
	// sql := fmt.Sprintf("INSERT INTO ? USING %s.%s TAGS(?) VALUES (?, ?, ?, ?, ?)", targetDatabase, superTableName)

	// // 2. 准备流式插入参数
	// stmt, err := db.Prepare(sql) // 使用Prepare创建流式语句
	// if err != nil {
	// 	log.Printf("Error preparing statement: %v. This batch will be NACK'd.", err)
	// 	nackMessages(messages) // 准备失败，让消息重试
	// 	return
	// }
	// defer stmt.Close()

	// 3. 遍历消息，添加到流式插入中
	for _, msg := range messages {
        var records []*schema.DataRecord
        var err error

        // 解析消息（支持多种格式）
        if isProtobufStreamMessage(msg.Body) {
            records, err = parseProtobufStreamMessage(msg.Body)
        } else if msg.ContentType == "application/x-protobuf" {
            records, err = parseProtobufRequest(msg.Body)
        } else {
            // 处理旧格式（向后兼容）
            records, err = parseLegacyFormat(msg.Body)
        }

        if err != nil {
            log.Printf("Error parsing message: %v", err)
            continue
        }

        // 处理每条记录
        for _, record := range records {
            _, err = stmt.Exec(
                "device_"+record.DeviceId,
                record.DeviceId,
                record.Ts,
                record.Temperature,
                record.Humidity,
                record.Pressure,    // 根据你的schema添加
                record.Voltage,     // 根据你的schema添加
            )
            if err != nil {
                log.Printf("Error executing insert: %v", err)
                break
            }
			log.Printf(record)
        }
    }

	// 4. 所有消息成功加入流，提交stmt（真正发送到TDengine）
    // 注意：Driver-go 的 stmt.Exec 可能已经自动提交，请查阅最新文档。
    // 如果需要显式提交，可能是 stmt.Close() 或 db.Exec("commit")。
    // 这里依赖于驱动实现。通常循环中的 Exec 会缓冲，Close 会刷新。

	// 5. 所有操作成功，批量确认RabbitMQ消息
	for _, msg := range messages {
		msg.Ack(false) // false 表示不要求多重确认
	}
	log.Printf("Successfully inserted and acknowledged batch of %d messages.", len(messages))
}
// isProtobufStreamMessage 检查是否是流式 Protobuf 消息
func isProtobufStreamMessage(data []byte) bool {
    if len(data) < 4 {
        return false
    }
    // 检查是否有头部长度信息
    return true
}

// parseProtobufStreamMessage 解析流式 Protobuf 消息
func parseProtobufStreamMessage(data []byte) ([]*schema.DataRecord, error) {
    if len(data) < 4 {
        return nil, fmt.Errorf("invalid message length")
    }

    // 读取头部长度
    headerLength := binary.BigEndian.Uint32(data[:4])
    if len(data) < int(4+headerLength) {
        return nil, fmt.Errorf("invalid header length")
    }

    // 解析头部信息（JSON）
    headerData := data[4 : 4+headerLength]
    var header struct {
        ProtoVersion string `json:"proto_version"`
        Compression  string `json:"compression"`
        BatchId      string `json:"batch_id"`
        RecordCount  int    `json:"record_count"`
    }
    
    if err := json.Unmarshal(headerData, &header); err != nil {
        return nil, fmt.Errorf("failed to parse header: %v", err)
    }

    // 解析 Protobuf 数据
    protobufData := data[4+headerLength:]
    
    // 解码 DataRequest
    var request schema.DataRequest
    if err := proto.Unmarshal(protobufData, &request); err != nil {
        return nil, fmt.Errorf("failed to unmarshal DataRequest: %v", err)
    }

    // 解压数据
    decompressed, err := decompressData(request.CompressedData, request.Compression)
    if err != nil {
        return nil, fmt.Errorf("failed to decompress data: %v", err)
    }

    // 解码 DataBatch
    var batch schema.DataBatch
    if err := proto.Unmarshal(decompressed, &batch); err != nil {
        return nil, fmt.Errorf("failed to unmarshal DataBatch: %v", err)
    }

    log.Printf("Processed batch %s with %d records", header.BatchId, len(batch.Records))
    return batch.Records, nil
}

// decompressData 解压数据
func decompressData(data []byte, compressionType string) ([]byte, error) {
    switch compressionType {
    case "GZIP":
        return gzip.NewReader(bytes.NewReader(data))
    case "DEFLATE":
        return zlib.NewReader(bytes.NewReader(data))
    case "NONE":
        return data, nil
    default:
        return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
    }
}
// nackMessages 拒绝一批消息，让RabbitMQ重新投递（或进入死信队列）
func nackMessages(messages []amqp091.Delivery) {
	for _, msg := range messages {
		msg.Nack(false, true) // false-不要求多重Nack, true-要求重新入队
	}
}