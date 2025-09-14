// processor.go
package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"compress/gzip"
	"compress/zlib"
	"bytes"
	"io"
	"strconv"

	"google.golang.org/protobuf/proto"
	"rabtd-app/dataservice"
)

// startBatchProcessor 处理批量消息
func startBatchProcessor(ctx context.Context, msgChan <-chan StreamMessage, db *sql.DB) {
	batch := make([]StreamMessage, 0, batchSize)
	batchTimeout := time.Duration(batchTimeoutSec) * time.Second
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	log.Println("Batch processor started, waiting for messages...")

	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				log.Println("Message channel closed")
				if len(batch) > 0 {
					processBatch(batch, db)
				}
				return
			}

			batch = append(batch, msg)

			if len(batch) >= batchSize {
				log.Printf("Batch size reached %d, processing...", batchSize)
				processBatch(batch, db)
				batch = make([]StreamMessage, 0, batchSize)
				ticker.Reset(batchTimeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				log.Printf("Batch timeout reached, processing %d messages", len(batch))
				processBatch(batch, db)
				batch = make([]StreamMessage, 0, batchSize)
			}

		case <-ctx.Done():
			if len(batch) > 0 {
				processBatch(batch, db)
			}
			log.Println("Processor exiting: context cancelled.")
			return
		}
	}
}

// processBatch 处理批量消息
func processBatch(messages []StreamMessage, db *sql.DB) {
	if len(messages) == 0 {
		return
	}

	startTime := time.Now()
	successCount := 0

	defer func() {
		log.Printf("Processed %d out of %d messages in %v", 
			successCount, len(messages), time.Since(startTime))
	}()

	// 准备SQL语句
	sqlStmt := fmt.Sprintf("INSERT INTO ? USING %s.%s TAGS(?) VALUES (?, ?, ?, ?, ?, ?)", 
		targetDatabase, superTableName)

	stmt, err := db.Prepare(sqlStmt)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return
	}
	defer stmt.Close()

	// 按消费者分组处理消息
	consumerOffsets := make(map[string]int64) // 使用消费者名称作为键

	// 处理每个消息
	for i, msg := range messages {
		records, err := parseMessage(msg.Data)
		if err != nil {
			log.Printf("Error parsing message %d (offset %d): %v", i, msg.Offset, err)
			continue
		}

		// 插入数据库
		insertSuccess := true
		for _, record := range records {
			continue
			log.Printf("record %s %s",record.DeviceId,record.Humidity)
			// _, err = stmt.Exec(
			// 	"device_"+record.DeviceId,
			// 	record.DeviceId,
			// 	record.Ts,
			// 	record.Temperature,
			// 	record.Humidity,
			// 	record.Pressure,
			// 	record.Voltage,
			// )
			// if err != nil {
			// 	log.Printf("Error inserting record %s (offset %d): %v", 
			// 		record.DeviceId, msg.Offset, err)
			// 	insertSuccess = false
			// 	break
			// }
		}

		if insertSuccess {
			// 记录成功处理的偏移量
			consumerName := msg.Consumer.GetName()
			if currentOffset, exists := consumerOffsets[consumerName]; !exists || msg.Offset > currentOffset {
				consumerOffsets[consumerName] = msg.Offset
			}
			successCount++
		}
	}

	// 提交每个消费者的偏移量
	for consumerName, offset := range consumerOffsets {
		// 找到对应的消费者实例
		for _, msg := range messages {
			if msg.Consumer.GetName() == consumerName {
				err := msg.Consumer.StoreOffset()
				if err != nil {
					log.Printf("Error storing offset %d for consumer %s: %v", offset, consumerName, err)
				} else {
					log.Printf("Confirmed offset %d for consumer %s", offset, consumerName)
				}
				break
			}
		}
	}
}

// 其余函数保持不变...

// parseMessage 解析消息（支持多种格式）
func parseMessage(data []byte) ([]*dataservice.DataRecord, error) {
	// 尝试解析为 Protobuf 流格式
	if isProtobufStreamMessage(data) {
		return parseProtobufStreamMessage(data)
	}

	// 尝试解析为普通 Protobuf
	if isLikelyProtobuf(data) {
		return parseProtobufRequest(data)
	}

	// 尝试解析为旧格式
	return parseLegacyFormat(data)
}

// isProtobufStreamMessage 检查是否是流式 Protobuf 消息
func isProtobufStreamMessage(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	headerLength := binary.BigEndian.Uint32(data[:4])
	return headerLength > 0 && headerLength < 1024 && len(data) >= int(4+headerLength)
}

// isLikelyProtobuf 检查是否是 Protobuf 数据
func isLikelyProtobuf(data []byte) bool {
	return len(data) > 2
}

// parseProtobufStreamMessage 解析流式 Protobuf 消息
func parseProtobufStreamMessage(data []byte) ([]*dataservice.DataRecord, error) {
	headerLength := binary.BigEndian.Uint32(data[:4])
	if len(data) < int(4+headerLength) {
		return nil, fmt.Errorf("invalid stream message length")
	}

	// 解析头部信息
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
	var request dataservice.DataRequest
	if err := proto.Unmarshal(protobufData, &request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DataRequest: %v", err)
	}

	// 解压数据
	decompressed, err := decompressData(request.CompressedData, request.Compression.String())
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %v", err)
	}

	// 解析批次数据
	var batch dataservice.DataBatch
	if err := proto.Unmarshal(decompressed, &batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DataBatch: %v", err)
	}

	log.Printf("Processed batch %s with %d records", header.BatchId, len(batch.Records))
	return batch.Records, nil
}

// parseProtobufRequest 解析普通 Protobuf 请求
func parseProtobufRequest(data []byte) ([]*dataservice.DataRecord, error) {
	var request dataservice.DataRequest
	if err := proto.Unmarshal(data, &request); err != nil {
		return nil, err
	}

	decompressed, err := decompressData(request.CompressedData, request.Compression.String())
	if err != nil {
		return nil, err
	}

	var batch dataservice.DataBatch
	if err := proto.Unmarshal(decompressed, &batch); err != nil {
		return nil, err
	}

	return batch.Records, nil
}

// parseLegacyFormat 解析旧格式
func parseLegacyFormat(data []byte) ([]*dataservice.DataRecord, error) {
	text := string(data)
	if strings.Contains(text, ",") {
		fields := strings.Split(text, ",")
		if len(fields) >= 6 {
			ts, _ := strconv.ParseInt(fields[0], 10, 64)
			temp, _ := strconv.ParseFloat(fields[2], 32)
			hum, _ := strconv.ParseFloat(fields[3], 32)
			press, _ := strconv.ParseFloat(fields[4], 32)
			volt, _ := strconv.ParseFloat(fields[5], 32)

			record := &dataservice.DataRecord{
				Ts:          ts,
				DeviceId:    fields[1],
				Temperature: float32(temp),
				Humidity:    float32(hum),
				Pressure:    float32(press),
				Voltage:     float32(volt),
			}
			return []*dataservice.DataRecord{record}, nil
		}
	}

	return nil, fmt.Errorf("unknown message format")
}

// decompressData 解压数据
func decompressData(data []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "GZIP", "1":
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case "DEFLATE", "2":
		reader, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case "NONE", "0":
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}