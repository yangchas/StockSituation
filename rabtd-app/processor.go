package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	// "strings"
	"time"

	"compress/gzip"
	"compress/zlib"
	"bytes"
	"io"
	// "strconv"
	// "reflect"
	"google.golang.org/protobuf/proto"
	"rabtd-app/dataservice"
)

// startBatchProcessor 处理批量消息
func startBatchProcessor(ctx context.Context, msgChan <-chan StreamMessage, db *sql.DB) {
	// batch := make([]StreamMessage, 0, batchSize)
	batchTimeout := time.Duration(batchTimeoutSec) * time.Second
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	log.Println("Batch processor started, waiting for messages...")

	for {
		select {
		// case msg, ok := <-msgChan:
			// if !ok {
			// 	log.Println("Message channel closed")
			// 	if len(batch) > 0 {
			// 		processBatch(batch, db)
			// 	}
			// 	return
			// }

			// batch = append(batch, msg)

			// if len(batch) >= batchSize {
			// 	log.Printf("Batch size reached %d, processing...", batchSize)
			// 	processBatch(batch, db)
			// 	batch = make([]StreamMessage, 0, batchSize)
			// 	ticker.Reset(batchTimeout)
			// }

		// case <-ticker.C:
		// 	if len(batch) > 0 {
		// 		log.Printf("Batch timeout reached, processing %d messages", len(batch))
		// 		processBatch(batch, db)
		// 		batch = make([]StreamMessage, 0, batchSize)
		// 	}

		// case <-ctx.Done():
		// 	if len(batch) > 0 {
		// 		processBatch(batch, db)
		// 	}
		// 	log.Println("Processor exiting: context cancelled.")
		// 	return
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
	failedCount := 0

	defer func() {
		log.Printf("Processed %d out of %d messages in %v (success: %d, failed: %d)", 
			successCount+failedCount, len(messages), time.Since(startTime), successCount, failedCount)
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
		// parseMessage(msg.Data)
		records, err := parseMessage(msg.Data)
		if err != nil {
			log.Printf("Error parsing message %d (offset %d): %v", i, msg.Offset, err)
			failedCount++
			continue
		}
		log.Printf("log %d %s",i,records)
		// 插入数据库
		insertSuccess := true
		for _, record := range records {
			// continue
			log.Printf("record %s %d",record.Symbol,record.XTs)
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
				// 使用 StoreCustomOffset 方法提交特定偏移量
				// 注意：我们需要提交下一个偏移量，这样下次启动时会从这个位置开始
				nextOffset := offset + 1
				err := msg.Consumer.StoreCustomOffset(nextOffset)
				if err != nil {
					log.Printf("Error storing offset %d for consumer %s: %v", nextOffset, consumerName, err)
				} else {
					log.Printf("Confirmed offset %d for consumer %s", nextOffset, consumerName)
				}
				break
			}
		}
	}
}

// parseMessage 解析消息（支持多种格式）
func parseMessage(data []byte) ([]*dataservice.DataRecord, error) {
	// 尝试解析为 Protobuf 流格式
	// if isProtobufStreamMessage(data) {
		log.Printf("尝试解析为 Protobuf 流格式")
		return parseProtobufStreamMessage(data)
	// }

	// // 尝试解析为普通 Protobuf
	// if isLikelyProtobuf(data) {
	// 	log.Printf("尝试解析为普通")
	// 	return parseProtobufRequest(data)
	// }
	// log.Printf("尝试解析为旧格式")
	// // 尝试解析为旧格式
	// return parseLegacyFormat(data)
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
	log.Printf("解析 %s",data);

	headerLength := binary.BigEndian.Uint32(data[:4])
	if len(data) < int(4+headerLength) {
		return nil, fmt.Errorf("invalid stream message length")
	}
	// log.Printf("解析头部信息")
	// 解析头部信息
	headerData := data[4 : 4+headerLength]
	var header struct {
		ProtoVersion string `json:"proto_version"`
		Compression  string `json:"compression"`
		BatchId      string `json:"batch_id"`
		RecordCount  int    `json:"record_count"`
	}
	// log.Printf("Unmarshal %s",headerData)
	if err := json.Unmarshal(headerData, &header); err != nil {
		return nil, fmt.Errorf("failed to parse header: %v", err)
	}

	// 解析 Protobuf 数据
	protobufData := data[4+headerLength:]
	// log.Printf("解析 Protobuf 数据 %s",protobufData)
	var request dataservice.DataRequest
	if err := proto.Unmarshal(protobufData, &request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DataRequest: %v", err)
	}
	// log.Printf("解压数据 %s",request.CompressedData)
	// 解压数据
	decompressed, err := decompressData(request.CompressedData, request.Compression.String())
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %v", err)
	}
	log.Printf("解析批次数据长度: %d", len(decompressed))
	// 解析批次数据
	var batch dataservice.DataBatch
	if err := proto.Unmarshal(decompressed, &batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DataBatch: %v", err)
	}
	// 添加详细日志
    // if len(batch.Records) > 0 {
    //     record := batch.Records[0]
    //     v := reflect.ValueOf(record).Elem()
    //     t := v.Type()
        
    //     log.Printf("解析出的字段数量: %d", v.NumField())
    //     for i := 0; i < v.NumField(); i++ {
    //         field := v.Field(i)
    //         fieldName := t.Field(i).Name
    //         log.Printf("字段 %s: %v (类型: %v)", fieldName, field.Interface(), field.Type())
    //     }
    // }
	log.Printf("parseProtobufStreamMessage END %s",batch)
	// log.Printf("Processed batch %s with %d records", header.BatchId, len(batch.Records))
	return batch.Records, nil
}

// // parseProtobufRequest 解析普通 Protobuf 请求
// func parseProtobufRequest(data []byte) ([]*dataservice.DataRecord, error) {
// 	var request dataservice.DataRequest
// 	log.Printf("into parseProtobufRequest ")
// 	if err := proto.Unmarshal(data, &request); err != nil {
// 		log.Printf("parseProtobufRequest 返回空 ")
// 		return nil, err
// 	}
// log.Printf("decompressData ")
// 	decompressed, err := decompressData(request.CompressedData, request.Compression.String())
// 	if err != nil {
// 		log.Printf("decompressData 返回空 ")
// 		return nil, err
// 	}
// log.Printf("Unmarshal ")
// 	var batch dataservice.DataBatch
// 	if err := proto.Unmarshal(decompressed, &batch); err != nil {
// 		log.Printf("Unmarshal 返回空 ")
// 		return nil, err
// 	}
// log.Printf("解析普通 Protobuf 请求 ")
// 	return batch.Records, nil
// }

// // parseLegacyFormat 解析旧格式
// func parseLegacyFormat(data []byte) ([]*dataservice.DataRecord, error) {
// 	text := string(data)
// 	if strings.Contains(text, ",") {
// 		fields := strings.Split(text, ",")
// 		log.Printf("%s %s %s %s %s %s",fields[0],fields[1],fields[2],fields[3],fields[4],fields[5])
// 		if len(fields) >= 6 {
// 			ts , _ := strconv.ParseInt(fields[0], 10, 64)
// 			lp, _ := strconv.ParseFloat(fields[1],  32)
// 			o, _ := strconv.ParseFloat(fields[2],  32)
// 			h, _ := strconv.ParseFloat(fields[3], 32)
// 			l, _ := strconv.ParseFloat(fields[4],  32)
// 			lc, _ := strconv.ParseFloat(fields[5],  32)
// 			a, _ := strconv.ParseFloat(fields[6],  32)
// 			v, _ := strconv.ParseInt(fields[7], 10, 64)
// 			p, _ := strconv.ParseInt(fields[8], 10, 64)
// 			ap1, _ := strconv.ParseFloat(fields[9], 32)
// 			ap2, _ := strconv.ParseFloat(fields[10], 32)
// 			ap3, _ := strconv.ParseFloat(fields[11], 32)
// 			ap4, _ := strconv.ParseFloat(fields[12], 32)
// 			ap5, _ := strconv.ParseFloat(fields[13], 32)
// 			bp1, _ := strconv.ParseFloat(fields[14], 32)
// 			bp2, _ := strconv.ParseFloat(fields[15], 32)
// 			bp3, _ := strconv.ParseFloat(fields[16], 32)
// 			bp4, _ := strconv.ParseFloat(fields[17], 32)
// 			bp5, _ := strconv.ParseFloat(fields[18], 32)
// 			av1, _ := strconv.ParseInt(fields[19], 10, 64)
// 			av2, _ := strconv.ParseInt(fields[20], 10, 64)
// 			av3, _ := strconv.ParseInt(fields[21], 10, 64)
// 			av4, _ := strconv.ParseInt(fields[22], 10, 64)
// 			av5, _ := strconv.ParseInt(fields[23], 10, 64)
// 			bv1, _ := strconv.ParseInt(fields[24], 10, 64)
// 			bv2, _ := strconv.ParseInt(fields[25], 10, 64)
// 			bv3, _ := strconv.ParseInt(fields[26], 10, 64)
// 			bv4, _ := strconv.ParseInt(fields[27], 10, 64)
// 			bv5, _ := strconv.ParseInt(fields[28], 10, 64)
// 			symbol:=  fields[29]
// 			exchange := fields[30]
// 			market:= fields[31]
// 			record := &dataservice.DataRecord{
// 				XTs: ts,
// 				Lp : float32(lp),
// 				O : float32(o),
// 				H : float32(h),
// 				L : float32(l),
// 				Lc : float32(lc),
// 				A : float32(a),
// 				V : int64(v),
// 				P : int64(p),
// 				Ap1 : float32(ap1),
// 				Ap2 : float32(ap2),
// 				Ap3 : float32(ap3),
// 				Ap4 : float32(ap4),
// 				Ap5 : float32(ap5),
// 				Bp1 : float32(bp1),
// 				Bp2 : float32(bp2),
// 				Bp3 : float32(bp3),
// 				Bp4 : float32(bp4),
// 				Bp5 : float32(bp5),
// 				Av1 : int64(av1),
// 				Av2 : int64(av2),
// 				Av3 : int64(av3),
// 				Av4 : int64(av4),
// 				Av5 : int64(av5),
// 				Bv1 : int64(bv1),
// 				Bv2 : int64(bv2),
// 				Bv3 : int64(bv3),
// 				Bv4 : int64(bv4),
// 				Bv5 : int64(bv5),
// 				Symbol:symbol,
// 				Exchange:exchange,
// 				Market:market,
// 			}
// 			return []*dataservice.DataRecord{record}, nil
// 		}
// 	}

// 	return nil, fmt.Errorf("unknown message format")
// }

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