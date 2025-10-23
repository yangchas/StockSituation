const grpc = require('@grpc/grpc-js');
const amqp = require('amqplib');
const zlib = require('zlib');
const { promisify } = require('util');

// 使用预生成的 Protobuf 代码
const schema_pb = require('./schema_pb');

// 配置
const GRPC_PORT = 50051;
const RABBITMQ_HOST = 'localhost';
const QUEUE_NAME = 'stream8';

// 异步压缩方法
const gunzip = promisify(zlib.gunzip);
const inflate = promisify(zlib.inflate);

// RabbitMQ连接
let rabbitChannel = null;

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(`amqp://${RABBITMQ_HOST}`);
    rabbitChannel = await connection.createChannel();
    
    await rabbitChannel.assertQueue(QUEUE_NAME, { 
      durable: true,
      maxLength: 100000,
      messageTtl: 3600000
    });
    
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('Failed to connect to RabbitMQ:', error);
    setTimeout(connectRabbitMQ, 5000);
  }
}

async function decompressData(compressedData, compressionType) {
  /** 解压缩数据 */
  try {
    // 确保压缩类型是数字
    let compressionTypeNum = parseInt(compressionType);
    if (isNaN(compressionTypeNum)) {
      // 如果是字符串，转换为对应的数字
      switch (compressionType.toUpperCase()) {
        case 'NONE':
          compressionTypeNum = 0;
          break;
        case 'GZIP':
          compressionTypeNum = 1;
          break;
        case 'DEFLATE':
          compressionTypeNum = 2;
          break;
        default:
          throw new Error(`Unknown compression type: ${compressionType}`);
      }
    }
    
    console.log(`Decompressing data, type: ${compressionTypeNum}, size: ${compressedData.length} bytes`);
    
    let decompressedData;
    switch (compressionTypeNum) {
      case 0: // NONE
        decompressedData = compressedData;
        break;
        
      case 1: // GZIP
        decompressedData = await gunzip(compressedData);
        break;
        
      case 2: // DEFLATE
        decompressedData = await inflate(compressedData);
        break;
        
      default:
        throw new Error(`Unsupported compression type: ${compressionTypeNum}`);
    }
    
    console.log(`Decompressed data size: ${decompressedData.length} bytes`);
    return decompressedData;
    
  } catch (error) {
    console.error('Decompression error:', error);
    console.error('Compressed data length:', compressedData.length);
    console.error('Compression type:', compressionType, typeof compressionType);
    
    // 尝试直接返回数据（假设没有压缩）
    try {
      const text = compressedData.toString('utf-8');
      console.error('Data as text (first 200 chars):', text.substring(0, 200));
      return compressedData;
    } catch (e) {
      console.error('Could not convert data to text:', e);
      throw error;
    }
  }
}

// gRPC服务实现
class EnhancedDataService {
  async SendDataStream(call) {
    /** 处理流式数据传输 */
    let processedCount = 0;
    
    console.log('Starting to process data stream...');
    
    call.on('data', async (request) => {
      try {
        let batchData;
        if (request.compression !== undefined && request.compressed_data) {
          // 解压缩数据
          const decompressedData = await decompressData(
            request.compressed_data, 
            request.compression
          );
          
          // 解析Protobuf数据 - 使用函数式API
          batchData = schema_pb.decodeDataBatch(decompressedData);
          console.log(batchData)
        } else {
          throw new Error('No compressed data found in request');
        }
        
        // 发送到RabbitMQ
        if (rabbitChannel) {
          // 转换为JSON格式发送到RabbitMQ
          const jsonData = {
            batch_id: batchData.batch_id,
            sent_at: batchData.sent_at,
            data: batchData.records.map(record => ({
              ts: record.ts,
              device_id: record.device_id,
              temperature: record.temperature,
              humidity: record.humidity,
              pressure: record.pressure,
              voltage: record.voltage
            }))
          };
          
          rabbitChannel.sendToQueue(
            QUEUE_NAME,
            Buffer.from(JSON.stringify(jsonData)),
            { persistent: true }
          );
          
          processedCount++;
          console.log(`Processed batch ${batchData.batch_id} with ${batchData.records.length} records`);
        }
      } catch (error) {
        console.error('Error processing stream data:', error);
      }
    });
    
    call.on('end', async () => {
      console.log(`Stream processing completed. Processed ${processedCount} batches`);
      call.write({
        success: true,
        message: 'Stream processed successfully',
        processed_count: processedCount
      });
      call.end();
    });
    
    call.on('error', (error) => {
      console.error('Stream error:', error);
    });
  }
  
  async SendData(call, callback) {
    /** 处理单次数据传输 */
    const request = call.request;
    
    try {
      let batchData;
      if (request.compression !== undefined && request.compressed_data) {
        // 解压缩数据
        const decompressedData = await decompressData(
          request.compressed_data, 
          request.compression
        );
        
        // 解析Protobuf数据 - 使用函数式API
        batchData = schema_pb.decodeDataBatch(decompressedData);
        console.log(batchData)
      } else {
        throw new Error('No compressed data found in request');
      }
      
      // 发送到RabbitMQ
      if (rabbitChannel) {
        // 转换为JSON格式发送到RabbitMQ
        const jsonData = {
          batch_id: batchData.batch_id,
          sent_at: batchData.sent_at,
          data: batchData.records.map(record => ({
            ts: record.ts,
            device_id: record.device_id,
            temperature: record.temperature,
            humidity: record.humidity,
            pressure: record.pressure,
            voltage: record.voltage
          }))
        };
        
        rabbitChannel.sendToQueue(
          QUEUE_NAME,
          Buffer.from(JSON.stringify(jsonData)),
          { persistent: true }
        );
        
        console.log(`Sent batch ${batchData.batch_id} to RabbitMQ`);
        
        callback(null, {
          success: true,
          message: 'Data received and queued successfully',
          processed_count: 1
        });
      } else {
        throw new Error('RabbitMQ not connected');
      }
    } catch (error) {
      console.error(`Error processing batch:`, error);
      
      callback(null, {
        success: false,
        message: `Error: ${error.message}`,
        processed_count: 0
      });
    }
  }
}

// 启动gRPC服务器
function startGrpcServer() {
  // 动态加载 Protobuf 定义用于 gRPC 服务
  const protoLoader = require('@grpc/proto-loader');
  const grpc = require('@grpc/grpc-js');
  
  const packageDefinition = protoLoader.loadSync('schema.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });
  
  const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
  
  const server = new grpc.Server({
    'grpc.max_receive_message_length': 100 * 1024 * 1024,
    'grpc.max_send_message_length': 100 * 1024 * 1024,
  });
  
  server.addService(protoDescriptor.dataservice.DataService.service, new EnhancedDataService());
  
  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('Failed to start gRPC server:', err);
        return;
      }
      
      console.log(`gRPC server started on port ${port}`);
      server.start();
    }
  );
}

// 主函数
async function main() {
  console.log('Starting Protobuf-based gRPC receiver and RabbitMQ producer...');
  
  // 连接RabbitMQ
//   await connectRabbitMQ();
  
  // 启动gRPC服务器
  startGrpcServer();
  
  // 保持进程运行
  process.on('SIGINT', () => {
    console.log('Shutting down gracefully...');
    if (rabbitChannel) {
      rabbitChannel.close();
    }
    process.exit(0);
  });
}

main().catch(console.error);