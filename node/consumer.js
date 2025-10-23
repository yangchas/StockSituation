const amqp = require('amqplib');
const protobuf = require('protobufjs');
const zlib = require('zlib');
const taos = require('@tdengine/websocket');
const { program } = require('commander');

// 命令行参数配置
program
  .option('--uri <uri>', 'AMQP URI', 'amqp://admin:admin@localhost:5672/')
  .option('--queue <queue>', 'AMQP queue name', 'stream2')
  .option('--consumer-tag <tag>', 'AMQP consumer tag', 'tdengine-consumer')
  .option('--tdengine-host <host>', 'TDengine host', 'localhost')
  .option('--tdengine-port <port>', 'TDengine port', '6041')
  .option('--tdengine-user <user>', 'TDengine user', 'root')
  .option('--tdengine-password <password>', 'TDengine password', 'taosdata')
  .option('--tdengine-database <database>', 'TDengine database', 'market_data')
  .option('--batch-size <size>', 'Batch size for TDengine insertion', '500')
  .option('--workers <count>', 'Number of TDengine insertion workers', '4')
  .option('--buffer-size <size>', 'Record channel buffer size', '50000')
  .option('--flush-timeout <timeout>', 'Timeout for flushing remaining data (ms)', '1000')
  .option('--verbose', 'Enable verbose output', true)
  .parse(process.argv);

const options = program.opts();

// 配置常量
const CONFIG = {
  uri: options.uri,
  queue: options.queue,
  consumerTag: options.consumerTag,
  tdengine: {
    host: options.tdengineHost,
    port: options.tdenginePort,
    user: options.tdengineUser,
    password: options.tdenginePassword,
    database: options.tdengineDatabase
  },
  batchSize: parseInt(options.batchSize),
  workerCount: parseInt(options.workers),
  bufferSize: parseInt(options.bufferSize),
  flushTimeout: parseInt(options.flushTimeout),
  verbose: options.verbose
};

class TDengineConsumer {
  constructor(config) {
    this.config = config;
    this.stopping = false;
    this.shouldReconnect = true;
    this.reconnectDelay = 1000;
    this.maxReconnectDelay = 30000;
    
    // 统计信息
    this.messageCount = 0;
    this.recordCount = 0;
    this.startTime = Date.now();
    this.lastReport = Date.now();
    
    // 通道和队列
    this.workers = [];
    this.protoRoot = null;
    this.connection = null;
    this.channel = null;
    this.wsSql = null;
    
    // 连接状态
    this.isTDengineConnected = false;
  }

  async init() {
    console.log(`Starting TDengine consumer for queue: ${this.config.queue}`);
    
    // 加载 protobuf 定义
    await this.loadProtobuf();
    
    // 初始化 TDengine
    await this.initTDengine();
    
    // 设置信号处理
    this.setupSignalHandler();
    
    // 启动工作线程
    this.startWorkers();
    
    // 启动主循环
    await this.mainLoop();
  }

  async loadProtobuf() {
    try {
      // 动态创建 protobuf 定义，避免外部文件依赖
      const protoDefinition = `
        syntax = "proto3";

        package dataservice;

        message DataRecord {
          int64 x_ts = 1;
          string symbol = 2;
          string exchange = 3;
          string market = 4;
          float lp = 5;
          float o = 6;
          float h = 7;
          float l = 8;
          float lc = 9;
          float a = 10;
          int64 v = 11;
          int64 p = 12;
          float ap1 = 13;
          float ap2 = 14;
          float ap3 = 15;
          float ap4 = 16;
          float ap5 = 17;
          float bp1 = 18;
          float bp2 = 19;
          float bp3 = 20;
          float bp4 = 21;
          float bp5 = 22;
          int64 av1 = 23;
          int64 av2 = 24;
          int64 av3 = 25;
          int64 av4 = 26;
          int64 av5 = 27;
          int64 bv1 = 28;
          int64 bv2 = 29;
          int64 bv3 = 30;
          int64 bv4 = 31;
          int64 bv5 = 32;
        }

        message DataBatch {
          string batch_id = 1;
          repeated DataRecord records = 2;
        }

        message DataRequest {
          bytes compressed_data = 1;
        }
      `;
      
      this.protoRoot = protobuf.Root.fromJSON(protobuf.parse(protoDefinition).root);
      console.log('Protobuf definitions loaded successfully');
    } catch (error) {
      console.error('Failed to load protobuf definitions:', error);
      process.exit(1);
    }
  }

async createTDengineConnection() {
  try {
    const dsn = `ws://${this.config.tdengine.host}:${this.config.tdengine.port}`;
    console.log(`Connecting to TDengine: ${dsn}`);
    
    let conf = new taos.WSConfig(dsn);
    conf.setUser(this.config.tdengine.user);
    conf.setPwd(this.config.tdengine.password);
    conf.setDb(this.config.tdengine.database);
    
    // 增加连接超时时间（单位：毫秒）
    // conf.setConnectTimeout(10000); // 10秒
    // conf.setRequestTimeout(15000); // 15秒
    
    const conn = await taos.sqlConnect(conf);
    console.log("Connected to TDengine successfully");
    return conn;
  } catch (err) {
    console.error(`Failed to connect to TDengine, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
    throw err;
  }
}
  async initTDengine() {
    try {
      // 创建连接
      this.wsSql = await this.createTDengineConnection();
      this.isTDengineConnected = true;

      // 创建数据库和表
      await this.createDatabaseAndTable();
      
      console.log('TDengine initialized successfully');
    } catch (error) {
      console.error('Failed to initialize TDengine:', error);
      process.exit(1);
    }
  }

  async createDatabaseAndTable() {
    try {
      // 创建数据库
      await this.wsSql.exec('CREATE DATABASE IF NOT EXISTS market_data');
      console.log("Create database market_data successfully.");

      // 使用数据库
      await this.wsSql.exec('USE market_data');

      // 创建超级表
      const createStableSQL = `
        CREATE STABLE IF NOT EXISTS stock_data (
          ts TIMESTAMP,
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
      `;
      
      await this.wsSql.exec(createStableSQL);
      console.log("Create stable market_data.stock_data successfully");
    } catch (error) {
      console.error(`Failed to create database or stable, ErrCode: ${error.code}, ErrMessage: ${error.message}`);
      throw error;
    }
  }

  startWorkers() {
    for (let i = 0; i < this.config.workerCount; i++) {
      const worker = this.createBatchWorker(i);
      this.workers.push(worker);
    }
    console.log(`Started ${this.config.workerCount} batch insertion workers`);
  }

  createBatchWorker(workerId) {
    let batchBuffer = [];
    let isProcessing = false;

    const processBatch = async () => {
      if (isProcessing || batchBuffer.length === 0) return;
      
      isProcessing = true;
      const recordsToProcess = [...batchBuffer];
      batchBuffer = [];
      
      try {
        await this.insertBatch(recordsToProcess, workerId);
      } catch (error) {
        console.error(`Worker ${workerId}: Failed to insert batch:`, error);
      } finally {
        isProcessing = false;
      }
    };

    // 定时刷新
    const flushInterval = setInterval(() => {
      if (!this.stopping && batchBuffer.length > 0) {
        processBatch();
      }
    }, 100);

    // 返回清理函数
    return {
      addRecord: (record) => {
        if (this.stopping) return;
        
        batchBuffer.push(record);
        if (batchBuffer.length >= this.config.batchSize) {
          processBatch();
        }
      },
      cleanup: async () => {
        clearInterval(flushInterval);
        if (batchBuffer.length > 0) {
          await this.insertBatch(batchBuffer, workerId);
        }
      }
    };
  }

  async insertBatch(records, workerId) {
    if (records.length === 0) return;

    const startTime = Date.now();
    
    // 按符号分组
    const recordsBySymbol = {};
    for (const record of records) {
      const symbol = record.symbol;
      if (!recordsBySymbol[symbol]) {
        recordsBySymbol[symbol] = [];
      }
      recordsBySymbol[symbol].push(record);
    }

    const insertPromises = [];
    
    for (const [symbol, symbolRecords] of Object.entries(recordsBySymbol)) {
      if (symbolRecords.length === 0) continue;
      
      insertPromises.push(this.insertSymbolRecords(symbol, symbolRecords));
    }

    try {
      const results = await Promise.allSettled(insertPromises);
      const totalInserted = results.reduce((sum, result) => 
        result.status === 'fulfilled' ? sum + result.value : sum, 0
      );

      if (totalInserted > 0 && this.config.verbose) {
        const duration = Date.now() - startTime;
        console.log(`Worker ${workerId}: Inserted ${totalInserted} records in ${duration}ms`);
      }
    } catch (error) {
      console.error(`Worker ${workerId}: Batch insertion failed:`, error);
    }
  }

  async insertSymbolRecords(symbol, records) {
    if (records.length === 0) return ;

    const firstRecord = records[0];
    const exchange = firstRecord.exchange || '';
    const market = firstRecord.market || '';
    
    const sanitizedSymbol = this.sanitizeSymbol(symbol);
    const tableName = `t_${sanitizedSymbol}`;
    
    try {
      // 确保 TDengine 连接正常
      if (!this.isTDengineConnected) {
        console.log('Reconnecting to TDengine...');
        this.wsSql = await this.createTDengineConnection();
        this.isTDengineConnected = true;
      }

      // 构建插入语句 - 使用 TDengine WebSocket 的 INSERT 语法
      let insertSQL = `INSERT INTO `;
      
      // 为每个记录构建 VALUES 子句
      const valueClauses = records.map(record => {
        const ts = new Date(record.x_ts).toISOString().replace('T', ' ').replace('Z', '').split('.')[0];
        
        return `${tableName} USING market_data.stock_data TAGS ('${symbol}', '${exchange}', '${market}') ` +
               `VALUES ('${ts}', ${record.lp || 0}, ${record.o || 0}, ${record.h || 0}, ${record.l || 0}, ` +
               `${record.lc || 0}, ${record.a || 0}, ${record.v || 0}, ${record.p || 0}, ` +
               `${record.ap1 || 0}, ${record.ap2 || 0}, ${record.ap3 || 0}, ${record.ap4 || 0}, ${record.ap5 || 0}, ` +
               `${record.bp1 || 0}, ${record.bp2 || 0}, ${record.bp3 || 0}, ${record.bp4 || 0}, ${record.bp5 || 0}, ` +
               `${record.av1 || 0}, ${record.av2 || 0}, ${record.av3 || 0}, ${record.av4 || 0}, ${record.av5 || 0}, ` +
               `${record.bv1 || 0}, ${record.bv2 || 0}, ${record.bv3 || 0}, ${record.bv4 || 0}, ${record.bv5 || 0})`;
      }).join(' ');

      insertSQL += valueClauses;

      const result = await this.wsSql.exec(insertSQL);
      
      if (this.config.verbose) {
        console.log(`Successfully inserted ${records.length} rows for symbol ${symbol}`);
      }
      
      return records.length;
    } catch (error) {
      console.error(`Failed to insert records for symbol ${symbol}:`, error);
      
      // 标记连接断开
      if (error.code === -1 || error.message.includes('connection')) {
        this.isTDengineConnected = false;
      }
      
      return 0;
    }
  }

  sanitizeSymbol(symbol) {
    let sanitized = symbol.replace(/[^a-zA-Z0-9_]/g, '_');
    
    // 如果以数字开头，添加前缀
    if (/^\d/.test(sanitized)) {
      sanitized = 's_' + sanitized;
    }
    
    return sanitized;
  }

  async mainLoop() {
    while (!this.stopping) {
      try {
        await this.connectAndConsume();
        this.reconnectDelay = 1000; // 重置重连延迟
      } catch (error) {
        console.error('Connection failed:', error);
        
        if (!this.shouldReconnect) {
          break;
        }
        
        console.log(`Reconnecting in ${this.reconnectDelay}ms...`);
        await this.delay(this.reconnectDelay);
        
        // 指数退避
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
      }
    }
  }

  async connectAndConsume() {
    // 清理旧连接
    await this.cleanupAMQP();

    console.log('Connecting to RabbitMQ...');
    this.connection = await amqp.connect(this.config.uri);
    
    console.log('Creating channel...');
    this.channel = await this.connection.createChannel();
    
    // 设置 QoS
    await this.channel.prefetch(10);
    
    console.log(`Declaring queue: ${this.config.queue}`);
    const args = {
      'x-max-length': 100000
    };
    
    await this.channel.assertQueue(this.config.queue, {
      durable: true,
      arguments: args
    });

    const uniqueTag = `${this.config.consumerTag}-${Date.now()}`;
    console.log(`Starting consumer with tag: ${uniqueTag}`);
    
    await this.channel.consume(this.config.queue, (message) => {
      this.handleMessage(message);
    }, {
      consumerTag: uniqueTag,
      noAck: true
    });

    console.log('AMQP connection established successfully');
    
    // 监听连接关闭
    this.connection.on('close', () => {
      if (!this.stopping) {
        console.log('Connection closed, reconnecting...');
      }
    });

    this.connection.on('error', (error) => {
      if (!this.stopping) {
        console.error('Connection error:', error);
      }
    });
  }

  async handleMessage(message) {
    if (!message) return;
    
    this.messageCount++;
    
    try {
      const recordsProcessed = await this.processMessage(message.content);
      this.recordCount += recordsProcessed;
      
      // 定期报告统计信息
      const now = Date.now();
      if (now - this.lastReport > 10000) { // 10秒
        const elapsed = (now - this.startTime) / 1000;
        const msgRate = this.messageCount / elapsed;
        const recordRate = this.recordCount / elapsed;
        
        console.log(`Processed ${this.messageCount} messages, ${this.recordCount} records ` +
                   `(${msgRate.toFixed(2)} msg/sec, ${recordRate.toFixed(2)} records/sec)`);
        this.lastReport = now;
      }
      
      if (this.config.verbose && this.messageCount % 1000 === 0) {
        console.log(`Received message ${this.messageCount}`);
      }
    } catch (error) {
      console.error(`Failed to process message ${this.messageCount}:`, error);
    }
  }

  async processMessage(body) {
    const { header, protoData } = this.parseMessageFormat(body);
    
    // 解析 DataRequest
    const DataRequest = this.protoRoot.lookupType('dataservice.DataRequest');
    const dr = DataRequest.decode(protoData);
    
    let batchBytes;
    switch (header.compression) {
      case 'GZIP':
      case 'ZLIB':
        batchBytes = await this.decompressZlib(dr.compressedData);
        break;
      case 'NONE':
      case '':
        batchBytes = dr.compressedData;
        break;
      default:
        throw new Error(`Unsupported compression: ${header.compression}`);
    }
    
    // 解析 DataBatch
    const DataBatch = this.protoRoot.lookupType('dataservice.DataBatch');
    const db = DataBatch.decode(batchBytes);
    
    let recordsProcessed = 0;
    let dropped = 0;
    
    // 分发记录到工作线程
    for (const record of db.records) {
      if (this.stopping) break;
      
      const workerIndex = Math.abs(this.hashString(record.symbol)) % this.config.workerCount;
      
      // 简单的背压控制 - 这里简化处理，实际应该检查每个worker的缓冲区
      if (recordsProcessed + dropped < this.config.bufferSize) {
        this.workers[workerIndex].addRecord(record);
        recordsProcessed++;
      } else {
        dropped++;
        if (dropped % 1000 === 0) {
          console.error(`Dropped ${dropped} records due to full buffer`);
        }
      }
    }
    
    if (this.config.verbose && db.records.length > 0) {
      console.log(`Batch ${db.batchId}: ${db.records.length} records, ` +
                 `inserted ${recordsProcessed}, dropped ${dropped}`);
    }
    
    return recordsProcessed;
  }

  parseMessageFormat(body) {
    if (body.length < 4) {
      throw new Error(`Message body too short: ${body.length} bytes`);
    }
    
    const headerLen = body.readUInt32BE(0);
    
    if (body.length < 4 + headerLen) {
      throw new Error(`Message body too short for header: have ${body.length}, need ${4 + headerLen}`);
    }
    
    const headerJSON = body.slice(4, 4 + headerLen);
    const header = JSON.parse(headerJSON.toString());
    const protoData = body.slice(4 + headerLen);
    
    return { header, protoData };
  }

  decompressZlib(data) {
    return new Promise((resolve, reject) => {
      zlib.inflate(data, (error, result) => {
        if (error) {
          reject(new Error(`Decompress failed: ${error.message}`));
        } else {
          resolve(result);
        }
      });
    });
  }

  hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
  }

  async stop() {
    console.log('Stopping consumer gracefully...');
    this.stopping = true;
    this.shouldReconnect = false;
    
    // 等待当前处理完成
    await this.delay(1000);
    
    // 清理工作线程
    for (const worker of this.workers) {
      await worker.cleanup();
    }
    
    // 关闭连接
    await this.cleanupAMQP();
    await this.cleanupTDengine();
    
    console.log('Consumer stopped gracefully');
    process.exit(0);
  }

  async cleanupAMQP() {
    if (this.channel) {
      try {
        await this.channel.close();
      } catch (error) {
        // 忽略关闭错误
      }
      this.channel = null;
    }
    
    if (this.connection) {
      try {
        await this.connection.close();
      } catch (error) {
        // 忽略关闭错误
      }
      this.connection = null;
    }
  }

  async cleanupTDengine() {
    if (this.wsSql) {
      try {
        await this.wsSql.close();
      } catch (error) {
        console.error('Error closing TDengine connection:', error);
      }
      this.wsSql = null;
    }
  }

  setupSignalHandler() {
    process.on('SIGINT', () => {
      console.log('Received SIGINT');
      this.stop();
    });
    
    process.on('SIGTERM', () => {
      console.log('Received SIGTERM');
      this.stop();
    });
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// 启动消费者
const consumer = new TDengineConsumer(CONFIG);
consumer.init().catch(error => {
  console.error('Consumer failed to start:', error);
  process.exit(1);
});