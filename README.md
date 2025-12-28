# 股票实时监控与分析系统

## 项目概述

本项目是一个集成了实时数据采集、技术分析、板块监控和题材分类的股票分析系统。系统采用微服务架构，包含C++高性能数据处理、Python Web服务和前端可视化界面。

## 系统架构

### 1. 前端界面 (Web前端)

#### 📊 excel.html - 股票题材分类表
- **功能**: 实时展示股票按题材分类的Excel风格表格
- **特点**:
  - WebSocket实时数据更新
  - 按连板数分类显示（首板、2板、3板及以上）
  - 题材分组和资金类型筛选
  - 个股详情面板和K线图展示
  - T1进程控制（启动/停止/回放模式）

#### 📈 bankuai.html - 实时板块监控系统
- **功能**: 板块行情实时监控和个股异动分析
- **特点**:
  - 板块涨跌幅排行
  - 个股实时数据展示
  - 异动监控侧边栏
  - 连接状态实时显示
  - 统计面板（板块数量、个股数量、涨跌比等）

### 2. 后端服务 (Python)

#### 🚀 integrated_server.py - 集成Web服务
- **功能**: 统一API网关和WebSocket服务
- **核心组件**:
  - **OptimizedIntegratedWebService**: 主服务类
  - **API路由**: 
    - `/api/other_stocks` - 其他个股查询
    - `/api/plate` - 板块数据
    - `/health` - 健康检查
    - `/redis-status` - Redis状态
  - **WebSocket端点**:
    - `/ws/plate` - 板块WebSocket
    - `/ws/plate/data` - 板块数据实时更新

#### 🔧 核心服务模块
- **limit_up_storage.py**: 涨停板数据存储和分析服务
  - `find_other_stocks_by_conditions()` - 条件筛选个股
  - 题材热度分析和评分计算
  - Redis缓存优化

- **plate_updater.py**: 板块数据更新服务
  - 板块层级关系管理
  - 个股-板块映射关系
  - 实时指标计算

- **redis_storage.py**: Redis数据存储管理
  - 股票基础数据缓存
  - 板块指标存储
  - 异动数据管理

### 3. 数据处理引擎 (C++)

#### ⚡ t1.cpp - 高性能数据处理引擎
- **功能**: 实时股票数据采集、分析和存储
- **核心特性**:
  - **多数据源支持**: RabbitMQ实时数据、TDengine历史回放
  - **技术指标计算**: 涨速、成交额、大单净额等
  - **异动检测**: 价格变化、成交量异常监控
  - **多存储后端**: Redis缓存、TDengine时序数据库

#### 🔌 数据流架构
```
数据源 → t1.cpp → Redis/TDengine → Python服务 → 前端界面
    ↓
实时分析 → 异动提醒 → 板块监控 → 题材分类
```

## 主要功能特性

### 📈 实时监控
- 股票实时行情数据
- 板块涨跌幅排行
- 个股异动实时检测
- WebSocket实时推送

### 🏷️ 题材分类
- 自动识别股票题材
- 按连板数分类显示
- 题材热度分析
- 资金类型筛选

### 🔍 技术分析
- 1分钟涨速计算
- 2分钟成交额统计
- 大单净额分析
- 高级技术指标

### 💾 数据存储
- Redis实时缓存
- TDengine时序数据库
- 数据持久化存储
- 历史数据回放

## 技术栈

### 前端技术
- HTML5 + CSS3 + JavaScript
- WebSocket实时通信
- 响应式设计
- 图表可视化

### 后端技术
- Python 3.8+
- aiohttp Web框架
- asyncio异步编程
- Redis缓存
- TDengine时序数据库

### 数据处理
- C++高性能引擎
- RabbitMQ消息队列
- 多线程处理
- 实时数据流处理

## 部署说明

### 环境要求
- Python 3.8+
- Redis Server
- TDengine (可选)
- RabbitMQ (可选)

### 启动顺序
1. **启动Redis服务**
2. **启动TDengine** (如使用历史回放)
3. **启动t1.cpp数据处理引擎**
4. **启动Python集成服务**
   ```bash
   cd web
   python integrated_server.py
   ```
5. **访问前端界面**
   - http://localhost:8080/html/excel.html
   - http://localhost:8080/html/bankuai.html

### 配置文件
- **数据源配置**: 支持RabbitMQ实时数据或TDengine回放模式
- **Redis配置**: 连接参数和键名配置
- **TDengine配置**: 数据库连接和表结构

## API接口文档

### 主要API端点
- `GET /api/other_stocks` - 获取其他个股数据
- `GET /api/plate` - 获取板块数据
- `GET /health` - 服务健康检查
- `GET /redis-status` - Redis状态检查

### WebSocket端点
- `ws://localhost:8080/ws/plate` - 板块数据推送
- `ws://localhost:8080/ws/plate/data` - 实时数据更新

## 性能优化

### 数据处理优化
- 批量处理减少I/O操作
- 缓存机制降低数据库压力
- 异步编程提高并发性能

### 前端优化
- DOM元素缓存
- 批量更新减少重绘
- WebSocket连接复用

## 项目结构

```
Go/
├── C/                          # C++数据处理引擎
│   ├── t1.cpp                  # 主数据处理程序
│   └── schema.proto           # 数据协议定义
├── web/                        # Web服务端和前端
│   ├── html/                   # 前端页面
│   │   ├── excel.html          # 题材分类表
│   │   └── bankuai.html        # 板块监控
│   ├── integrated_server.py    # 集成Web服务
│   ├── limit_up_storage.py     # 涨停板服务
│   ├── plate_updater.py       # 板块更新服务
│   └── redis_storage.py       # Redis存储管理
└── README.md                   # 项目文档
```

## 开发团队

本项目采用模块化设计，各组件职责明确，便于维护和扩展。系统设计注重实时性和性能，适用于股票市场的实时监控和分析需求。