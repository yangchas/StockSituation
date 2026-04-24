import logging
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union

try:
    import taos
except Exception as e:
    # Catch both ImportError and InterfaceError (DLL load fail)
    taos = None
    # print(f"TDengine client not available: {e}")

logger = logging.getLogger(__name__)

class TDengineService:
    """TDengine数据库服务 - 基于官方示例的连接方式"""
    _instance = None
    
    def __new__(cls, host: str = '127.0.0.1', port: int = 6030, 
                 user: str = 'root', password: str = 'taosdata', 
                 database: str = 'market_data1', config: str = '/etc/taos', 
                 timezone: str = 'Asia/Shanghai'):
        # 修复单例模式：只创建一个实例，忽略后续参数差异
        if cls._instance is None:
            cls._instance = super(TDengineService, cls).__new__(cls)
            # 初始化参数
            cls._instance.host = host
            cls._instance.port = port
            cls._instance.user = user
            cls._instance.password = password
            cls._instance.database = database
            cls._instance.config = config
            cls._instance.timezone = timezone
            # [V3.3] 使用线程局部存储 (Thread-Local Storage)
            # 让每个并发线程拥有独立的 conn 和 cursor，彻底防止 double free
            cls._instance._local = threading.local()
            # 建立初始连接/结构检查 (在主线程执行)
            cls._instance._init_db_structures()
        return cls._instance

    def _get_cursor(self):
        """[V3.3] 获取当前线程专属的游标。如果该线程尚未连接，则自动创建。"""
        if not hasattr(self._local, 'cursor') or self._local.cursor is None:
            if taos is not None:
                try:
                    conn = taos.connect(
                        host=self.host, user=self.user, password=self.password,
                        database=self.database, config=self.config, timezone=self.timezone
                    )
                    self._local.cursor = conn.cursor()
                    # logger.debug(f"✅ TDengine 线程局部连接已建立: {threading.current_thread().name}")
                except Exception as e:
                    logger.error(f"❌ TDengine 线程局部连接失败: {e}")
                    return None
            else:
                # 模拟模式
                self._local.cursor = "mock_cursor"
        return self._local.cursor

    def _connect(self):
        """[V3.3 废弃] 统一改用 _get_cursor 动态获取"""
        pass

    def _init_db_structures(self):
        """初始化超级表结构 (V3.3 线程安全版)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return
        try:
            # 1. 确保数据库存在
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")
            
            # 2. 创建日线超级表
            cursor.execute("""
                CREATE STABLE IF NOT EXISTS daily_kline (
                    ts TIMESTAMP, open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                    volume BIGINT, amount FLOAT, turnover FLOAT, pct_chg FLOAT
                ) TAGS (symbol NCHAR(10))
            """)
            
            # 3. 创建 DDE/因子/筹码超级表
            cursor.execute("CREATE STABLE IF NOT EXISTS daily_dde (ts TIMESTAMP, ddje FLOAT, large_net FLOAT, ddx FLOAT, ddy FLOAT, ddz FLOAT) TAGS (symbol NCHAR(10))")
            
            # [V39.2 Evolution] 动态演进因子表结构
            cursor.execute("CREATE STABLE IF NOT EXISTS daily_factors (ts TIMESTAMP, profit_ratio FLOAT, avg_cost FLOAT, concentration FLOAT, bias_20 FLOAT, rsi_6 FLOAT, vol_ratio FLOAT) TAGS (symbol NCHAR(10))")
            
            # 自动补全缺失的列 (MACD, KDJ, BOLL, MA)
            # 注意：TDengine 3.0+ 支持一次性 ADD 多个列，但为了极致兼容性，我们逐个探测并添加
            new_cols = [
                ('ma5', 'FLOAT'), ('ma10', 'FLOAT'), ('ma20', 'FLOAT'),
                ('macd_dif', 'FLOAT'), ('macd_dea', 'FLOAT'), ('macd_hist', 'FLOAT'),
                ('kdj_k', 'FLOAT'), ('kdj_d', 'FLOAT'), ('kdj_j', 'FLOAT'),
                ('boll_up', 'FLOAT'), ('boll_mid', 'FLOAT'), ('boll_low', 'FLOAT')
            ]
            
            # 获取当前列，避免重复添加
            cursor.execute("DESC daily_factors")
            existing_cols = [row[0].lower() for row in cursor.fetchall()]
            
            for col_name, col_type in new_cols:
                if col_name.lower() not in existing_cols:
                    logger.info(f"🛠️ [Database-Evolution] 正在为 daily_factors 增加列: {col_name}")
                    cursor.execute(f"ALTER STABLE daily_factors ADD COLUMN {col_name} {col_type}")

            cursor.execute("CREATE STABLE IF NOT EXISTS daily_chips (ts TIMESTAMP, peak_price FLOAT, peak_weight FLOAT, upper_cost FLOAT, lower_cost FLOAT) TAGS (symbol NCHAR(10))")
            
            logger.info("✅ TDengine STables (Kline/DDE/Factors/Chips) 检查/创建成功")
        except Exception as e:
            logger.error(f"❌ 初始化TDengine表结构失败: {e}")

    def save_daily_kline(self, symbol: str, df: pd.DataFrame) -> bool:
        """批量保存日线数据到TDengine (V3.3 线程本域化)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor" or df is None or df.empty: return False
        try:
            # 兼容 symbol 格式 (去除 .sh/.sz 仅保留 6 位)
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            table_name = f"d_{clean_symbol}"
            
            # 使用 INSERT INTO ... USING ... 语法处理单行或多行写入
            for _, row in df.iterrows():
                try:
                    ts = row['date'] if isinstance(row['date'], str) else row['date'].strftime('%Y-%m-%d %H:%M:%S')
                    o, h, l, c = row.get('open', 0), row.get('high', 0), row.get('low', 0), row.get('close', 0)
                    v, a = int(row.get('volume', 0)), row.get('amount', 0)
                    t, p = row.get('turn', row.get('turnover', 0)), row.get('pct_chg', 0)
                    
                    sql = f"INSERT INTO {table_name} USING daily_kline TAGS ('{symbol}') VALUES ('{ts}', {o}, {h}, {l}, {c}, {v}, {a}, {t}, {p})"
                    cursor.execute(sql)
                except Exception as row_e:
                    logger.debug(f"⚠️ [TDengine] 跳过重复或异常行: {row_e}")
            return True
        except Exception as e:
            logger.error(f"❌ TDengine K线保存失败 {symbol}: {e}")
            return False

    def save_daily_dde(self, symbol: str, df: pd.DataFrame) -> bool:
        """批量保存DDE数据到TDengine (V3.3 线程本域化)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor" or df is None or df.empty: return False
        try:
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            if clean_symbol.isdigit() and len(clean_symbol) == 6:
                if clean_symbol.startswith(('6', '9', '5')): table_name = f"dde_sh{clean_symbol}"
                else: table_name = f"dde_sz{clean_symbol}"
            else:
                table_name = f"dde_{clean_symbol}"
            
            # [V40.5] 严格时间对位：禁止使用系统请求时间，必须使用数据业务时间
            if 'date' in df.columns:
                df['ts'] = pd.to_datetime(df['date']).dt.normalize() # 强制对齐到 00:00:00
            
            if 'ts' not in df.columns:
                logger.error(f"❌ [TDengine] {symbol} DDE 数据缺失日期列，拒绝写入以防污染水位")
                return False
            
            # 清洗 NaN -> 0 (防止 SQL 语法错误)
            df = df.fillna(0)
                
            for _, row in df.iterrows():
                try:
                    ts_val = row['ts'].strftime('%Y-%m-%d %H:%M:%S')
                    sql = f"INSERT INTO {table_name} USING daily_dde TAGS ('{symbol}') VALUES ('{ts_val}', {row.get('ddje', 0)}, {row.get('large_net', 0)}, {row.get('ddx', 0)}, {row.get('ddy', 0)}, {row.get('ddz', 0)})"
                    cursor.execute(sql)
                except Exception: pass
            return True
        except Exception as e:
            logger.error(f"❌ [TDengine] 保存DDE失败 {symbol}: {e}")
            return False

    def get_all_latest_dates(self, stable_name: str = "daily_kline", filter_col: str = None) -> Dict[str, str]:
        """[V40.4] 聚合获取水位 (支持非零字段物理穿透校验)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return {}
        try:
            where_clause = f"WHERE {filter_col} != 0" if filter_col else ""
            sql = f"SELECT symbol, LAST(ts) as latest_ts FROM {stable_name} {where_clause} GROUP BY symbol"
            cursor.execute(sql)
            rows = cursor.fetchall()
            return {row[0]: row[1].strftime('%Y-%m-%d') for row in rows if row[0] and row[1]}
        except Exception as e:
            logger.error(f"❌ TDengine 聚合自检失败 ({stable_name}): {e}")
            return {}

    def cleanup_polluted_data(self, start_ts: str) -> Dict[str, int]:
        """[V40.6] 物理清洗工具：删除指定时间后的污染数据 (Request-Time Cleanup)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return {}
        
        results = {}
        stables = ["daily_kline", "daily_dde", "daily_factors", "daily_chips"]
        for table in stables:
            try:
                # TDengine 删除语法：DELETE FROM super_table WHERE ts >= 'target'
                sql = f"DELETE FROM {table} WHERE ts >= '{start_ts}'"
                cursor.execute(sql)
                # 注意：TDengine 的 DELETE 可能不返回受影响行数，或者根据版本不同有差异
                logger.info(f"🧹 [Cleanup] 已下达清理指令: {table} (Since: {start_ts})")
                results[table] = 1
            except Exception as e:
                logger.error(f"❌ [Cleanup] {table} 失败: {e}")
                results[table] = 0
        return results

    def get_latest_daily_date(self, symbol: str) -> Optional[str]:
        """[V3.3 TLS] 获取本地最新日期"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return None
        try:
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            table_name = f"d_{clean_symbol}"
            sql = f"SELECT MAX(ts) FROM {table_name}"
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0].strftime('%Y-%m-%d') if row and row[0] else None
        except Exception: return None

    def _get_table_id(self, symbol: str, prefix: str = "d") -> str:
        """统一生成表名"""
        clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
        if prefix == "dde":
            if clean_symbol.isdigit() and len(clean_symbol) == 6:
                return f"dde_sh{clean_symbol}" if clean_symbol.startswith(('6', '9', '5')) else f"dde_sz{clean_symbol}"
            return f"dde_{clean_symbol}"
        return f"{prefix}_{clean_symbol}"

    def get_latest_dde_date(self, symbol: str) -> Optional[str]:
        """获取 DDE 最新日期"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return None
        try:
            table_name = self._get_table_id(symbol, "dde")
            sql = f"SELECT MAX(ts) FROM {table_name}"
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0].strftime('%Y-%m-%d') if row and row[0] else None
        except Exception: return None

    def get_latest_factor_date(self, symbol: str) -> Optional[str]:
        """获取因子最新日期"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return None
        try:
            table_name = self._get_table_id(symbol, "factors")
            sql = f"SELECT MAX(ts) FROM {table_name}"
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0].strftime('%Y-%m-%d') if row and row[0] else None
        except Exception: return None

    def save_factors(self, symbol: str, df: pd.DataFrame) -> bool:
        """保存因子到 TDengine (V3.3 TLS) - [V39.2] 动态列对位版"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor" or df.empty: return False
        try:
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            table_name = self._get_table_id(symbol, "factors")
            
            # 基础列
            base_cols = ['ts', 'profit_ratio', 'avg_cost', 'concentration', 'bias_20', 'rsi_6', 'vol_ratio']
            # 扩展列 (V39.2)
            ext_cols = ['ma5', 'ma10', 'ma20', 'macd_dif', 'macd_dea', 'macd_hist', 'kdj_k', 'kdj_d', 'kdj_j', 'boll_up', 'boll_mid', 'boll_low']
            all_cols = base_cols + ext_cols

            for _, row in df.iterrows():
                ts = row['date'] if 'date' in row else (row['ts'] if 'ts' in row else row.name)
                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S') if hasattr(ts, 'strftime') else str(ts)
                
                # 动态构造 VALUES 部分
                vals = [f"'{ts_str}'"]
                for col in all_cols[1:]: # 跳过 ts
                    # [V40.10 Fix] 字段别名对位：profit_ratio <- change_pct_5d, avg_cost <- peak_price
                    alias_map = {
                        'profit_ratio': 'change_pct_5d',
                        'avg_cost': 'avg_cost',
                        'concentration': 'concentration',
                    }
                    actual_col = alias_map.get(col, col)
                    val = row.get(actual_col, row.get(col, row.get(col.upper(), 0)))
                    # 处理 NaN
                    if pd.isna(val): val = 0.0
                    vals.append(str(val))
                
                sql = f"INSERT INTO {table_name} USING daily_factors TAGS ('{symbol}') VALUES ({', '.join(vals)})"
                cursor.execute(sql)
            return True
        except Exception as e:
            logger.warning(f"❌ [TDengine] Factors 保存失败 {symbol}: {e}")
            return False

    def save_chips(self, symbol: str, peak_data: Dict) -> bool:
        """保存筹码到 TDengine (V3.3 TLS)"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor" or not peak_data: return False
        try:
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            table_name = f"chips_{clean_symbol}"
            # [V40.5] 严格时间对位：禁止使用系统请求时间，必须使用数据业务时间
            ts_str = peak_data.get('date')
            if not ts_str:
                logger.error(f"❌ [TDengine] {symbol} Chips 数据缺失日期，拒绝写入以防污染水位")
                return False
            sql = f"INSERT INTO {table_name} USING daily_chips TAGS ('{symbol}') VALUES ('{ts_str}', {peak_data.get('peak_price', 0)}, {peak_data.get('peak_weight', 0)}, {peak_data.get('upper_cost', 0)}, {peak_data.get('lower_cost', 0)})"
            cursor.execute(sql)
            return True
        except Exception as e:
            logger.warning(f"❌ [TDengine] Chips 保存失败 {symbol}: {e}")
            return False

    def execute_query(self, sql: str):
        """[V3.3 TLS] 执行查询"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return None
        try:
            cursor.execute(sql)
            return cursor
        except Exception as e:
            logger.error(f"❌ TDengine 执行查询失败: {e}\nSQL: {sql}")
            return None

    def get_daily_kline(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        """[V3.3 TLS] 从TDengine获取日线数据"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return []
        try:
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            table_name = f"d_{clean_symbol}"
            sql = f"SELECT * FROM {table_name} WHERE ts >= '{start_date}' AND ts <= '{end_date}' ORDER BY ts"
            cursor.execute(sql)
            data = []
            rows = cursor.fetchall()
            for row in rows:
                data.append({
                    'time': row[0].strftime('%Y-%m-%d'),
                    'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4],
                    'volume': row[5], 'amount': row[6], 'turn': row[7], 'pct_chg': row[8]
                })
            return data
        except Exception: return []

    def get_daily_count(self, target_day: str) -> int:
        """[V3.3 TLS] 获取指定日期记录总数"""
        cursor = self._get_cursor()
        if not cursor or cursor == "mock_cursor": return 0
        try:
            sql = f"SELECT COUNT(*) FROM daily_kline WHERE ts = '{target_day} 00:00:00'"
            cursor.execute(sql)
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] else 0
        except Exception as e:
            logger.error(f"❌ TDengine 查询数量失败: {e}")
            return 0

    def get_minute_kline(self, symbol: str, start_time: str = None, end_time: str = None, 
                        days: int = 1) -> List[Dict]:
        """
        从TDengine获取分钟K线数据 - 正确处理累计值volume和amount
        
        Args:
            symbol: 股票代码 (如 '000001')
            start_time: 开始时间 'YYYY-MM-DD HH:MM:SS'
            end_time: 结束时间 'YYYY-MM-DD HH:MM:SS'
            days: 获取最近几天的数据
            
        Returns:
            List[Dict]: 分钟K线数据
        """
        try:
            # 设置默认时间范围
            if not end_time:
                end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not start_time:
                start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            # 构建SQL查询 - 按1分钟聚合tick数据，正确处理累计值
            sql = f"""
            SELECT 
                _wstart as time,
                FIRST(lp) as open,
                MAX(lp) as high,
                MIN(lp) as low,
                LAST(lp) as close,
                LAST(v) - FIRST(v) as volume,  -- 处理累计值：用最后一个值减去第一个值
                LAST(a) - FIRST(a) as amount   -- 处理累计值：用最后一个值减去第一个值
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
            INTERVAL(1m)
            ORDER BY time
            """
            
            logger.debug(f"📊 查询TDengine分钟数据: {symbol}")
            
            cursor = self.execute_query(sql)
            if not cursor:
                return []
            
            # 处理查询结果 - 使用cursor.fetchall()方式，并确保浮点数保留两位小数
            data = []
            rows = cursor.fetchall()
            for row in rows:
                # row是一个元组，按SELECT字段顺序
                data.append({
                    'time': row[0].strftime('%Y-%m-%d %H:%M:%S'),
                    'open': round(float(row[1]) if row[1] is not None else 0, 2),
                    'high': round(float(row[2]) if row[2] is not None else 0, 2),
                    'low': round(float(row[3]) if row[3] is not None else 0, 2),
                    'close': round(float(row[4]) if row[4] is not None else 0, 2),
                    'volume': int(row[5]) if row[5] is not None else 0,
                    'amount': float(row[6]) if row[6] is not None else 0
                })
            
            logger.debug(f"✅ 从TDengine获取分钟数据成功: {symbol}, 数据点: {len(data)}")
            return data
            
        except Exception as e:
            logger.error(f"❌ 获取TDengine分钟数据失败: {e}")
            return []
    
    def get_tick_data(self, symbol: str, start_time: str = None, end_time: str = None, 
                     limit: int = 1000) -> List[Dict]:
        """
        获取原始tick数据（用于调试）
        
        Args:
            symbol: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            limit: 限制条数
            
        Returns:
            List[Dict]: tick数据
        """
        try:
            if not end_time:
                end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not start_time:
                start_time = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            
            sql = f"""
            SELECT 
                ts, lp, o, h, l, lc, a, v, p,
                ap1, ap2, ap3, ap4, ap5,
                bp1, bp2, bp3, bp4, bp5,
                av1, av2, av3, av4, av5,
                bv1, bv2, bv3, bv4, bv5,
                inst_vol, inst_amt, large_net
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
            ORDER BY ts
            LIMIT {limit}
            """
            
            cursor = self.execute_query(sql)
            if not cursor:
                return []
            
            # 获取字段信息
            fields = [field[0] for field in cursor.description]
            
            data = []
            rows = cursor.fetchall()
            for row in rows:
                row_dict = {}
                for i, field_name in enumerate(fields):
                    value = row[i]
                    if isinstance(value, datetime):
                        row_dict[field_name] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
                    elif value is None:
                        row_dict[field_name] = None
                    else:
                        row_dict[field_name] = value
                data.append(row_dict)
            
            return data
            
        except Exception as e:
            logger.error(f"❌ 获取TDengine tick数据失败: {e}")
            return []
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 TDengine连接已关闭")
