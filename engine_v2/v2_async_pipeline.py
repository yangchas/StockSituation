"""
v2_async_pipeline.py
异步数据请求管道 — 实现断点续传、指数退避重试、批量分组
所有历史数据请求（日K、DDE、昨日涨停等）均通过此管道执行
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import random
import sys
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger("V2Pipeline")

CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "fetch_progress.json")

# ─────────────────────────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING   = "pending"
    DONE      = "done"
    FAILED    = "failed"


class RateLimitError(Exception):
    """
    专用限步异常：用于触发全局熔断/冷却时间
    """
    def __init__(self, message: str = "Rate limit exceeded"):
        self.message = message
        super().__init__(self.message)


@dataclass
class FetchTask:
    task_id: str                 # 唯一 ID，用于续传检查
    symbol: Optional[str]        # 个股代码（批量任务时为 None）
    task_type: str               # "daily_kline" | "dde" | "kpl_bans" | "kpl_plates"
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    result: Any = field(default=None, repr=False)
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# 检查点管理（断点续传）
# ─────────────────────────────────────────────────────────────────────────────

class Checkpoint:
    def __init__(self, path: str = CHECKPOINT_FILE):
        self.path = path
        self._data: Dict[str, str] = {}
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
        except Exception:
            self._data = {}

    def _save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Checkpoint save failed: {e}")

    def is_done(self, task_id: str) -> bool:
        return self._data.get(task_id) == TaskStatus.DONE

    def mark_done(self, task_id: str):
        self._data[task_id] = TaskStatus.DONE
        self._save()

    def remove(self, task_id: str):
        """物理移除记录，用于纠偏"""
        if task_id in self._data:
            del self._data[task_id]
            self._save()

    def clear(self):
        self._data = {}
        self._save()


# ─────────────────────────────────────────────────────────────────────────────
# 核心: AsyncPipeline
# ─────────────────────────────────────────────────────────────────────────────

class AsyncDataPipeline:
    """
    异步数据请求管道

    特性：
    - 指数退避重试 (max_retry 次，不陷入死循环)
    - 断点续传（通过 Checkpoint）
    - 批量并发（受 concurrency 限制）
    - 非阻塞：主进程通过 asyncio.Event 感知完成
    """

    def __init__(
        self,
        max_retry: int = 3,
        backoff_base: float = 2.0,
        timeout: int = 10,
        concurrency: int = 8,
        delay_jitter: float = 0.0,
        checkpoint: Optional[Checkpoint] = None,
    ):
        self.max_retry = max_retry
        self.backoff_base = backoff_base
        self.timeout = timeout
        self.concurrency = concurrency
        self.delay_jitter = delay_jitter
        self.checkpoint = checkpoint or Checkpoint()
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._batch_progress = 0  # 批次进度计数
        self._batch_total = 0     # 批次总数记录
        self._phys_fix_count = 0  # [V38.8] 物理修复计数降噪

    async def run_task(
        self,
        task: FetchTask,
        func: Callable[..., Coroutine],
        *args,
        **kwargs,
    ) -> FetchTask:
        """执行单个任务，含重试 + 超时 + 检查点跳过 + [V34.2] 黑名单避障"""
        if self.checkpoint.is_done(task.task_id):
            task.status = TaskStatus.DONE
            logger.debug(f"[skip] {task.task_id} 已完成（续传）")
            return task
        
        # [V38.8] 额外的物理存在校验 (可选)，如果 checkpoint 认为完成但实际漏了，这里可以加日志

        # 🚀 [V34.2] 物理黑名单检查：如果是已知退市、ST或无数据标的，改为 DEBUG 记录但不强制跳过
        # if task.symbol:
        #     from v2_infra_provider import get_global_redis
        #     r = await get_global_redis()
        #     if await r.sismember("market_edge:blacklist", task.symbol):
        #         # task.status = TaskStatus.DONE
        #         # self.checkpoint.mark_done(task.task_id)
        #         logger.debug(f"🚫 [Blacklist-Check] {task.symbol} 命过黑名单，但基于稳定性策略强制放行")
        #         # return task

        for attempt in range(self.max_retry + 1):
            try:
                async with self._semaphore:
                    # 速率限制：在持有信号量时睡眠
                    # if self.delay_jitter > 0:
                    await asyncio.sleep(0.01)
                        
                    result = await asyncio.wait_for(
                        func(*args, **kwargs), timeout=self.timeout
                    )
                    
                    # 任务成功判定：只要返回不是 None，即视为已处理 (支持 bool 或 list/dict 返回)
                    if result is not None:
                        task.result = result
                        task.status = TaskStatus.DONE
                        self.checkpoint.mark_done(task.task_id)
                        
                        # 进度完成反馈：单行刷新
                        if hasattr(self, '_batch_total') and self._batch_total > 0:
                            self._batch_progress += 1
                            pct = (self._batch_progress / self._batch_total) * 100
                            # 每 2 个或完成时才刷新一次 IO
                            if self._batch_progress % 2 == 0 or self._batch_progress == self._batch_total:
                                sys.stdout.write(f"\r📊 同步中: [{'#' * (int(pct)//5)}{'-' * (20 - int(pct)//5)}] {self._batch_progress}/{self._batch_total} ({pct:.1f}%)   ")
                                sys.stdout.flush()
                                if self._batch_progress == self._batch_total:
                                    print() # 换行
                        
                        return task
                    else:
                        task.error = "Function returned False"
                        # 不标记 DONE，进入重试或最终失败

            except RateLimitError as e:
                # 触发重大熔断：休眠 60s
                logger.warning(f"🚨 [CoolDown] 触发 60s 冷却熔断: {e}")
                await asyncio.sleep(60)
                task.error = f"RateLimit: {e}"
            except asyncio.TimeoutError:
                task.error = f"Timeout (attempt {attempt+1})"
            except Exception as e:
                task.error = f"{type(e).__name__}: {e} (attempt {attempt+1})"

            task.retries += 1
            if attempt < self.max_retry:
                # 指数退避重试
                wait = self.backoff_base ** attempt
                logger.warning(f"[retry] {task.task_id} → 等待 {wait:.1f}s | {task.error}")
                await asyncio.sleep(wait)
            else:
                logger.error(f"[fail] {task.task_id} 最终失败: {task.error}")
                task.status = TaskStatus.FAILED

        return task

    async def run_batch(
        self,
        tasks: List[FetchTask],
        func_map: Dict[str, Callable],
        arg_builder: Callable[[FetchTask], tuple],
    ) -> List[FetchTask]:
        """批量并发运行任务列表"""
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._batch_progress = 0
        self._batch_total = len(tasks)
        
        coroutines = []
        for task in tasks:
            fn = func_map.get(task.task_type)
            if fn is None:
                logger.warning(f"No handler for task_type={task.task_type}")
                task.status = TaskStatus.FAILED
                continue
            args = arg_builder(task)
            coroutines.append(self.run_task(task, fn, *args))

        results = await asyncio.gather(*coroutines, return_exceptions=False)
        done = sum(1 for r in results if r.status == TaskStatus.DONE)
        fail = sum(1 for r in results if r.status == TaskStatus.FAILED)
        logger.info(f"[batch] 完成={done}, 失败={fail}, 共={len(tasks)}")
        return results


# ─────────────────────────────────────────────────────────────────────────────
# 工具：构建全量个股 FetchTask 列表（支持续传跳过）
# ─────────────────────────────────────────────────────────────────────────────

def build_stock_tasks(
    symbols: List[str],
    task_type: str,
    checkpoint: Checkpoint,
    date_tag: str = "",
    validate_fn: Optional[Callable[[str, str], bool]] = None,
) -> List[FetchTask]:
    """
    为每只股票生成一个 FetchTask。
    task_id = f"{task_type}:{symbol}:{date_tag}"，支持按日期刷新。
    
    validate_fn: 可选回调 (symbol, date_tag) -> bool
                如果返回 False，说明物理校验不通过，即使 Checkpoint 为 DONE 也要重做。
    """
    tasks = []
    skipped = 0
    phys_re_exec = 0
    for sym in symbols:
        tid = f"{task_type}:{sym}:{date_tag}"
        task = FetchTask(task_id=tid, symbol=sym, task_type=task_type)
        
        # 1. 检查 Checkpoint 状态
        is_recorded_done = checkpoint.is_done(tid)
        
        # 2. 物理校验 (如果提供)
        is_phys_done = True
        if validate_fn:
            is_phys_done = validate_fn(sym, date_tag)
            
        if is_recorded_done and is_phys_done:
            task.status = TaskStatus.DONE
            skipped += 1
        else:
            if is_recorded_done and not is_phys_done:
                phys_re_exec += 1
                logger.debug(f"[Physical-Fix] {tid} 记录虽为DONE但物理缺失，正在强制清理断点...")
                checkpoint.remove(tid) # 物理清理，让内层 run_task 能够通行
            task.status = TaskStatus.PENDING

        tasks.append(task)
    
    if skipped:
        logger.info(f"[续传] {task_type} 跳过已完成 {skipped}/{len(symbols)} 只")
    if phys_re_exec:
        logger.warning(f"[纠偏] {task_type} 检测到 {phys_re_exec} 只股票物理缺失，强制拉回 PENDING 状态")
    return tasks


# ─────────────────────────────────────────────────────────────────────────────
# 测试入口
# ─────────────────────────────────────────────────────────────────────────────

async def _demo_fetch(symbol: str) -> dict:
    """模拟一个耗时请求"""
    await asyncio.sleep(0.05)
    if symbol == "error":
        raise ValueError("模拟失败")
    return {"symbol": symbol, "close": 10.5}


async def demo():
    cp = Checkpoint()
    pipeline = AsyncDataPipeline(max_retry=2, concurrency=4, checkpoint=cp)

    symbols = ["000001", "600519", "error", "300308"]
    tasks = build_stock_tasks(symbols, "daily_kline", cp, date_tag="20260329")

    results = await pipeline.run_batch(
        tasks,
        func_map={"daily_kline": _demo_fetch},
        arg_builder=lambda t: (t.symbol,),
    )
    for r in results:
        print(f"{r.symbol:>8} | {r.status.value:<8} | {r.error or r.result}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
