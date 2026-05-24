import json
import logging
from dataclasses import dataclass
from typing import Any

from engine_next.domain.enums import RunPhase
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.session_facts import SessionFacts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SectorFlowTrajectory:
    plate_name: str
    current_net_inflow_yi: float
    slope_15m_yi_per_min: float
    is_withdrawing: bool
    is_accelerating: bool
    data_points: int


class SectorFlowTracker:
    """Tracks the slope of net inflows for top plates to detect intraday migration."""

    def __init__(self, hub: IntradayDataHub):
        self.hub = hub
        self._history_window_seconds = 15 * 60  # 15 minutes

    def _get_redis_key(self, trade_date: str) -> str:
        return f"cache:sector_flow:{trade_date}"

    def update_and_evaluate(
        self,
        trade_date: str,
        phase: RunPhase,
        session_facts: SessionFacts,
        timestamp_ms: int,
    ) -> dict[str, SectorFlowTrajectory]:
        """Records current inflow and evaluates trajectory for all hot plates."""
        if phase not in (RunPhase.INTRADAY, RunPhase.POSTMARKET):
            return {}
            
        if not session_facts.hot_plate_facts or timestamp_ms <= 0:
            return {}

        redis_key = self._get_redis_key(trade_date)
        current_time_sec = timestamp_ms / 1000.0
        
        # 1. Prepare current snapshot
        snapshot = {}
        for fact in session_facts.hot_plate_facts:
            if not fact.plate_name:
                continue
            snapshot[fact.plate_name] = fact.net_inflow_yi

        if not snapshot:
            return {}

        # 2. Append to Redis TimeSeries (we use ZSET for simple TS)
        # ZSET member: JSON string of snapshot, score: timestamp
        payload = json.dumps(snapshot, ensure_ascii=False)
        try:
            self.hub.redis.zadd(redis_key, {payload: current_time_sec})
            # Trim old data (older than 15 mins)
            cutoff_time = current_time_sec - self._history_window_seconds
            self.hub.redis.zremrangebyscore(redis_key, 0, cutoff_time)
            # Expire after 2 hours (intraday only)
            self.hub.redis.expire(redis_key, 7200)
        except Exception as e:
            logger.warning(f"Failed to update sector flow tracking: {e}")
            return {}

        # 3. Read back recent history to compute slope
        try:
            raw_history = self.hub.redis.zrange(redis_key, 0, -1, withscores=True)
        except Exception as e:
            logger.warning(f"Failed to read sector flow tracking: {e}")
            return {}

        history = []
        for raw_payload, ts in raw_history:
            try:
                data = json.loads(raw_payload)
                history.append((float(ts), data))
            except Exception:
                continue
                
        if len(history) < 2:
            # Not enough data points to compute slope
            return {
                plate: SectorFlowTrajectory(
                    plate_name=plate,
                    current_net_inflow_yi=inflow,
                    slope_15m_yi_per_min=0.0,
                    is_withdrawing=False,
                    is_accelerating=False,
                    data_points=len(history)
                ) for plate, inflow in snapshot.items()
            }

        # 4. Compute slope
        # For simplicity, we use (last - first) / (time_delta_minutes)
        first_ts, first_data = history[0]
        last_ts, last_data = history[-1]
        time_delta_mins = (last_ts - first_ts) / 60.0
        
        results = {}
        for plate, current_inflow in snapshot.items():
            first_inflow = first_data.get(plate, current_inflow) # Assume steady if missing
            
            slope = 0.0
            if time_delta_mins > 0:
                slope = (current_inflow - first_inflow) / time_delta_mins
                
            # Definitions for migration:
            # Withdrawing: Slope < -1.5 yi/min and current inflow dropping significantly
            is_withdrawing = slope <= -1.5 and current_inflow < max(0, first_inflow - 10.0)
            
            # Accelerating: Slope > 2.0 yi/min and positive inflow
            is_accelerating = slope >= 2.0 and current_inflow > 0
            
            results[plate] = SectorFlowTrajectory(
                plate_name=plate,
                current_net_inflow_yi=current_inflow,
                slope_15m_yi_per_min=slope,
                is_withdrawing=is_withdrawing,
                is_accelerating=is_accelerating,
                data_points=len(history)
            )
            
        return results
