import json
import logging
from dataclasses import dataclass

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
    """Track hot-plate net-inflow slope to detect intraday capital migration."""

    def __init__(self, hub: IntradayDataHub):
        self.hub = hub
        self._history_window_seconds = 15 * 60

    @staticmethod
    def _get_redis_key(trade_date: str) -> str:
        return f"cache:sector_flow:{trade_date}"

    def update_and_evaluate(
        self,
        trade_date: str,
        phase: RunPhase,
        session_facts: SessionFacts,
        timestamp_ms: int,
    ) -> dict[str, SectorFlowTrajectory]:
        if phase not in (RunPhase.INTRADAY, RunPhase.POSTMARKET):
            return {}
        hot_plate_facts = tuple(getattr(session_facts, "hot_plate_today", ()) or getattr(session_facts, "hot_plate_facts", ()) or ())
        if not hot_plate_facts or timestamp_ms <= 0:
            return {}

        snapshot = {
            fact.plate_name: float(fact.net_inflow_yi or 0.0)
        for fact in hot_plate_facts
            if fact.plate_name
        }
        if not snapshot:
            return {}

        redis_key = self._get_redis_key(trade_date)
        current_time_sec = timestamp_ms / 1000.0
        payload = json.dumps(snapshot, ensure_ascii=False)
        try:
            self.hub.redis.zadd(redis_key, {payload: current_time_sec})
            self.hub.redis.zremrangebyscore(redis_key, 0, current_time_sec - self._history_window_seconds)
            self.hub.redis.expire(redis_key, 7200)
            raw_history = self.hub.redis.zrange(redis_key, 0, -1, withscores=True)
        except Exception as exc:
            logger.warning("sector flow tracking unavailable: %s", exc)
            return {}

        history: list[tuple[float, dict[str, float]]] = []
        for raw_payload, ts in raw_history:
            try:
                decoded = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
                data = json.loads(decoded)
                history.append((float(ts), {str(k): float(v or 0.0) for k, v in data.items()}))
            except Exception:
                continue

        if len(history) < 2:
            return {
                plate: SectorFlowTrajectory(
                    plate_name=plate,
                    current_net_inflow_yi=inflow,
                    slope_15m_yi_per_min=0.0,
                    is_withdrawing=False,
                    is_accelerating=False,
                    data_points=len(history),
                )
                for plate, inflow in snapshot.items()
            }

        first_ts, first_data = history[0]
        last_ts, _last_data = history[-1]
        time_delta_mins = (last_ts - first_ts) / 60.0

        results: dict[str, SectorFlowTrajectory] = {}
        for plate, current_inflow in snapshot.items():
            first_inflow = float(first_data.get(plate, current_inflow) or 0.0)
            slope = (current_inflow - first_inflow) / time_delta_mins if time_delta_mins > 0 else 0.0
            is_withdrawing = slope <= -1.5 and current_inflow < max(0.0, first_inflow - 10.0)
            is_accelerating = slope >= 2.0 and current_inflow > 0.0
            results[plate] = SectorFlowTrajectory(
                plate_name=plate,
                current_net_inflow_yi=current_inflow,
                slope_15m_yi_per_min=slope,
                is_withdrawing=is_withdrawing,
                is_accelerating=is_accelerating,
                data_points=len(history),
            )
        return results
