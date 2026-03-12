from __future__ import annotations

import dataclasses
import datetime as dt
import json
from typing import Any, AsyncIterator, Dict, Optional

from apps.nikkei_iceberg_monitor.monitor import BookEvent, TradeEvent


@dataclasses.dataclass
class HyperSbiConfig:
    ws_url: str
    token: str
    symbol: str
    contract_month: Optional[str] = None


class HyperSbiClient:
    """HYPER SBI API向けのWebSocketクライアント。

    HYPER SBI側の配信仕様差異を吸収するため、受信payloadを
    `book` / `trade` に正規化して監視器へ渡す。
    """

    def __init__(self, config: HyperSbiConfig):
        self.config = config

    async def stream_events(self) -> AsyncIterator[BookEvent | TradeEvent]:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover - runtime guard
            raise RuntimeError(
                "realtimeモードには `websockets` パッケージが必要です。"
                "`pip install websockets` を実行してください。"
            ) from exc

        headers = {"Authorization": f"Bearer {self.config.token}"}
        async with websockets.connect(self.config.ws_url, additional_headers=headers) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "symbol": self.config.symbol,
                        "contract_month": self.config.contract_month,
                        "channels": ["board", "executions"],
                    }
                )
            )

            async for raw in ws:
                payload = json.loads(raw)
                event = normalize_hyper_sbi_payload(payload)
                if event is None:
                    continue
                if not self._match_instrument(payload):
                    continue
                yield event

    def _match_instrument(self, payload: Dict[str, Any]) -> bool:
        symbol = payload.get("symbol")
        if symbol and symbol != self.config.symbol:
            return False

        if self.config.contract_month:
            month = payload.get("contract_month")
            if month and month != self.config.contract_month:
                return False

        return True


def normalize_hyper_sbi_payload(payload: Dict[str, Any]) -> BookEvent | TradeEvent | None:
    """HYPER SBI配信payloadを検知器イベントへ変換する。"""
    message_type = payload.get("type")
    ts = _parse_timestamp(payload.get("ts"))

    if message_type == "board":
        side = payload.get("side")
        price = payload.get("price")
        size = payload.get("size")
        if side in {"bid", "ask"} and price is not None and size is not None:
            return BookEvent(timestamp=ts, side=side, price=float(price), size=int(size))
        return None

    if message_type == "execution":
        aggressor = payload.get("aggressor")
        price = payload.get("price")
        size = payload.get("size")
        if aggressor in {"buy", "sell"} and price is not None and size is not None:
            return TradeEvent(timestamp=ts, side=aggressor, price=float(price), size=int(size))
        return None

    return None


def _parse_timestamp(value: Any) -> dt.datetime:
    if isinstance(value, str):
        return dt.datetime.fromisoformat(value)
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).replace(tzinfo=None)
    return dt.datetime.utcnow()
