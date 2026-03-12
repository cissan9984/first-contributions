from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime as dt
import json
from collections import defaultdict, deque
from typing import AsyncIterable, Deque, Dict, Iterable, Optional


@dataclasses.dataclass
class Thresholds:
    min_absorbed_volume: int = 120
    min_replenishments: int = 3
    max_price_drift_ticks: int = 1
    min_alert_cooldown_sec: int = 15
    sliding_window_sec: int = 30


@dataclasses.dataclass
class BookEvent:
    timestamp: dt.datetime
    side: str  # bid / ask
    price: float
    size: int


@dataclasses.dataclass
class TradeEvent:
    timestamp: dt.datetime
    side: str  # buy / sell aggressor
    price: float
    size: int


@dataclasses.dataclass
class PriceLevelState:
    visible_size: int = 0
    absorbed_volume: int = 0
    replenishments: int = 0
    last_alert_at: Optional[dt.datetime] = None
    trade_flow: Deque[tuple[dt.datetime, int]] = dataclasses.field(default_factory=deque)


class IcebergDetector:
    """Simple heuristic detector for iceberg-like behavior at a fixed price level.

    Logic:
    - Track visible queue size changes from board updates.
    - When marketable flow hits that price (from tape), count absorbed volume.
    - If visible size refills repeatedly while absorbed volume grows, emit alert.
    """

    def __init__(self, thresholds: Thresholds):
        self.thresholds = thresholds
        self.state: Dict[tuple[str, float], PriceLevelState] = defaultdict(PriceLevelState)

    def on_book(self, event: BookEvent) -> None:
        key = (event.side, event.price)
        level = self.state[key]

        previous_size = level.visible_size
        level.visible_size = event.size

        if previous_size > 0 and event.size > previous_size:
            level.replenishments += 1

    def on_trade(self, event: TradeEvent) -> Optional[str]:
        resting_side = "ask" if event.side == "buy" else "bid"
        key = (resting_side, event.price)
        level = self.state[key]

        level.absorbed_volume += event.size
        level.trade_flow.append((event.timestamp, event.size))
        self._evict_old(level.trade_flow, event.timestamp)

        if self._should_alert(level, event.timestamp):
            level.last_alert_at = event.timestamp
            return self._format_alert(resting_side, event.price, level)

        return None

    def _should_alert(self, level: PriceLevelState, now: dt.datetime) -> bool:
        if level.absorbed_volume < self.thresholds.min_absorbed_volume:
            return False
        if level.replenishments < self.thresholds.min_replenishments:
            return False

        if level.last_alert_at:
            elapsed = (now - level.last_alert_at).total_seconds()
            if elapsed < self.thresholds.min_alert_cooldown_sec:
                return False

        recent_volume = sum(size for _, size in level.trade_flow)
        return recent_volume >= self.thresholds.min_absorbed_volume

    def _evict_old(self, flow: Deque[tuple[dt.datetime, int]], now: dt.datetime) -> None:
        cutoff = now - dt.timedelta(seconds=self.thresholds.sliding_window_sec)
        while flow and flow[0][0] < cutoff:
            flow.popleft()

    @staticmethod
    def _format_alert(resting_side: str, price: float, level: PriceLevelState) -> str:
        return (
            f"[ALERT] iceberg候補: side={resting_side} price={price:.1f} "
            f"absorbed={level.absorbed_volume} replenishments={level.replenishments} "
            f"visible={level.visible_size}"
        )


def parse_event(raw: str) -> BookEvent | TradeEvent:
    payload = json.loads(raw)
    timestamp = dt.datetime.fromisoformat(payload["ts"])
    event_type = payload["type"]
    if event_type == "book":
        return BookEvent(
            timestamp=timestamp,
            side=payload["side"],
            price=float(payload["price"]),
            size=int(payload["size"]),
        )
    if event_type == "trade":
        return TradeEvent(
            timestamp=timestamp,
            side=payload["side"],
            price=float(payload["price"]),
            size=int(payload["size"]),
        )
    raise ValueError(f"unknown type: {event_type}")


async def consume_lines(lines: Iterable[str], detector: IcebergDetector) -> None:
    async def _iter() -> AsyncIterable[BookEvent | TradeEvent]:
        for raw in lines:
            await asyncio.sleep(0)
            yield parse_event(raw)

    await consume_events(_iter(), detector)


async def consume_events(events: AsyncIterable[BookEvent | TradeEvent], detector: IcebergDetector) -> None:
    async for event in events:
        if isinstance(event, BookEvent):
            detector.on_book(event)
        else:
            alert = detector.on_trade(event)
            if alert:
                print(alert)


def load_thresholds(path: Optional[str]) -> Thresholds:
    if not path:
        return Thresholds()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return Thresholds(**data)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="日経先物の板情報/歩み値からアイスバーン注文候補を監視するCLI"
    )
    parser.add_argument(
        "--input",
        default="-",
        help="JSONLイベント入力ファイル。'-'の場合はstdinから読み込み",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="しきい値JSONファイルのパス",
    )
    parser.add_argument("--realtime", action="store_true", help="HYPER SBI API WebSocketからリアルタイム受信")
    parser.add_argument("--ws-url", default=None, help="HYPER SBI API WebSocket URL")
    parser.add_argument("--token", default=None, help="HYPER SBI APIアクセストークン")
    parser.add_argument("--symbol", default="NK225", help="監視シンボル (例: NK225)")
    parser.add_argument("--contract-month", default=None, help="限月フィルタ (例: 202606)")
    args = parser.parse_args()

    thresholds = load_thresholds(args.config)
    detector = IcebergDetector(thresholds)

    if args.realtime:
        if not args.ws_url or not args.token:
            raise SystemExit("--realtime利用時は --ws-url と --token が必要です")

        from apps.nikkei_iceberg_monitor.hyper_sbi_client import HyperSbiClient, HyperSbiConfig

        client = HyperSbiClient(
            HyperSbiConfig(
                ws_url=args.ws_url,
                token=args.token,
                symbol=args.symbol,
                contract_month=args.contract_month,
            )
        )
        asyncio.run(consume_events(client.stream_events(), detector))
        return

    if args.input == "-":
        import sys

        lines = (line.strip() for line in sys.stdin if line.strip())
    else:
        with open(args.input, "r", encoding="utf-8") as f:
            file_lines = [line.strip() for line in f if line.strip()]
        lines = file_lines

    asyncio.run(consume_lines(lines, detector))


if __name__ == "__main__":
    main()
