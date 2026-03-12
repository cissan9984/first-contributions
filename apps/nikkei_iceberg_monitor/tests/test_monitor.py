import datetime as dt

from apps.nikkei_iceberg_monitor.monitor import (
    BookEvent,
    IcebergDetector,
    Thresholds,
    TradeEvent,
)


def test_alert_when_absorption_and_replenishments_reach_threshold() -> None:
    detector = IcebergDetector(
        Thresholds(min_absorbed_volume=100, min_replenishments=2, sliding_window_sec=30)
    )
    base = dt.datetime(2026, 1, 10, 9, 0, 0)

    detector.on_book(BookEvent(base, "ask", 40320.0, 20))
    detector.on_trade(TradeEvent(base + dt.timedelta(seconds=1), "buy", 40320.0, 40))
    detector.on_book(BookEvent(base + dt.timedelta(seconds=2), "ask", 40320.0, 24))
    detector.on_trade(TradeEvent(base + dt.timedelta(seconds=3), "buy", 40320.0, 35))
    detector.on_book(BookEvent(base + dt.timedelta(seconds=4), "ask", 40320.0, 28))

    alert = detector.on_trade(TradeEvent(base + dt.timedelta(seconds=5), "buy", 40320.0, 30))

    assert alert is not None
    assert "iceberg候補" in alert
    assert "price=40320.0" in alert


def test_no_alert_without_replenishment() -> None:
    detector = IcebergDetector(
        Thresholds(min_absorbed_volume=80, min_replenishments=2, sliding_window_sec=30)
    )
    base = dt.datetime(2026, 1, 10, 9, 0, 0)

    detector.on_book(BookEvent(base, "ask", 40320.0, 20))
    detector.on_trade(TradeEvent(base + dt.timedelta(seconds=1), "buy", 40320.0, 30))
    detector.on_trade(TradeEvent(base + dt.timedelta(seconds=2), "buy", 40320.0, 30))
    alert = detector.on_trade(TradeEvent(base + dt.timedelta(seconds=3), "buy", 40320.0, 30))

    assert alert is None
