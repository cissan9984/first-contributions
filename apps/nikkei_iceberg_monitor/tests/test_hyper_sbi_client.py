import datetime as dt

from apps.nikkei_iceberg_monitor.hyper_sbi_client import normalize_hyper_sbi_payload
from apps.nikkei_iceberg_monitor.monitor import BookEvent, TradeEvent


def test_normalize_board_payload() -> None:
    payload = {
        "type": "board",
        "ts": "2026-01-10T09:00:00",
        "side": "ask",
        "price": 40320,
        "size": 25,
    }

    event = normalize_hyper_sbi_payload(payload)

    assert isinstance(event, BookEvent)
    assert event.timestamp == dt.datetime(2026, 1, 10, 9, 0, 0)
    assert event.side == "ask"


def test_normalize_execution_payload() -> None:
    payload = {
        "type": "execution",
        "ts": "2026-01-10T09:00:01",
        "aggressor": "buy",
        "price": 40320,
        "size": 10,
    }

    event = normalize_hyper_sbi_payload(payload)

    assert isinstance(event, TradeEvent)
    assert event.side == "buy"
    assert event.size == 10
