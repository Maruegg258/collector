from __future__ import annotations

import math

import pytest

from app.collector import HypeSpotCollector
from app.storage import TradeStore


def sample_trade(*, side: str = "B", tid: int = 123, time_ms: int = 1_700_000_000_000):
    return {
        "coin": "@107",
        "side": side,
        "px": "82.5",
        "sz": "2.0",
        "hash": "0xabc",
        "time": time_ms,
        "tid": tid,
        "users": ["0xbuyer", "0xseller"],
    }


def test_parse_buy_trade_has_positive_signed_notional(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    collector = HypeSpotCollector(store)
    record = collector.parse_trade(sample_trade(side="B"))
    assert math.isclose(record.notional_usdc, 165.0)
    assert math.isclose(record.signed_notional_usdc, 165.0)
    store.close()


def test_parse_sell_trade_has_negative_signed_notional(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    collector = HypeSpotCollector(store)
    record = collector.parse_trade(sample_trade(side="A"))
    assert math.isclose(record.notional_usdc, 165.0)
    assert math.isclose(record.signed_notional_usdc, -165.0)
    store.close()


def test_invalid_side_is_rejected(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    collector = HypeSpotCollector(store)
    with pytest.raises(ValueError):
        collector.parse_trade(sample_trade(side="X"))
    store.close()


def test_websocket_message_is_persisted_and_deduplicated(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    collector = HypeSpotCollector(store)
    message = {"channel": "trades", "data": [sample_trade()]}

    assert collector.process_message(message) == (1, 0)
    assert collector.process_message(message) == (0, 1)
    assert store.count() == 1
    assert collector.state.trades_seen == 2
    assert collector.state.trades_inserted == 1
    assert collector.state.duplicates_ignored == 1
    store.close()


def test_non_trade_message_is_ignored(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    collector = HypeSpotCollector(store)
    assert collector.process_message({"channel": "subscriptionResponse", "data": {}}) == (0, 0)
    assert store.count() == 0
    store.close()
