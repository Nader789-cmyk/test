#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMCI V7 Feature Recorder + V2 Trigger Paper Trading Engine — AON Safety Build
=======================================

Purpose
-------
Capture the widest practical day-trading dataset from Moomoo + optional IBKR
WITHOUT placing trades. This is a recorder / research database builder only.

V4 keeps async Moomoo push ingestion and adds production-safety engineering on top of V3:
  - exponential L2 decay plus top-3/top-10/top-50 depth features
  - bounded push deques for SMCI and peer tape buffers
  - atomic buffer-swap drains to minimize lock contention during volatile opens
  - cycle/bar/IBKR timestamp alignment columns to avoid look-ahead drift
  - peer subscriptions and peer tape buffers for NVDA/AMD/SMH/QQQ/SPY
  - beta-adjusted SMCI momentum vs QQQ/SPY
  - second-level peer lead-lag tape features, especially NVDA vs SMCI
Polling remains a safety net for bars/snapshots/capital-flow and for SDKs that do not expose push handlers.

V6 architecture:
  - push/poll events are summarized in RAM only
  - features_wide.csv appends one engineered row after each closed RTH minute
  - no 5-second feature rows, to avoid duplicated/stale ML rows

It preserves the feature columns from the attached files:
  - COIN_live_features_2026-07-02.csv style 39-column live features
  - MRVL_2026-07-02_v5_merged_features.csv style 57-column merged features

Output:
  - *_features_wide.csv        ONE ENGINEERED ROW PER CLOSED RTH MINUTE ONLY

Raw tape/orderbook/quote events are NOT written to disk in v7. They are held in bounded
RAM buffers and distilled into minute-level feature columns such as block delta, L2
persistence, spread statistics, book-update counts, and peer lead/lag metrics.
  - *_heartbeat.log            status/errors

Run example
-----------
python SMCI_Slope20_Only_IBKR_Paper_NAMEERROR_FIXED.py --symbol SMCI --mode paper --ibkr-paper-port 4002 --ibkr-client-id 8831

python SMCI_Slope20_Only_IBKR_Paper.py --symbol SMCI --mode paper --ibkr-paper-port 4002 --ibkr-client-id 8831

Requirements
------------
pip install pandas numpy pytz moomoo ib_insync
Moomoo OpenD running on 127.0.0.1:11111
IBKR TWS/Gateway paper API optional on 127.0.0.1:4002

Important
---------
This engine records the full v7 feature set and can place IBKR paper orders by default. Live mode requires an explicit acknowledgement flag.
"""

# SLOPE20 CONVERSION: paper only; original Slope20 thresholds, +1% native limit,
# one entry per NY day, no stop or daily loss floor, 15:50 NY forced liquidation.
# Live IBKR paper execution has not yet been verified.

from __future__ import annotations

import argparse
import csv
import json
import math
import queue
import threading
import time
import traceback
from collections import deque
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

try:
    import moomoo as ft
    from moomoo import OpenQuoteContext, RET_OK
    try:
        from moomoo import TickerHandlerBase, OrderBookHandlerBase, StockQuoteHandlerBase, CurKlineHandlerBase
    except Exception:
        TickerHandlerBase = OrderBookHandlerBase = StockQuoteHandlerBase = CurKlineHandlerBase = None
except Exception:
    try:
        import futu as ft
        from futu import OpenQuoteContext, RET_OK
        try:
            from futu import TickerHandlerBase, OrderBookHandlerBase, StockQuoteHandlerBase, CurKlineHandlerBase
        except Exception:
            TickerHandlerBase = OrderBookHandlerBase = StockQuoteHandlerBase = CurKlineHandlerBase = None
    except Exception:
        ft = None
        OpenQuoteContext = None
        RET_OK = 0
        TickerHandlerBase = OrderBookHandlerBase = StockQuoteHandlerBase = CurKlineHandlerBase = None

try:
    from ib_insync import IB, Stock, MarketOrder, LimitOrder, StopOrder, util
except Exception:
    IB = None
    Stock = None
    MarketOrder = None
    LimitOrder = None
    StopOrder = None
    util = None

NY_TZ = pytz.timezone("America/New_York")
RTH_START = dtime(9, 30)
RTH_END = dtime(16, 0)

def is_rth_now(dt: Optional[datetime] = None) -> bool:
    t = (dt or now_ny()).time()
    return RTH_START <= t < RTH_END

def before_rth_now(dt: Optional[datetime] = None) -> bool:
    t = (dt or now_ny()).time()
    return t < RTH_START

def after_rth_now(dt: Optional[datetime] = None) -> bool:
    t = (dt or now_ny()).time()
    return t >= RTH_END

def seconds_until_rth_start(dt: Optional[datetime] = None) -> float:
    cur = dt or now_ny()
    start = cur.replace(hour=RTH_START.hour, minute=RTH_START.minute, second=0, microsecond=0)
    return max(0.0, (start - cur).total_seconds())

def filter_rth_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "ny_dt" not in df.columns:
        if "date" in df.columns:
            tmp = df.copy()
            tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce", utc=True)
            tmp["ny_dt"] = tmp["date"].dt.tz_convert(NY_TZ)
            return tmp[tmp["ny_dt"].dt.time.apply(lambda t: RTH_START <= t < RTH_END)].copy()
        return df
    return df[df["ny_dt"].dt.time.apply(lambda t: RTH_START <= t < RTH_END)].copy()


def rth_minute_floor(dt: Optional[datetime] = None) -> datetime:
    cur = (dt or now_ny()).astimezone(NY_TZ)
    return cur.replace(second=0, microsecond=0)

def latest_closed_rth_bar(bars: pd.DataFrame, asof: Optional[datetime] = None) -> Tuple[Optional[pd.Timestamp], pd.DataFrame]:
    """Return the latest closed 1-minute RTH bar and bars up to it.

    Moomoo 1m timestamps are treated as bar-start timestamps. At 09:31:05,
    the 09:30:00 bar is closed and the 09:31:00 bar is still forming.
    This prevents feature rows from using incomplete current-minute candles.
    """
    if bars is None or bars.empty:
        return None, pd.DataFrame()
    df = bars.copy()
    if "date" not in df.columns:
        return None, pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return None, pd.DataFrame()
    cur_floor_ny = rth_minute_floor(asof)
    cur_floor_utc = pd.Timestamp(cur_floor_ny.astimezone(pytz.UTC))
    closed = df[df["date"] < cur_floor_utc].copy()
    if closed.empty:
        return None, pd.DataFrame()
    latest_ts = pd.Timestamp(closed["date"].iloc[-1])
    return latest_ts, closed

# Push-buffer caps. These prevent RAM drift during explosive market days.
SMCI_TAPE_BUFFER_MAXLEN = 25000
PEER_TAPE_BUFFER_MAXLEN = 8000
PUSH_TAPE_DEDUP_MAXLEN = 40000
ORDERBOOK_PUSH_BUFFER_MAXLEN = 6000
QUOTE_PUSH_BUFFER_MAXLEN = 6000
KLINE_PUSH_BUFFER_MAXLEN = 6000


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def today_str() -> str:
    return now_ny().strftime("%Y-%m-%d")


def ts_iso() -> str:
    return now_ny().isoformat()


def parse_any_ts_ny(x: Any) -> Optional[datetime]:
    """Parse Moomoo/IBKR/string timestamps into NY timezone.

    Used only for alignment diagnostics so we do not accidentally train on
    mismatched rows later. If parsing fails, return None rather than guessing.
    """
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        py = dt.to_pydatetime()
        if py.tzinfo is None:
            # Moomoo US bars are normally exchange-local strings.
            return NY_TZ.localize(py)
        return py.astimezone(NY_TZ)
    except Exception:
        return None


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def first_positive(*values: Any) -> float:
    """Return the first finite positive numeric value, else 0.0.

    Used for MARKET-DATA selection only. A zero quote is treated as unavailable.
    """
    for value in values:
        v = safe_float(value, 0.0)
        if v > 0:
            return v
    return 0.0


IBKR_TERMINAL_STATES = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
IBKR_WORKING_STATES = {"PendingSubmit", "PreSubmitted", "Submitted", "PendingCancel", "ApiPending"}


def _trade_status(trade) -> str:
    return str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")


def _trade_filled_from_execs(trade) -> int:
    """Execution-derived filled quantity; more robust than transient orderStatus fields."""
    total = 0.0
    try:
        for fill in list(getattr(trade, "fills", []) or []):
            ex = getattr(fill, "execution", None)
            if ex is not None:
                total += safe_float(getattr(ex, "shares", 0.0), 0.0)
    except Exception:
        pass
    return int(round(total))


def _trade_avg_fill_from_execs(trade) -> float:
    qty = 0.0
    notional = 0.0
    try:
        for fill in list(getattr(trade, "fills", []) or []):
            ex = getattr(fill, "execution", None)
            if ex is None:
                continue
            sh = safe_float(getattr(ex, "shares", 0.0), 0.0)
            px = safe_float(getattr(ex, "price", 0.0), 0.0)
            if sh > 0 and px > 0:
                qty += sh
                notional += sh * px
    except Exception:
        pass
    return (notional / qty) if qty > 0 else 0.0


_CSV_FIXED_SCHEMA_VALIDATED: set[Tuple[str, Tuple[str, ...]]] = set()

def append_csv(path: Path, row: Dict[str, Any], fieldnames: Optional[List[str]] = None) -> None:
    """Append one CSV row.

    When ``fieldnames`` is supplied, that list is the immutable CSV schema contract:
    missing row keys are written as blank cells, extra keys are ignored by DictWriter,
    and an existing file must already have the exact same header/order.  This prevents
    a temporarily missing optional feature from shrinking a row and shifting every
    later value into the wrong column.

    Calls that omit ``fieldnames`` retain the legacy dynamic behavior for files whose
    schemas have not been frozen.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fixed_schema = fieldnames is not None
    if fieldnames is None:
        fieldnames = list(row.keys())

    fieldnames = list(fieldnames)
    exists_with_data = path.exists() and path.stat().st_size > 0

    if fixed_schema and exists_with_data:
        cache_key = (str(path.resolve()), tuple(fieldnames))
        if cache_key not in _CSV_FIXED_SCHEMA_VALIDATED:
            with open(path, "r", newline="", encoding="utf-8-sig", errors="replace") as rf:
                existing_header = next(csv.reader(rf), [])
            if existing_header != fieldnames:
                raise RuntimeError(
                    f"CSV_SCHEMA_MISMATCH path={path} expected_cols={len(fieldnames)} "
                    f"actual_cols={len(existing_header)}. Refusing to append because doing so "
                    f"would corrupt column alignment. Start a new/clean daily file."
                )
            _CSV_FIXED_SCHEMA_VALIDATED.add(cache_key)

    clean = {k: row.get(k, "") for k in fieldnames}
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists_with_data:
            w.writeheader()
        w.writerow(clean)


def log(path: Path, msg: str) -> None:
    line = f"[{ts_iso()}] {msg}"
    print(line, flush=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def moomoo_code(symbol: str, prefix: str = "US") -> str:
    sym = str(symbol).upper().strip()
    if sym == "SQ":
        sym = "XYZ"
    if sym.startswith("US."):
        return sym
    return f"{prefix}.{sym}"



# ============================================================
# Optional async Moomoo push handlers
# ============================================================

class _BaseSafePushHandler:
    def _safe_call(self, owner, method_name: str, content: Any) -> None:
        try:
            getattr(owner, method_name)(content)
        except Exception as e:
            try:
                log(owner.log_file, f"WARN: push handler {method_name} failed: {e}")
            except Exception:
                pass


class AsyncTickerPushHandler(_BaseSafePushHandler, TickerHandlerBase if TickerHandlerBase is not None else object):
    def __init__(self, owner):
        if TickerHandlerBase is not None:
            super().__init__()
        self.owner = owner

    def on_recv_rsp(self, rsp_pb):
        if TickerHandlerBase is None:
            return 0, None
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code == RET_OK and content is not None:
            self._safe_call(self.owner, "ingest_ticker_push", content)
        return ret_code, content


class AsyncOrderBookPushHandler(_BaseSafePushHandler, OrderBookHandlerBase if OrderBookHandlerBase is not None else object):
    def __init__(self, owner):
        if OrderBookHandlerBase is not None:
            super().__init__()
        self.owner = owner

    def on_recv_rsp(self, rsp_pb):
        if OrderBookHandlerBase is None:
            return 0, None
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code == RET_OK and content is not None:
            self._safe_call(self.owner, "ingest_orderbook_push", content)
        return ret_code, content


class AsyncQuotePushHandler(_BaseSafePushHandler, StockQuoteHandlerBase if StockQuoteHandlerBase is not None else object):
    def __init__(self, owner):
        if StockQuoteHandlerBase is not None:
            super().__init__()
        self.owner = owner

    def on_recv_rsp(self, rsp_pb):
        if StockQuoteHandlerBase is None:
            return 0, None
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code == RET_OK and content is not None:
            self._safe_call(self.owner, "ingest_quote_push", content)
        return ret_code, content


class AsyncKlinePushHandler(_BaseSafePushHandler, CurKlineHandlerBase if CurKlineHandlerBase is not None else object):
    def __init__(self, owner):
        if CurKlineHandlerBase is not None:
            super().__init__()
        self.owner = owner

    def on_recv_rsp(self, rsp_pb):
        if CurKlineHandlerBase is None:
            return 0, None
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code == RET_OK and content is not None:
            self._safe_call(self.owner, "ingest_kline_push", content)
        return ret_code, content


def first_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    low = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    for c in df.columns:
        lc = str(c).lower()
        if any(n.lower() in lc for n in names):
            return c
    return None


def percentile_rank(series: pd.Series, value: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 5:
        return 50.0
    return float((clean <= value).mean() * 100.0)


class FeatureEngine:
    """Bar-derived features copied/extended from your v6 feature files."""

    def prepare_moomoo_bars(self, data: Any) -> pd.DataFrame:
        if data is None:
            return pd.DataFrame()
        df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            return pd.DataFrame()
        df.columns = [str(c).lower() for c in df.columns]
        time_col = first_col(df, ["time_key", "time", "date", "datetime"])
        open_col = first_col(df, ["open"])
        high_col = first_col(df, ["high"])
        low_col = first_col(df, ["low"])
        close_col = first_col(df, ["close", "last_price"])
        vol_col = first_col(df, ["volume", "vol"])
        turnover_col = first_col(df, ["turnover", "amount"])
        if not all([time_col, open_col, high_col, low_col, close_col]):
            return pd.DataFrame()
        out = pd.DataFrame()
        dt = pd.to_datetime(df[time_col], errors="coerce")
        # Moomoo US bars are usually NY local timestamps if timezone-naive.
        try:
            if dt.dt.tz is None:
                dt = dt.dt.tz_localize(NY_TZ, ambiguous="NaT", nonexistent="shift_forward").dt.tz_convert("UTC")
            else:
                dt = dt.dt.tz_convert("UTC")
        except Exception:
            # Conservative fallback: localize each timestamp if needed.
            dt = pd.to_datetime(df[time_col], errors="coerce", utc=True)
        out["date"] = dt
        out["open"] = pd.to_numeric(df[open_col], errors="coerce")
        out["high"] = pd.to_numeric(df[high_col], errors="coerce")
        out["low"] = pd.to_numeric(df[low_col], errors="coerce")
        out["close"] = pd.to_numeric(df[close_col], errors="coerce")
        out["volume"] = pd.to_numeric(df[vol_col], errors="coerce") if vol_col else 0.0
        out["turnover"] = pd.to_numeric(df[turnover_col], errors="coerce") if turnover_col else 0.0
        out = out.dropna(subset=["date", "open", "high", "low", "close"])
        out = out[out["close"] > 0].sort_values("date").drop_duplicates("date", keep="last")
        return out

    def add_features(self, raw: pd.DataFrame) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame()
        df = raw.copy().sort_values("date").drop_duplicates("date", keep="last")
        df = df.set_index("date") if "date" in df.columns else df
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        df["ny_dt"] = df.index.tz_convert(NY_TZ)
        df["date_ny"] = df["ny_dt"].dt.strftime("%Y-%m-%d")
        df["time_ny"] = df["ny_dt"].dt.strftime("%H:%M:%S")
        df["is_rth"] = df["ny_dt"].dt.time.apply(lambda t: RTH_START <= t < RTH_END)
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["range"] = (df["high"] - df["low"]).clip(lower=0)
        df["body"] = (df["close"] - df["open"]).abs()
        rng = df["range"].replace(0, np.nan)
        df["close_location"] = ((df["close"] - df["low"]) / rng).clip(0, 1).fillna(0.5)
        df["ret"] = df["close"].pct_change().fillna(0.0)
        df["typical"] = (df["high"] + df["low"] + df["close"]) / 3.0
        df["pv"] = df["typical"] * df["volume"]
        df["cum_pv"] = df.groupby("date_ny")["pv"].cumsum()
        df["cum_vol"] = df.groupby("date_ny")["volume"].cumsum().replace(0, np.nan)
        df["vwap"] = (df["cum_pv"] / df["cum_vol"]).ffill()
        for span in [8, 21, 50, 100]:
            df[f"ema_{span}"] = df["close"].ewm(span=span, adjust=False).mean()
        df["day_high"] = df.groupby("date_ny")["high"].cummax()
        df["day_low"] = df.groupby("date_ny")["low"].cummin()
        df["day_range"] = (df["day_high"] - df["day_low"]).replace(0, np.nan)
        df["day_position"] = ((df["close"] - df["day_low"]) / df["day_range"]).clip(0, 1).fillna(0.5)
        df["dist_vwap_pct"] = (df["close"] / df["vwap"].replace(0, np.nan) - 1.0) * 100.0
        for w in [3, 5, 10, 20, 30, 60, 90]:
            df[f"vol_ma_{w}"] = df["volume"].rolling(w, min_periods=max(2, w // 4)).mean()
            df[f"range_ma_{w}"] = df["range"].rolling(w, min_periods=max(2, w // 4)).mean()
            df[f"body_ma_{w}"] = df["body"].rolling(w, min_periods=max(2, w // 4)).mean()
            df[f"close_slope_{w}"] = df["close"].pct_change(w) * 100.0
        df["rel_vol_30"] = df["volume"] / df["vol_ma_30"].replace(0, np.nan)
        df["rel_range_30"] = df["range"] / df["range_ma_30"].replace(0, np.nan)
        df["rel_body_30"] = df["body"] / df["body_ma_30"].replace(0, np.nan)
        df["green"] = (df["close"] > df["open"]).astype(int)
        df["red"] = (df["close"] < df["open"]).astype(int)
        df["above_vwap"] = (df["close"] > df["vwap"]).astype(int)
        df["below_vwap"] = (df["close"] < df["vwap"]).astype(int)
        df["bull_stack"] = ((df["ema_8"] > df["ema_21"]) & (df["ema_21"] > df["ema_50"])).astype(int)
        df["bear_stack"] = ((df["ema_8"] < df["ema_21"]) & (df["ema_21"] < df["ema_50"])).astype(int)
        rv = df["rel_vol_30"].clip(0, 3).fillna(1) / 3.0
        rb = df["rel_body_30"].clip(0, 3).fillna(1) / 3.0
        df["buyer_pressure_raw"] = (df["close_location"] * 0.40 + df["green"] * 0.20 + df["above_vwap"] * 0.12 + rv * 0.15 + rb * 0.13) * 100.0
        df["seller_pressure_raw"] = ((1 - df["close_location"]) * 0.40 + df["red"] * 0.20 + df["below_vwap"] * 0.12 + rv * 0.15 + rb * 0.13) * 100.0
        df["buyer_pressure"] = df["buyer_pressure_raw"].rolling(5, min_periods=2).mean()
        df["seller_pressure"] = df["seller_pressure_raw"].rolling(5, min_periods=2).mean()
        df["control_delta"] = df["buyer_pressure"] - df["seller_pressure"]
        df["green_ratio_10"] = df["green"].rolling(10, min_periods=3).mean()
        df["red_ratio_10"] = df["red"].rolling(10, min_periods=3).mean()
        df["above_vwap_ratio_20"] = df["above_vwap"].rolling(20, min_periods=5).mean()
        df["bull_stack_ratio_20"] = df["bull_stack"].rolling(20, min_periods=5).mean()
        df["bear_stack_ratio_20"] = df["bear_stack"].rolling(20, min_periods=5).mean()
        df["normal_range"] = df["range"].rolling(30, min_periods=5).median()
        df = df.replace([np.inf, -np.inf], np.nan)
        return df

    def live_features(self, featured: pd.DataFrame) -> Dict[str, Any]:
        if featured.empty:
            return {}
        # Prefer current RTH day rows for day-position percentiles. If not enough, use all rows.
        day = featured[featured["date_ny"] == today_str()].copy()
        if day.empty:
            day = featured.copy()
        row = day.iloc[-1]
        recent = day.tail(30)
        long_recent = day.tail(90)
        f: Dict[str, Any] = {
            "bar_time_utc": str(featured.index[-1]),
            "bar_time_ny": str(row.get("ny_dt", "")),
            "close": safe_float(row.get("close")),
            "open": safe_float(row.get("open")),
            "high": safe_float(row.get("high")),
            "low": safe_float(row.get("low")),
            "volume": safe_float(row.get("volume")),
            "turnover": safe_float(row.get("turnover")),
            "vwap": safe_float(row.get("vwap")),
            "ema_8": safe_float(row.get("ema_8")),
            "ema_21": safe_float(row.get("ema_21")),
            "ema_50": safe_float(row.get("ema_50")),
            "ema_100": safe_float(row.get("ema_100")),
            "range": safe_float(row.get("range")),
            "body": safe_float(row.get("body")),
            "close_location": safe_float(row.get("close_location")) * 100.0,
            "day_position": safe_float(row.get("day_position")) * 100.0,
            "dist_vwap_pct": safe_float(row.get("dist_vwap_pct")),
            "control_delta": safe_float(row.get("control_delta")),
            "buyer_pressure": safe_float(row.get("buyer_pressure"), 50.0),
            "seller_pressure": safe_float(row.get("seller_pressure"), 50.0),
            "rel_vol_30": safe_float(row.get("rel_vol_30"), 1.0),
            "rel_range_30": safe_float(row.get("rel_range_30"), 1.0),
            "rel_body_30": safe_float(row.get("rel_body_30"), 1.0),
            "green_ratio_10": safe_float(row.get("green_ratio_10"), 0.5) * 100.0,
            "red_ratio_10": safe_float(row.get("red_ratio_10"), 0.5) * 100.0,
            "above_vwap_ratio_20": safe_float(row.get("above_vwap_ratio_20"), 0.5) * 100.0,
            "bull_stack_ratio_20": safe_float(row.get("bull_stack_ratio_20"), 0.0) * 100.0,
            "bear_stack_ratio_20": safe_float(row.get("bear_stack_ratio_20"), 0.0) * 100.0,
            "slope_3": safe_float(row.get("close_slope_3")),
            "slope_5": safe_float(row.get("close_slope_5")),
            "slope_10": safe_float(row.get("close_slope_10")),
            "slope_20": safe_float(row.get("close_slope_20")),
            "slope_60": safe_float(row.get("close_slope_60")),
            "normal_range": safe_float(row.get("normal_range"), safe_float(recent["range"].median() if "range" in recent else 0.0)),
        }
        f["vol_pctile_today"] = percentile_rank(day["volume"], safe_float(row.get("volume"))) if "volume" in day else 50.0
        f["range_pctile_today"] = percentile_rank(day["range"], safe_float(row.get("range"))) if "range" in day else 50.0
        f["body_pctile_today"] = percentile_rank(day["body"], safe_float(row.get("body"))) if "body" in day else 50.0
        day_open = safe_float(day.iloc[0].get("open"), f["close"])
        day_high = safe_float(day["high"].max(), f["close"])
        day_low = safe_float(day["low"].min(), f["close"])
        f["drop_from_open_pct"] = (f["close"] / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        f["drop_from_high_pct"] = (f["close"] / day_high - 1.0) * 100.0 if day_high > 0 else 0.0
        f["bounce_from_low_pct"] = (f["close"] / day_low - 1.0) * 100.0 if day_low > 0 else 0.0
        f["low_age_bars"] = float(len(day) - 1 - int(np.argmin(day["low"].values))) if len(day) > 0 and "low" in day else 0.0
        if len(day) >= 12:
            f["higher_low_proxy"] = 100.0 if safe_float(day["low"].tail(5).min()) > safe_float(day["low"].tail(12).min()) else 0.0
            f["seller_pressure_change_5"] = safe_float(row.get("seller_pressure"), 50.0) - safe_float(day["seller_pressure"].iloc[-6], 50.0)
            f["buyer_pressure_change_5"] = safe_float(row.get("buyer_pressure"), 50.0) - safe_float(day["buyer_pressure"].iloc[-6], 50.0)
        else:
            f["higher_low_proxy"] = 0.0
            f["seller_pressure_change_5"] = 0.0
            f["buyer_pressure_change_5"] = 0.0
        f["ema8_reclaimed"] = 100.0 if f["close"] >= f["ema_8"] else 0.0
        f["ema8_hold_bars_2"] = float((day.tail(2)["close"] >= day.tail(2)["ema_8"]).sum()) if len(day) >= 2 else 0.0
        f["vwap_hold_bars_2"] = float((day.tail(2)["close"] >= day.tail(2)["vwap"]).sum()) if len(day) >= 2 else 0.0
        f["two_bar_reclaim_hold"] = 100.0 if f["ema8_hold_bars_2"] >= 2 and f["vwap_hold_bars_2"] >= 1 else 0.0
        directional_sum = safe_float(long_recent["close"].pct_change().sum()) if len(long_recent) else 0.0
        path_sum = safe_float(long_recent["ret"].abs().sum(), 1e-9) if "ret" in long_recent else 1e-9
        f["trend_cleanliness"] = clamp(abs(directional_sum) / max(path_sum, 1e-9) * 100.0)
        f["noise_score"] = clamp(100.0 - f["trend_cleanliness"])
        # Current schema aliases.
        f["tf5_control"] = f.get("control_delta", 0.0)
        f["tf10_control"] = f.get("control_delta", 0.0)
        f["tf15_control"] = f.get("control_delta", 0.0)
        return f


class MoomooRecorder:
    def __init__(self, symbol: str, host: str, port: int, prefix: str, l2_levels: int, log_file: Path, peer_symbols: Optional[List[str]] = None):
        self.symbol = symbol.upper().strip()
        self.code = moomoo_code(symbol, prefix)
        self.prefix = prefix
        self.peer_symbols = [p.upper().strip() for p in (peer_symbols or []) if p and p.upper().strip() != self.symbol]
        self.host = host
        self.port = port
        self.l2_levels = int(l2_levels)
        self.log_file = log_file
        self.ctx = None
        self.connected = False
        self.l2_imbalance_hist = deque(maxlen=8)
        self.tape_delta_hist = deque(maxlen=8)
        self.last_cf: Dict[str, Any] = {}
        self.last_cf_ts = 0.0
        self.push_enabled = False
        self.push_handlers: List[Any] = []
        self.tape_lock = threading.Lock()
        self.tape_buffer = deque(maxlen=SMCI_TAPE_BUFFER_MAXLEN)
        self.peer_tape_buffers: Dict[str, deque] = {p: deque(maxlen=PEER_TAPE_BUFFER_MAXLEN) for p in self.peer_symbols}
        self.tape_seen_keys = set()
        self.tape_seen_order = deque()  # explicit eviction keeps companion set bounded
        self.tape_seq = 0
        self.orderbook_lock = threading.Lock()
        self.orderbook_buffer = deque(maxlen=ORDERBOOK_PUSH_BUFFER_MAXLEN)
        self.quote_lock = threading.Lock()
        self.quote_buffer = deque(maxlen=QUOTE_PUSH_BUFFER_MAXLEN)
        self.kline_lock = threading.Lock()
        self.kline_buffer = deque(maxlen=KLINE_PUSH_BUFFER_MAXLEN)
        self.peer_cache: Dict[str, Any] = {"ts": 0.0, "features": {}}
        self.last_smci_tape_push_wall = 0.0

    def connect(self) -> bool:
        if OpenQuoteContext is None:
            log(self.log_file, "Moomoo package not available.")
            return False
        try:
            self.ctx = OpenQuoteContext(host=self.host, port=self.port)
            self.connected = True
            self._register_push_handlers()
            subtypes = ["QUOTE", "K_1M", "ORDER_BOOK", "TICKER"]
            subscribe_codes = [self.code] + [moomoo_code(p, self.prefix) for p in self.peer_symbols]
            # preserve order and avoid duplicate subscription codes
            subscribe_codes = list(dict.fromkeys(subscribe_codes))
            try:
                ret, data = self.ctx.subscribe(subscribe_codes, subtypes,
                                               is_first_push=True, subscribe_push=True,
                                               is_detailed_orderbook=True, extended_time=True)
                log(self.log_file, f"Moomoo subscribe ret={ret} data={data}")
            except TypeError:
                ret, data = self.ctx.subscribe(subscribe_codes, subtypes)
                log(self.log_file, f"Moomoo subscribe basic ret={ret} data={data}")
            if ret != RET_OK:
                raise RuntimeError(f"Moomoo subscribe failed: {data}")
            return True
        except Exception as e:
            self.connected = False
            log(self.log_file, f"Moomoo connect failed: {e}")
            return False


    def _register_push_handlers(self) -> None:
        """Register push handlers when the installed moomoo/futu SDK supports them.

        If any handler is unavailable the recorder continues with polling. This keeps
        the file safe across different OpenD/SDK versions.
        """
        if self.ctx is None or not hasattr(self.ctx, "set_handler"):
            log(self.log_file, "Async push not available: quote context has no set_handler; using polling fallback.")
            return
        specs = [
            (TickerHandlerBase, AsyncTickerPushHandler, "TICKER"),
            (OrderBookHandlerBase, AsyncOrderBookPushHandler, "ORDER_BOOK"),
            (StockQuoteHandlerBase, AsyncQuotePushHandler, "QUOTE"),
            (CurKlineHandlerBase, AsyncKlinePushHandler, "K_1M"),
        ]
        ok = 0
        for base, cls, name in specs:
            if base is None:
                log(self.log_file, f"Async {name} handler class not available in this SDK.")
                continue
            try:
                h = cls(self)
                self.ctx.set_handler(h)
                self.push_handlers.append(h)
                ok += 1
                log(self.log_file, f"Async Moomoo {name} push handler registered.")
            except Exception as e:
                log(self.log_file, f"WARN: could not register async {name} handler: {e}")
        self.push_enabled = ok > 0

    def _content_to_df(self, content: Any) -> pd.DataFrame:
        try:
            if content is None:
                return pd.DataFrame()
            if isinstance(content, pd.DataFrame):
                return content.copy()
            if isinstance(content, dict):
                return pd.DataFrame([content])
            return pd.DataFrame(content)
        except Exception:
            return pd.DataFrame()

    def ingest_ticker_push(self, content: Any) -> None:
        df = self._content_to_df(content)
        if df.empty:
            return
        df.columns = [str(c).lower() for c in df.columns]
        code_col = first_col(df, ["code", "stock_code", "security", "symbol"])
        price_col = first_col(df, ["price", "ticker_price", "last_price", "cur_price", "last"])
        vol_col = first_col(df, ["volume", "vol", "ticker_volume", "qty", "size"])
        dir_col = first_col(df, ["ticker_direction", "direction", "dir", "type", "side"])
        time_col = first_col(df, ["time", "time_key", "datetime", "turnover_time", "date"])
        if price_col is None:
            return
        now = now_ny()
        with self.tape_lock:
            for _, row in df.iterrows():
                raw_code = str(row.get(code_col, self.code)) if code_col else self.code
                sym = raw_code.split(".")[-1].upper()
                if sym != self.symbol and sym not in self.peer_tape_buffers:
                    continue
                px = safe_float(row.get(price_col), np.nan)
                if np.isnan(px) or px <= 0:
                    continue
                vol = max(safe_float(row.get(vol_col), 1.0) if vol_col else 1.0, 0.0)
                raw_dir = str(row.get(dir_col, "")) if dir_col else ""
                raw_time = str(row.get(time_col, "")) if time_col else ""
                key = (sym, raw_time, round(px, 6), round(vol, 4), raw_dir)
                if key in self.tape_seen_keys:
                    continue
                self.tape_seen_keys.add(key)
                self.tape_seen_order.append(key)
                while len(self.tape_seen_order) > PUSH_TAPE_DEDUP_MAXLEN:
                    self.tape_seen_keys.discard(self.tape_seen_order.popleft())
                self.tape_seq += 1
                dollar_value = px * vol
                rec = {
                    "ts": now.isoformat(), "symbol": sym, "seq": self.tape_seq, "trade_time": raw_time,
                    "price": px, "volume": vol, "dollar_value": dollar_value, "direction": raw_dir,
                    "derived_side": "unknown", "source": "MOOMOO_PUSH",
                    "is_block_250k": 1 if dollar_value >= 250000 else 0,
                    "is_block_5000sh": 1 if vol >= 5000 else 0,
                    "raw": json.dumps({str(c): row[c] for c in df.columns}, default=str)[:2000],
                }
                if sym == self.symbol:
                    self.last_smci_tape_push_wall = time.time()
                    self.tape_buffer.append(rec)
                else:
                    self.peer_tape_buffers.setdefault(sym, deque(maxlen=PEER_TAPE_BUFFER_MAXLEN)).append(rec)

    def ingest_orderbook_push(self, content: Any) -> None:
        # Moomoo order-book push content shape varies by SDK. Store raw content so no update is lost.
        now = ts_iso()
        with self.orderbook_lock:
            self.orderbook_buffer.append({
                "ts": now, "symbol": self.symbol, "source": "MOOMOO_ORDERBOOK_PUSH",
                "raw": json.dumps(content, default=str)[:8000],
            })

    def ingest_quote_push(self, content: Any) -> None:
        df = self._content_to_df(content)
        if df.empty:
            return
        now = ts_iso()
        with self.quote_lock:
            for _, row in df.iterrows():
                self.quote_buffer.append({
                    "ts": now, "symbol": self.symbol, "source": "MOOMOO_QUOTE_PUSH",
                    "raw": json.dumps({str(c): row[c] for c in df.columns}, default=str)[:4000],
                })

    def ingest_kline_push(self, content: Any) -> None:
        df = self._content_to_df(content)
        if df.empty:
            return
        now = ts_iso()
        with self.kline_lock:
            for _, row in df.iterrows():
                self.kline_buffer.append({
                    "ts": now, "symbol": self.symbol, "source": "MOOMOO_KLINE_PUSH",
                    "raw": json.dumps({str(c): row[c] for c in df.columns}, default=str)[:4000],
                })

    def drain_push_rows(self) -> Dict[str, List[Dict[str, Any]]]:
        """Atomically swap push buffers, then process/write outside locks.

        This avoids holding a lock while copying thousands of rows to CSV during
        the open. Push handlers only pause for the pointer swap, not disk I/O.
        """
        out = {"tape": [], "orderbook": [], "quote": [], "kline": []}
        with self.tape_lock:
            old_tape = self.tape_buffer
            self.tape_buffer = deque(maxlen=SMCI_TAPE_BUFFER_MAXLEN)
        out["tape"] = [dict(x) for x in old_tape]

        with self.orderbook_lock:
            old_orderbook = self.orderbook_buffer
            self.orderbook_buffer = deque(maxlen=ORDERBOOK_PUSH_BUFFER_MAXLEN)
        out["orderbook"] = [dict(x) for x in old_orderbook]

        with self.quote_lock:
            old_quote = self.quote_buffer
            self.quote_buffer = deque(maxlen=QUOTE_PUSH_BUFFER_MAXLEN)
        out["quote"] = [dict(x) for x in old_quote]

        with self.kline_lock:
            old_kline = self.kline_buffer
            self.kline_buffer = deque(maxlen=KLINE_PUSH_BUFFER_MAXLEN)
        out["kline"] = [dict(x) for x in old_kline]
        return out

    def get_push_buffer_health(self) -> Dict[str, Any]:
        """Lightweight diagnostics to verify buffers are bounded and not saturating."""
        with self.tape_lock:
            smci_len = len(self.tape_buffer)
            peer_lens = {p: len(b) for p, b in self.peer_tape_buffers.items()}
            dedup_len = len(self.tape_seen_keys)
        with self.orderbook_lock:
            orderbook_len = len(self.orderbook_buffer)
        with self.quote_lock:
            quote_len = len(self.quote_buffer)
        with self.kline_lock:
            kline_len = len(self.kline_buffer)
        out = {
            "push_smci_tape_buffer_len": float(smci_len),
            "push_smci_tape_buffer_maxlen": float(SMCI_TAPE_BUFFER_MAXLEN),
            "push_tape_dedup_len": float(dedup_len),
            "push_tape_dedup_maxlen": float(PUSH_TAPE_DEDUP_MAXLEN),
            "push_orderbook_buffer_len": float(orderbook_len),
            "push_quote_buffer_len": float(quote_len),
            "push_kline_buffer_len": float(kline_len),
        }
        for p, n in peer_lens.items():
            out[f"push_peer_{p.lower()}_tape_buffer_len"] = float(n)
            out[f"push_peer_{p.lower()}_tape_buffer_maxlen"] = float(PEER_TAPE_BUFFER_MAXLEN)
        return out

    def close(self):
        try:
            if self.ctx is not None:
                self.ctx.close()
        except Exception:
            pass

    def get_live_1m(self, count: int = 420) -> pd.DataFrame:
        if not self.connected:
            return pd.DataFrame()
        try:
            ret, data = self.ctx.get_cur_kline(self.code, count, ktype="K_1M", autype="qfq")
            if ret == RET_OK and data is not None and not data.empty:
                return data
        except Exception as e:
            log(self.log_file, f"Moomoo get_cur_kline failed: {e}")
        return pd.DataFrame()

    def get_snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"moomoo_snapshot_available": 0.0}
        if not self.connected:
            return out
        try:
            ret, data = self.ctx.get_market_snapshot([self.code])
            if ret == RET_OK and data is not None and not data.empty:
                row = data.iloc[0]
                out = {"moomoo_snapshot_available": 1.0}
                for c in data.columns:
                    val = row[c]
                    if isinstance(val, (np.generic,)):
                        val = val.item()
                    out[f"snapshot_{str(c).lower()}"] = val
                return out
        except Exception as e:
            log(self.log_file, f"Moomoo snapshot failed: {e}")
        return out

    def get_l2(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        empty = {"moomoo_l2_available": 0.0, "moomoo_l2_score": 50.0, "moomoo_l2_imbalance": 0.0,
                 "moomoo_l2_persistence": 50.0, "moomoo_bid_levels": 0.0, "moomoo_ask_levels": 0.0}
        raw_rows: List[Dict[str, Any]] = []
        if not self.connected:
            return empty, raw_rows
        try:
            ret, data = self.ctx.get_order_book(self.code, num=self.l2_levels)
            if ret != RET_OK or not isinstance(data, dict):
                raise RuntimeError(str(data))
            bids = data.get("Bid") or data.get("bid") or data.get("bids") or []
            asks = data.get("Ask") or data.get("ask") or data.get("asks") or []

            def parse_level(level: Any, idx: int, side: str) -> Dict[str, Any]:
                price = volume = order_num = 0.0
                raw = json.dumps(level, default=str)[:1000]
                if isinstance(level, dict):
                    price = safe_float(level.get("price", level.get("bid_price", level.get("ask_price", 0))))
                    for k in ["volume", "vol", "qty", "size"]:
                        if k in level:
                            volume = safe_float(level[k]); break
                    order_num = safe_float(level.get("order_num", level.get("orders", 0)))
                elif isinstance(level, (list, tuple)):
                    price = safe_float(level[0] if len(level) > 0 else 0.0)
                    volume = safe_float(level[1] if len(level) > 1 else 0.0)
                    order_num = safe_float(level[2] if len(level) > 2 else 0.0)
                return {"ts": ts_iso(), "symbol": self.symbol, "side": side, "level": idx + 1,
                        "price": price, "volume": volume, "order_num": order_num, "raw": raw}

            for i, lvl in enumerate(bids[:self.l2_levels]):
                raw_rows.append(parse_level(lvl, i, "bid"))
            for i, lvl in enumerate(asks[:self.l2_levels]):
                raw_rows.append(parse_level(lvl, i, "ask"))

            # Exponential decay gives far more weight to immediate inside liquidity.
            # Level 1 weight=1.00, L2≈0.67, L3≈0.45, L5≈0.20, L10≈0.027.
            decay_lambda = 0.40
            bid_vol = sum(r["volume"] * math.exp(-decay_lambda * (r["level"] - 1)) for r in raw_rows if r["side"] == "bid")
            ask_vol = sum(r["volume"] * math.exp(-decay_lambda * (r["level"] - 1)) for r in raw_rows if r["side"] == "ask")
            raw_bid_vol = sum(r["volume"] for r in raw_rows if r["side"] == "bid")
            raw_ask_vol = sum(r["volume"] for r in raw_rows if r["side"] == "ask")
            bid_top3 = sum(r["volume"] for r in raw_rows if r["side"] == "bid" and r["level"] <= 3)
            ask_top3 = sum(r["volume"] for r in raw_rows if r["side"] == "ask" and r["level"] <= 3)
            bid_top10 = sum(r["volume"] for r in raw_rows if r["side"] == "bid" and r["level"] <= 10)
            ask_top10 = sum(r["volume"] for r in raw_rows if r["side"] == "ask" and r["level"] <= 10)
            top3_total = bid_top3 + ask_top3
            top10_total = bid_top10 + ask_top10
            top3_imbalance = (bid_top3 - ask_top3) / top3_total if top3_total > 0 else 0.0
            top10_imbalance = (bid_top10 - ask_top10) / top10_total if top10_total > 0 else 0.0
            total = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0
            self.l2_imbalance_hist.append(imbalance)
            persistence = clamp(50.0 + safe_float(np.mean(self.l2_imbalance_hist), 0.0) * 60.0)
            out = {
                "moomoo_l2_available": 1.0,
                "moomoo_l2_score": round(clamp(50.0 + imbalance * 60.0), 3),
                "moomoo_l2_imbalance": round(imbalance, 5),
                "moomoo_l2_persistence": round(persistence, 3),
                "moomoo_l2_positive_count": float(sum(1 for x in self.l2_imbalance_hist if x > 0.05)),
                "moomoo_l2_negative_count": float(sum(1 for x in self.l2_imbalance_hist if x < -0.05)),
                "moomoo_bid_levels": float(len(bids)),
                "moomoo_ask_levels": float(len(asks)),
                "moomoo_bid_volume_50": float(raw_bid_vol),
                "moomoo_ask_volume_50": float(raw_ask_vol),
                "moomoo_bid_volume_50_weighted": float(bid_vol),
                "moomoo_ask_volume_50_weighted": float(ask_vol),
                "moomoo_l2_decay_lambda": float(decay_lambda),
                "moomoo_l2_exp_imbalance": round(imbalance, 5),
                "moomoo_bid_volume_top3": float(bid_top3),
                "moomoo_ask_volume_top3": float(ask_top3),
                "moomoo_l2_top3_imbalance": round(top3_imbalance, 5),
                "moomoo_bid_volume_top10": float(bid_top10),
                "moomoo_ask_volume_top10": float(ask_top10),
                "moomoo_l2_top10_imbalance": round(top10_imbalance, 5),
                "coiled_l2_imbalance_diff_2bar": round(imbalance - list(self.l2_imbalance_hist)[-2], 5) if len(self.l2_imbalance_hist) >= 2 else 0.0,
            }
            return out, raw_rows
        except Exception as e:
            log(self.log_file, f"Moomoo L2 failed: {e}")
            return empty, raw_rows


    def _classify_tape_df(self, df: pd.DataFrame, bid: float = np.nan, ask: float = np.nan) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        px = pd.to_numeric(df.get("price"), errors="coerce").ffill().fillna(0.0)
        vol = pd.to_numeric(df.get("volume"), errors="coerce").fillna(1.0)
        direction_text = df.get("direction", pd.Series("", index=df.index)).astype(str).str.lower()
        price_diff = px.diff().fillna(0.0)
        buy_mask = direction_text.str.contains("buy|up|ask", regex=True) | (price_diff > 0)
        sell_mask = direction_text.str.contains("sell|down|bid", regex=True) | (price_diff < 0)
        return px, vol, buy_mask, sell_mask

    def _tape_metrics_from_buffer(self, max_age_sec: float = 180, max_rows: int = 500) -> Optional[Dict[str, Any]]:
        cutoff = now_ny() - timedelta(seconds=max_age_sec)
        with self.tape_lock:
            rows = [dict(r) for r in self.tape_buffer if pd.to_datetime(r.get("ts"), errors="coerce") is not pd.NaT]
        recent = []
        for r in rows:
            try:
                dt = pd.to_datetime(r.get("ts"), errors="coerce")
                if dt is pd.NaT:
                    continue
                if dt.tzinfo is None:
                    dt = NY_TZ.localize(dt.to_pydatetime())
                else:
                    dt = dt.to_pydatetime().astimezone(NY_TZ)
                if dt >= cutoff:
                    recent.append(r)
            except Exception:
                continue
        if len(recent) < 20:
            return None
        df = pd.DataFrame(recent).tail(max_rows).copy()
        if df.empty or "price" not in df.columns:
            return None
        px, vol, buy_mask, sell_mask = self._classify_tape_df(df)
        buy_vol = float(vol[buy_mask].sum())
        sell_vol = float(vol[sell_mask].sum())
        dollar_value = px * vol
        block_mask = (dollar_value >= 250000) | (vol >= 5000)
        block_buy_vol = float(vol[buy_mask & block_mask].sum())
        block_sell_vol = float(vol[sell_mask & block_mask].sum())
        block_delta = block_buy_vol - block_sell_vol
        block_count = int(block_mask.sum())
        total = buy_vol + sell_vol
        delta = buy_vol - sell_vol
        delta_ratio = delta / total if total > 0 else 0.0
        tape_score = clamp(50.0 + delta_ratio * 50.0)
        recent30 = min(30, len(df))
        buy_burst = clamp((vol[buy_mask].tail(recent30).sum() / max(vol.tail(recent30).sum(), 1e-9)) * 100.0) if len(vol) else 50.0
        sell_burst = clamp((vol[sell_mask].tail(recent30).sum() / max(vol.tail(recent30).sum(), 1e-9)) * 100.0) if len(vol) else 50.0
        self.tape_delta_hist.append(delta_ratio)
        delta_change = delta_ratio - list(self.tape_delta_hist)[-2] if len(self.tape_delta_hist) >= 2 else 0.0
        velocity = float(len(df) / max(max_age_sec, 1.0))
        buyer_abs = clamp(50.0 + max(0.0, delta_change) * 80.0 + max(0.0, sell_burst - 50.0) * 0.15)
        seller_abs = clamp(50.0 + max(0.0, -delta_change) * 80.0 + max(0.0, buy_burst - 50.0) * 0.15)
        return {
            "moomoo_tape_available": 1.0, "moomoo_tape_source": "MOOMOO_PUSH_BUFFER",
            "moomoo_tape_tick_count": float(len(df)), "moomoo_tape_score": round(tape_score, 3),
            "moomoo_delta_score": round(tape_score, 3), "moomoo_buy_burst_score": round(buy_burst, 3),
            "moomoo_sell_burst_score": round(sell_burst, 3), "moomoo_buyer_absorption_score": round(buyer_abs, 3),
            "moomoo_seller_absorption_score": round(seller_abs, 3), "moomoo_aggr_buy_volume": round(buy_vol, 3),
            "moomoo_aggr_sell_volume": round(sell_vol, 3), "moomoo_tape_delta": round(delta, 3),
            "moomoo_tape_delta_ratio": round(delta_ratio, 5), "moomoo_tape_delta_change": round(delta_change, 5),
            "moomoo_tape_velocity": round(velocity, 3), "moomoo_block_trade_count": float(block_count),
            "moomoo_block_buy_volume": round(block_buy_vol, 3), "moomoo_block_sell_volume": round(block_sell_vol, 3),
            "moomoo_block_delta": round(block_delta, 3),
            "moomoo_block_delta_score": round(clamp(50.0 + (block_delta / max(block_buy_vol + block_sell_vol, 1.0)) * 50.0), 3),
            "moomoo_bear_reversal_confidence": round(clamp((tape_score * 0.45) + (buyer_abs * 0.35) + ((100.0 - sell_burst) * 0.20)), 3),
            "moomoo_bull_continuation_confidence": round(clamp((tape_score * 0.50) + (buy_burst * 0.30) + (buyer_abs * 0.20)), 3),
        }

    def get_tape(self, num: int = 300) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        empty = {"moomoo_tape_available": 0.0, "moomoo_tape_tick_count": 0.0, "moomoo_tape_score": 50.0,
                 "moomoo_delta_score": 50.0, "moomoo_buy_burst_score": 50.0, "moomoo_sell_burst_score": 50.0,
                 "moomoo_buyer_absorption_score": 50.0, "moomoo_seller_absorption_score": 50.0,
                 "moomoo_aggr_buy_volume": 0.0, "moomoo_aggr_sell_volume": 0.0, "moomoo_tape_delta": 0.0,
                 "moomoo_tape_delta_change": 0.0, "moomoo_tape_velocity": 0.0,
                 "moomoo_bear_reversal_confidence": 50.0, "moomoo_bull_continuation_confidence": 50.0}
        raw_rows: List[Dict[str, Any]] = []
        # Prefer async push buffer for metrics when enough recent rows exist.
        pushed = self._tape_metrics_from_buffer(max_age_sec=180, max_rows=max(num, 300))
        if pushed is not None:
            return pushed, raw_rows
        if not self.connected:
            return empty, raw_rows
        try:
            ret, data = self.ctx.get_rt_ticker(self.code, num=num)
            if ret != RET_OK or data is None:
                raise RuntimeError(str(data))
            df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            if df.empty:
                return empty, raw_rows
            df.columns = [str(c).lower() for c in df.columns]
            price_col = first_col(df, ["price", "last_price", "current_price", "last"])
            vol_col = first_col(df, ["volume", "vol", "qty", "size"])
            dir_col = first_col(df, ["ticker_direction", "direction", "type", "side", "trade_direction"])
            time_col = first_col(df, ["time", "time_key", "datetime", "date"])
            if price_col is None:
                return empty, raw_rows
            px = pd.to_numeric(df[price_col], errors="coerce").ffill().fillna(0.0)
            vol = pd.to_numeric(df[vol_col], errors="coerce").fillna(1.0) if vol_col else pd.Series(1.0, index=df.index)
            direction_text = df[dir_col].astype(str).str.lower() if dir_col else pd.Series("", index=df.index)
            price_diff = px.diff().fillna(0.0)
            buy_mask = direction_text.str.contains("buy|up|bidask|ask", regex=True) | (price_diff > 0)
            sell_mask = direction_text.str.contains("sell|down", regex=True) | (price_diff < 0)
            buy_vol = float(vol[buy_mask].sum())
            sell_vol = float(vol[sell_mask].sum())
            dollar_value = px * vol
            block_mask = (dollar_value >= 250000) | (vol >= 5000)
            block_buy_vol = float(vol[buy_mask & block_mask].sum())
            block_sell_vol = float(vol[sell_mask & block_mask].sum())
            block_delta = block_buy_vol - block_sell_vol
            block_count = int(block_mask.sum())
            total = buy_vol + sell_vol
            delta = buy_vol - sell_vol
            delta_ratio = delta / total if total > 0 else 0.0
            tape_score = clamp(50.0 + delta_ratio * 50.0)
            delta_score = tape_score
            buy_burst = clamp((vol[buy_mask].tail(30).sum() / max(vol.tail(30).sum(), 1e-9)) * 100.0) if len(vol) else 50.0
            sell_burst = clamp((vol[sell_mask].tail(30).sum() / max(vol.tail(30).sum(), 1e-9)) * 100.0) if len(vol) else 50.0
            self.tape_delta_hist.append(delta_ratio)
            delta_change = delta_ratio - list(self.tape_delta_hist)[-2] if len(self.tape_delta_hist) >= 2 else 0.0
            velocity = float(len(df) / max(1.0, 60.0))  # approximate ticks/sec if requesting recent window
            buyer_abs = clamp(50.0 + max(0.0, delta_change) * 80.0 + max(0.0, sell_burst - 50.0) * 0.15)
            seller_abs = clamp(50.0 + max(0.0, -delta_change) * 80.0 + max(0.0, buy_burst - 50.0) * 0.15)
            out = {
                "moomoo_tape_available": 1.0,
                "moomoo_tape_tick_count": float(len(df)),
                "moomoo_tape_score": round(tape_score, 3),
                "moomoo_delta_score": round(delta_score, 3),
                "moomoo_buy_burst_score": round(buy_burst, 3),
                "moomoo_sell_burst_score": round(sell_burst, 3),
                "moomoo_buyer_absorption_score": round(buyer_abs, 3),
                "moomoo_seller_absorption_score": round(seller_abs, 3),
                "moomoo_aggr_buy_volume": round(buy_vol, 3),
                "moomoo_aggr_sell_volume": round(sell_vol, 3),
                "moomoo_tape_delta": round(delta, 3),
                "moomoo_tape_delta_ratio": round(delta_ratio, 5),
                "moomoo_tape_delta_change": round(delta_change, 5),
                "moomoo_tape_velocity": round(velocity, 3),
                "moomoo_block_trade_count": float(block_count),
                "moomoo_block_buy_volume": round(block_buy_vol, 3),
                "moomoo_block_sell_volume": round(block_sell_vol, 3),
                "moomoo_block_delta": round(block_delta, 3),
                "moomoo_block_delta_score": round(clamp(50.0 + (block_delta / max(block_buy_vol + block_sell_vol, 1.0)) * 50.0), 3),
                "moomoo_bear_reversal_confidence": round(clamp((tape_score * 0.45) + (buyer_abs * 0.35) + ((100.0 - sell_burst) * 0.20)), 3),
                "moomoo_bull_continuation_confidence": round(clamp((tape_score * 0.50) + (buy_burst * 0.30) + (buyer_abs * 0.20)), 3),
            }
            for i, row in df.iterrows():
                raw_rows.append({
                    "ts": ts_iso(), "symbol": self.symbol, "seq": int(i) if str(i).isdigit() else len(raw_rows),
                    "trade_time": str(row.get(time_col, "")) if time_col else "",
                    "price": safe_float(row.get(price_col, 0.0)),
                    "volume": safe_float(row.get(vol_col, 1.0)) if vol_col else 1.0,
                    "dollar_value": safe_float(row.get(price_col, 0.0)) * (safe_float(row.get(vol_col, 1.0)) if vol_col else 1.0),
                    "direction": str(row.get(dir_col, "")) if dir_col else "",
                    "derived_side": "buy" if bool(buy_mask.loc[i]) else ("sell" if bool(sell_mask.loc[i]) else "unknown"),
                    "source": "MOOMOO_POLL",
                    "is_block_250k": 1 if (safe_float(row.get(price_col, 0.0)) * (safe_float(row.get(vol_col, 1.0)) if vol_col else 1.0)) >= 250000 else 0,
                    "is_block_5000sh": 1 if (safe_float(row.get(vol_col, 1.0)) if vol_col else 1.0) >= 5000 else 0,
                    "raw": json.dumps({str(c): row[c] for c in df.columns}, default=str)[:2000],
                })
            return out, raw_rows
        except Exception as e:
            log(self.log_file, f"Moomoo tape failed: {e}")
            return empty, raw_rows


    def get_peer_context(self, engine: FeatureEngine, peers: List[str], refresh_sec: float = 20.0, count: int = 90) -> Dict[str, Any]:
        """Compute live relative-strength and peer/ETF co-movement features.

        This is intentionally polled/cached because bars change once per minute and peer
        requests across many symbols can overload OpenD if done every few seconds.
        """
        now_ts = time.time()
        if self.peer_cache.get("features") and now_ts - float(self.peer_cache.get("ts", 0.0)) < refresh_sec:
            return dict(self.peer_cache["features"])
        out: Dict[str, Any] = {}
        symbols = [self.symbol] + [p.upper().strip() for p in peers if p.upper().strip() != self.symbol]
        bars_by_sym: Dict[str, pd.DataFrame] = {}
        if not self.connected:
            return out
        for sym in symbols:
            try:
                ret, data = self.ctx.get_cur_kline(moomoo_code(sym), count, ktype="K_1M", autype="qfq")
                if ret == RET_OK and data is not None and not data.empty:
                    b = engine.prepare_moomoo_bars(data)
                    if not b.empty:
                        bars_by_sym[sym] = b
            except Exception as e:
                log(self.log_file, f"Peer kline failed {sym}: {e}")
        def ret_n(sym: str, n: int) -> float:
            b = bars_by_sym.get(sym)
            if b is None or len(b) <= n:
                return 0.0
            c = pd.to_numeric(b["close"], errors="coerce").dropna()
            if len(c) <= n or safe_float(c.iloc[-n-1]) <= 0:
                return 0.0
            return (safe_float(c.iloc[-1]) / safe_float(c.iloc[-n-1]) - 1.0) * 100.0
        stock5 = ret_n(self.symbol, 5)
        qqq5 = ret_n("QQQ", 5)
        spy5 = ret_n("SPY", 5)
        out["stock_ret_5m"] = stock5
        out["qqq_ret_5m"] = qqq5
        out["spy_ret_5m"] = spy5
        out["rs_qqq_5m"] = stock5 - qqq5
        out["rs_spy_5m"] = stock5 - spy5
        out["market"] = (qqq5 + spy5) / 2.0
        # Simple beta-adjusted momentum. Defaults are deliberately conservative and only for recording.
        beta_qqq = 1.70
        beta_spy = 2.20
        out["beta_assumed_qqq"] = beta_qqq
        out["beta_assumed_spy"] = beta_spy
        out["beta_adj_rs_qqq_5m"] = stock5 - (beta_qqq * qqq5)
        out["beta_adj_rs_spy_5m"] = stock5 - (beta_spy * spy5)
        for peer in [p.upper().strip() for p in peers]:
            if not peer:
                continue
            out[f"peer_{peer.lower()}_ret_1m"] = ret_n(peer, 1)
            out[f"peer_{peer.lower()}_ret_5m"] = ret_n(peer, 5)
            out[f"peer_{peer.lower()}_lead_vs_smci_5m"] = ret_n(peer, 5) - stock5
        # Simple rolling correlation on 1m returns for available peers.
        smci_b = bars_by_sym.get(self.symbol)
        if smci_b is not None and len(smci_b) >= 20:
            smci_r = pd.to_numeric(smci_b["close"], errors="coerce").pct_change().tail(30).reset_index(drop=True)
            for peer in [p.upper().strip() for p in peers]:
                pb = bars_by_sym.get(peer)
                if pb is None or len(pb) < 20:
                    continue
                pr = pd.to_numeric(pb["close"], errors="coerce").pct_change().tail(30).reset_index(drop=True)
                n = min(len(smci_r), len(pr))
                corr = float(smci_r.tail(n).corr(pr.tail(n))) if n >= 10 else 0.0
                out[f"peer_{peer.lower()}_corr_30m"] = 0.0 if math.isnan(corr) else corr
        self.peer_cache = {"ts": now_ts, "features": dict(out)}
        return out


    def get_peer_tape_context(self, lookback_sec: float = 45.0) -> Dict[str, Any]:
        """Second-level peer tape features from async push buffers.

        This is intentionally independent from 1-minute peer bars. It can show
        whether NVDA/SMH/AMD aggressive prints are leading SMCI by 10-30 seconds.
        """
        out: Dict[str, Any] = {}
        now = now_ny()
        cutoff = now - timedelta(seconds=lookback_sec)

        def rows_for(sym: str) -> List[Dict[str, Any]]:
            with self.tape_lock:
                buf = self.tape_buffer if sym == self.symbol else self.peer_tape_buffers.get(sym, deque(maxlen=PEER_TAPE_BUFFER_MAXLEN))
                local_rows = list(buf)
            rows = []
            for r in local_rows:
                dt = parse_any_ts_ny(r.get("ts"))
                if dt is not None and dt >= cutoff:
                    rows.append(dict(r))
            return rows

        smci_rows = rows_for(self.symbol)
        def signed_delta(rows: List[Dict[str, Any]]) -> float:
            total = 0.0
            prev_px = None
            for r in rows:
                px = safe_float(r.get("price"), 0.0)
                vol = safe_float(r.get("volume"), 0.0)
                side = str(r.get("derived_side", "")).lower()
                direction = str(r.get("direction", "")).lower()
                sign = 0
                if "buy" in side or "buy" in direction or "ask" in direction or "up" in direction:
                    sign = 1
                elif "sell" in side or "sell" in direction or "bid" in direction or "down" in direction:
                    sign = -1
                elif prev_px is not None:
                    sign = 1 if px > prev_px else (-1 if px < prev_px else 0)
                total += sign * vol
                if px > 0:
                    prev_px = px
            return total

        smci_delta = signed_delta(smci_rows)
        out["smci_push_tick_count_45s"] = float(len(smci_rows))
        out["smci_push_delta_45s"] = float(smci_delta)
        for peer in self.peer_symbols:
            pr = rows_for(peer)
            pdlt = signed_delta(pr)
            out[f"peer_{peer.lower()}_push_tick_count_45s"] = float(len(pr))
            out[f"peer_{peer.lower()}_push_delta_45s"] = float(pdlt)
            out[f"peer_{peer.lower()}_delta_lead_vs_smci_45s"] = float(pdlt - smci_delta)
            # crude but useful: last-15 event price-change correlation where timestamps are too sparse for millisecond alignment
            try:
                sm_px = pd.Series([safe_float(x.get("price"), np.nan) for x in smci_rows[-15:]]).pct_change().dropna().reset_index(drop=True)
                pr_px = pd.Series([safe_float(x.get("price"), np.nan) for x in pr[-15:]]).pct_change().dropna().reset_index(drop=True)
                n = min(len(sm_px), len(pr_px))
                corr = float(sm_px.tail(n).corr(pr_px.tail(n))) if n >= 5 else 0.0
                out[f"peer_{peer.lower()}_push_corr_last15"] = 0.0 if math.isnan(corr) else corr
            except Exception:
                out[f"peer_{peer.lower()}_push_corr_last15"] = 0.0
        return out

    def get_capital_flow(self, min_interval_sec: int = 55) -> Dict[str, Any]:
        empty = {k: 0 for k in [
            "moomoo_cf_available", "moomoo_cf_rows", "moomoo_cf_in_flow", "moomoo_cf_super_in_flow", "moomoo_cf_big_in_flow",
            "moomoo_cf_mid_in_flow", "moomoo_cf_sml_in_flow", "moomoo_cf_main_in_flow", "moomoo_cf_whale_flow",
            "moomoo_cf_support_flow", "moomoo_cf_in_flow_delta_1m", "moomoo_cf_big_flow_delta_1m",
            "moomoo_cf_super_flow_delta_1m", "moomoo_cf_whale_flow_delta_1m", "moomoo_cf_in_flow_slope_3m",
            "moomoo_cf_big_flow_slope_3m", "moomoo_cf_whale_flow_slope_3m"]}
        empty["moomoo_cf_last_valid_time"] = ""
        empty["moomoo_cf_item_time"] = ""
        if not self.connected or ft is None:
            return empty
        now_ts = time.time()
        if self.last_cf and now_ts - self.last_cf_ts < min_interval_sec:
            return dict(self.last_cf)
        try:
            ret, data = self.ctx.get_capital_flow(self.code, period_type=ft.PeriodType.INTRADAY)
            self.last_cf_ts = now_ts
            if ret != RET_OK or data is None:
                return self.last_cf or empty
            df = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            if df.empty:
                return self.last_cf or empty
            df.columns = [str(c).lower() for c in df.columns]
            def col(name): return pd.to_numeric(df[name], errors="coerce") if name in df.columns else pd.Series(dtype=float)
            def last(name):
                s = col(name).dropna(); return safe_float(s.iloc[-1]) if len(s) else 0.0
            def delta(name):
                s = col(name).dropna(); return safe_float(s.iloc[-1] - s.iloc[-2]) if len(s) >= 2 else 0.0
            def slope(name):
                s = col(name).dropna(); return safe_float(s.iloc[-1] - s.iloc[-4]) if len(s) >= 4 else delta(name)
            super_f, big_f, mid_f, sml_f = last("super_in_flow"), last("big_in_flow"), last("mid_in_flow"), last("sml_in_flow")
            whale_series = col("super_in_flow").fillna(0) + col("big_in_flow").fillna(0) if "super_in_flow" in df.columns and "big_in_flow" in df.columns else pd.Series(dtype=float)
            whale_delta = safe_float(whale_series.iloc[-1] - whale_series.iloc[-2]) if len(whale_series.dropna()) >= 2 else 0.0
            whale_slope = safe_float(whale_series.iloc[-1] - whale_series.iloc[-4]) if len(whale_series.dropna()) >= 4 else whale_delta
            out = {
                "moomoo_cf_available": 1.0, "moomoo_cf_rows": float(len(df)),
                "moomoo_cf_last_valid_time": str(df["last_valid_time"].iloc[-1]) if "last_valid_time" in df.columns else "",
                "moomoo_cf_item_time": str(df["capital_flow_item_time"].iloc[-1]) if "capital_flow_item_time" in df.columns else "",
                "moomoo_cf_in_flow": round(last("in_flow"), 3),
                "moomoo_cf_super_in_flow": round(super_f, 3), "moomoo_cf_big_in_flow": round(big_f, 3),
                "moomoo_cf_mid_in_flow": round(mid_f, 3), "moomoo_cf_sml_in_flow": round(sml_f, 3),
                "moomoo_cf_main_in_flow": round(last("main_in_flow"), 3),
                "moomoo_cf_whale_flow": round(super_f + big_f, 3),
                "moomoo_cf_support_flow": round(mid_f + sml_f, 3),
                "moomoo_cf_in_flow_delta_1m": round(delta("in_flow"), 3),
                "moomoo_cf_big_flow_delta_1m": round(delta("big_in_flow"), 3),
                "moomoo_cf_super_flow_delta_1m": round(delta("super_in_flow"), 3),
                "moomoo_cf_whale_flow_delta_1m": round(whale_delta, 3),
                "moomoo_cf_in_flow_slope_3m": round(slope("in_flow"), 3),
                "moomoo_cf_big_flow_slope_3m": round(slope("big_in_flow"), 3),
                "moomoo_cf_whale_flow_slope_3m": round(whale_slope, 3),
            }
            self.last_cf = out
            return out
        except Exception as e:
            log(self.log_file, f"Moomoo capital flow failed: {e}")
            return self.last_cf or empty


class IBKRRecorder:
    def __init__(self, symbol: str, host: str, port: int, client_id: int, log_file: Path):
        self.symbol = symbol.upper().strip()
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.log_file = log_file
        self.ib = None
        self.contract = None
        self.ticker = None
        self.connected = False

    def connect(self) -> bool:
        if IB is None or Stock is None:
            log(self.log_file, "ib_insync not available; IBKR disabled.")
            return False
        try:
            self.ib = IB()
            self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=5)
            self.contract = Stock(self.symbol, "SMART", "USD")
            self.ib.qualifyContracts(self.contract)
            self.ticker = self.ib.reqMktData(self.contract, "", False, False)
            self.connected = True
            log(self.log_file, f"IBKR connected {self.host}:{self.port} clientId={self.client_id}")
            return True
        except Exception as e:
            self.connected = False
            log(self.log_file, f"IBKR connect failed: {e}")
            return False


    def close(self):
        try:
            if self.ib:
                self.ib.disconnect()
        except Exception:
            pass

    def snapshot(self) -> Dict[str, Any]:
        out = {"ibkr_available": 0.0}
        if not self.connected or self.ib is None or self.ticker is None:
            return out
        try:
            self.ib.sleep(0.1)
            bid = safe_float(getattr(self.ticker, "bid", 0.0))
            ask = safe_float(getattr(self.ticker, "ask", 0.0))
            last = safe_float(getattr(self.ticker, "last", 0.0))
            close = safe_float(getattr(self.ticker, "close", 0.0))
            out.update({
                "ibkr_available": 1.0,
                "ibkr_bid": bid,
                "ibkr_ask": ask,
                "ibkr_last": last,
                "ibkr_close": close,
                "ibkr_bid_size": safe_float(getattr(self.ticker, "bidSize", 0.0)),
                "ibkr_ask_size": safe_float(getattr(self.ticker, "askSize", 0.0)),
                "ibkr_last_size": safe_float(getattr(self.ticker, "lastSize", 0.0)),
                "ibkr_volume": safe_float(getattr(self.ticker, "volume", 0.0)),
                "ibkr_high": safe_float(getattr(self.ticker, "high", 0.0)),
                "ibkr_low": safe_float(getattr(self.ticker, "low", 0.0)),
                "ibkr_spread": ask - bid if ask > 0 and bid > 0 else 0.0,
                "ibkr_mid": (ask + bid) / 2.0 if ask > 0 and bid > 0 else 0.0,
                "ibkr_snapshot_time_ny": ts_iso(),
            })
            return out
        except Exception as e:
            log(self.log_file, f"IBKR snapshot failed: {e}")
            return out


# Preserve all attached feature columns and add extras.
BASE_FEATURE_COLUMNS = [
    # Existing 39-column live feature file
    "ts", "symbol", "last", "bid", "ask", "spread", "vwap", "bias_score", "entry_score", "regime",
    "rs_qqq_5m", "stock_ret_5m", "qqq_ret_5m", "volume_ratio", "structure_score", "momentum_score",
    "vwap_score", "orderflow_score", "orderbook_score", "orderbook_imbalance", "decision", "reason",
    "smart_money_composite_score", "smart_money_gate_ok", "smart_money_reason", "reversal_reclaim_score",
    "reversal_reclaim_ok", "reversal_reason", "bars_since_reversal_low", "recovery_from_reversal_low_pct",
    "selloff_from_reversal_high_pct", "tape_score", "delta_score", "buy_burst_score", "sell_burst_score",
    "buyer_absorption_score", "seller_absorption_score", "bear_day_reversal_confidence", "bull_continuation_confidence",
    # Existing 57-column merged feature file
    "close", "day_position", "control_delta", "rel_vol_30", "rel_range_30", "rel_body_30", "vol_pctile_today",
    "range_pctile_today", "slope_5", "slope_20", "noise_score", "tf5_control", "tf10_control", "tf15_control",
    "market", "micro", "spread_score", "moomoo_l2_score", "moomoo_l2_imbalance", "moomoo_l2_persistence",
    "moomoo_spread_pct", "moomoo_bid_levels", "moomoo_ask_levels", "moomoo_tape_score", "moomoo_delta_score",
    "moomoo_buy_burst_score", "moomoo_sell_burst_score", "moomoo_buyer_absorption_score", "moomoo_seller_absorption_score",
    "moomoo_bear_reversal_confidence", "moomoo_bull_continuation_confidence", "moomoo_cf_available", "moomoo_cf_rows",
    "moomoo_cf_last_valid_time", "moomoo_cf_item_time", "moomoo_cf_in_flow", "moomoo_cf_super_in_flow",
    "moomoo_cf_big_in_flow", "moomoo_cf_mid_in_flow", "moomoo_cf_sml_in_flow", "moomoo_cf_main_in_flow",
    "moomoo_cf_whale_flow", "moomoo_cf_support_flow", "moomoo_cf_in_flow_delta_1m", "moomoo_cf_big_flow_delta_1m",
    "moomoo_cf_super_flow_delta_1m", "moomoo_cf_whale_flow_delta_1m", "moomoo_cf_in_flow_slope_3m",
    "moomoo_cf_big_flow_slope_3m", "moomoo_cf_whale_flow_slope_3m", "coiled_l2_imbalance_diff_2bar",
    "coiled_entry_ready", "coiled_break_even_locked", "coiled_peak_return_pct",
    # Extra recorder features
    "bar_time_utc", "bar_time_ny", "open", "high", "low", "volume", "turnover", "ema_8", "ema_21", "ema_50", "ema_100",
    "range", "body", "close_location", "dist_vwap_pct", "buyer_pressure", "seller_pressure", "green_ratio_10", "red_ratio_10",
    "above_vwap_ratio_20", "bull_stack_ratio_20", "bear_stack_ratio_20", "slope_3", "slope_10", "slope_60", "body_pctile_today",
    "drop_from_open_pct", "drop_from_high_pct", "bounce_from_low_pct", "low_age_bars", "higher_low_proxy",
    "seller_pressure_change_5", "buyer_pressure_change_5", "ema8_reclaimed", "ema8_hold_bars_2", "vwap_hold_bars_2",
    "two_bar_reclaim_hold", "trend_cleanliness", "normal_range", "moomoo_l2_available", "moomoo_l2_positive_count",
    "moomoo_l2_negative_count", "moomoo_bid_volume_50", "moomoo_ask_volume_50", "moomoo_bid_volume_50_weighted",
    "moomoo_ask_volume_50_weighted", "moomoo_tape_available", "moomoo_tape_tick_count", "moomoo_aggr_buy_volume",
    "moomoo_aggr_sell_volume", "moomoo_tape_delta", "moomoo_tape_delta_ratio", "moomoo_tape_delta_change", "moomoo_tape_velocity",
    "moomoo_snapshot_available", "snapshot_last_price", "snapshot_bid_price", "snapshot_ask_price", "snapshot_volume", "snapshot_turnover",
    "snapshot_open_price", "snapshot_high_price", "snapshot_low_price", "snapshot_prev_close_price", "ibkr_available", "ibkr_bid",
    "ibkr_ask", "ibkr_last", "ibkr_close", "ibkr_bid_size", "ibkr_ask_size", "ibkr_last_size", "ibkr_volume", "ibkr_high",
    "ibkr_low", "ibkr_spread", "ibkr_mid",
    "moomoo_bid_volume_top3", "moomoo_ask_volume_top3", "moomoo_l2_top3_imbalance",
    "moomoo_bid_volume_top10", "moomoo_ask_volume_top10", "moomoo_l2_top10_imbalance",
    "moomoo_block_trade_count", "moomoo_block_buy_volume", "moomoo_block_sell_volume", "moomoo_block_delta", "moomoo_block_delta_score",
    "spy_ret_5m", "rs_spy_5m", "peer_nvda_ret_1m", "peer_nvda_ret_5m", "peer_nvda_lead_vs_smci_5m", "peer_nvda_corr_30m",
    "peer_amd_ret_1m", "peer_amd_ret_5m", "peer_amd_lead_vs_smci_5m", "peer_amd_corr_30m",
    "peer_smh_ret_1m", "peer_smh_ret_5m", "peer_smh_lead_vs_smci_5m", "peer_smh_corr_30m",
    "moomoo_l2_decay_lambda", "moomoo_l2_exp_imbalance",
    "beta_assumed_qqq", "beta_assumed_spy", "beta_adj_rs_qqq_5m", "beta_adj_rs_spy_5m",
    "cycle_ts_ny", "feature_bar_time_ny", "feature_bar_age_sec", "ibkr_snapshot_time_ny", "ibkr_vs_cycle_age_sec",
    "ibkr_vs_bar_lag_sec", "alignment_warning",
    "smci_push_tick_count_45s", "smci_push_delta_45s",
    "peer_nvda_push_tick_count_45s", "peer_nvda_push_delta_45s", "peer_nvda_delta_lead_vs_smci_45s", "peer_nvda_push_corr_last15",
    "peer_amd_push_tick_count_45s", "peer_amd_push_delta_45s", "peer_amd_delta_lead_vs_smci_45s", "peer_amd_push_corr_last15",
    "peer_smh_push_tick_count_45s", "peer_smh_push_delta_45s", "peer_smh_delta_lead_vs_smci_45s", "peer_smh_push_corr_last15",
    "peer_qqq_push_tick_count_45s", "peer_qqq_push_delta_45s", "peer_qqq_delta_lead_vs_smci_45s", "peer_qqq_push_corr_last15",
    "peer_spy_push_tick_count_45s", "peer_spy_push_delta_45s", "peer_spy_delta_lead_vs_smci_45s", "peer_spy_push_corr_last15"
]


# ============================================================================
# FIXED FEATURE CSV SCHEMA CONTRACT
# ----------------------------------------------------------------------------
# Canonical full V7 feature schema verified from complete 433-column recordings
# (2026-08-26, 2026-08-28 and 2026-09-02 all have the same header/order).
# Every features_wide row MUST be written against this exact list. A source value
# may be unavailable for a minute, but its column must never disappear.
# ============================================================================
FEATURE_FIELDS: List[str] = ['ts',
 'symbol',
 'bar_time_utc',
 'bar_time_ny',
 'close',
 'open',
 'high',
 'low',
 'volume',
 'turnover',
 'vwap',
 'ema_8',
 'ema_21',
 'ema_50',
 'ema_100',
 'range',
 'body',
 'close_location',
 'day_position',
 'dist_vwap_pct',
 'control_delta',
 'buyer_pressure',
 'seller_pressure',
 'rel_vol_30',
 'rel_range_30',
 'rel_body_30',
 'green_ratio_10',
 'red_ratio_10',
 'above_vwap_ratio_20',
 'bull_stack_ratio_20',
 'bear_stack_ratio_20',
 'slope_3',
 'slope_5',
 'slope_10',
 'slope_20',
 'slope_60',
 'normal_range',
 'vol_pctile_today',
 'range_pctile_today',
 'body_pctile_today',
 'drop_from_open_pct',
 'drop_from_high_pct',
 'bounce_from_low_pct',
 'low_age_bars',
 'higher_low_proxy',
 'seller_pressure_change_5',
 'buyer_pressure_change_5',
 'ema8_reclaimed',
 'ema8_hold_bars_2',
 'vwap_hold_bars_2',
 'two_bar_reclaim_hold',
 'trend_cleanliness',
 'noise_score',
 'tf5_control',
 'tf10_control',
 'tf15_control',
 'stock_ret_5m',
 'qqq_ret_5m',
 'spy_ret_5m',
 'rs_qqq_5m',
 'rs_spy_5m',
 'market',
 'beta_assumed_qqq',
 'beta_assumed_spy',
 'beta_adj_rs_qqq_5m',
 'beta_adj_rs_spy_5m',
 'peer_nvda_ret_1m',
 'peer_nvda_ret_5m',
 'peer_nvda_lead_vs_smci_5m',
 'peer_amd_ret_1m',
 'peer_amd_ret_5m',
 'peer_amd_lead_vs_smci_5m',
 'peer_smh_ret_1m',
 'peer_smh_ret_5m',
 'peer_smh_lead_vs_smci_5m',
 'peer_qqq_ret_1m',
 'peer_qqq_ret_5m',
 'peer_qqq_lead_vs_smci_5m',
 'peer_spy_ret_1m',
 'peer_spy_ret_5m',
 'peer_spy_lead_vs_smci_5m',
 'peer_nvda_corr_30m',
 'peer_amd_corr_30m',
 'peer_smh_corr_30m',
 'peer_qqq_corr_30m',
 'peer_spy_corr_30m',
 'smci_push_tick_count_45s',
 'smci_push_delta_45s',
 'peer_nvda_push_tick_count_45s',
 'peer_nvda_push_delta_45s',
 'peer_nvda_delta_lead_vs_smci_45s',
 'peer_nvda_push_corr_last15',
 'peer_amd_push_tick_count_45s',
 'peer_amd_push_delta_45s',
 'peer_amd_delta_lead_vs_smci_45s',
 'peer_amd_push_corr_last15',
 'peer_smh_push_tick_count_45s',
 'peer_smh_push_delta_45s',
 'peer_smh_delta_lead_vs_smci_45s',
 'peer_smh_push_corr_last15',
 'peer_qqq_push_tick_count_45s',
 'peer_qqq_push_delta_45s',
 'peer_qqq_delta_lead_vs_smci_45s',
 'peer_qqq_push_corr_last15',
 'peer_spy_push_tick_count_45s',
 'peer_spy_push_delta_45s',
 'peer_spy_delta_lead_vs_smci_45s',
 'peer_spy_push_corr_last15',
 'push_smci_tape_buffer_len',
 'push_smci_tape_buffer_maxlen',
 'push_tape_dedup_len',
 'push_tape_dedup_maxlen',
 'push_orderbook_buffer_len',
 'push_quote_buffer_len',
 'push_kline_buffer_len',
 'push_peer_nvda_tape_buffer_len',
 'push_peer_nvda_tape_buffer_maxlen',
 'push_peer_amd_tape_buffer_len',
 'push_peer_amd_tape_buffer_maxlen',
 'push_peer_smh_tape_buffer_len',
 'push_peer_smh_tape_buffer_maxlen',
 'push_peer_qqq_tape_buffer_len',
 'push_peer_qqq_tape_buffer_maxlen',
 'push_peer_spy_tape_buffer_len',
 'push_peer_spy_tape_buffer_maxlen',
 'moomoo_snapshot_available',
 'snapshot_code',
 'snapshot_name',
 'snapshot_update_time',
 'snapshot_last_price',
 'snapshot_open_price',
 'snapshot_high_price',
 'snapshot_low_price',
 'snapshot_prev_close_price',
 'snapshot_volume',
 'snapshot_turnover',
 'snapshot_turnover_rate',
 'snapshot_suspension',
 'snapshot_listing_date',
 'snapshot_lot_size',
 'snapshot_price_spread',
 'snapshot_stock_owner',
 'snapshot_ask_price',
 'snapshot_bid_price',
 'snapshot_ask_vol',
 'snapshot_bid_vol',
 'snapshot_enable_margin',
 'snapshot_mortgage_ratio',
 'snapshot_long_margin_initial_ratio',
 'snapshot_enable_short_sell',
 'snapshot_short_sell_rate',
 'snapshot_short_available_volume',
 'snapshot_short_margin_initial_ratio',
 'snapshot_amplitude',
 'snapshot_avg_price',
 'snapshot_bid_ask_ratio',
 'snapshot_volume_ratio',
 'snapshot_highest52weeks_price',
 'snapshot_lowest52weeks_price',
 'snapshot_highest_history_price',
 'snapshot_lowest_history_price',
 'snapshot_close_price_5min',
 'snapshot_after_volume',
 'snapshot_after_turnover',
 'snapshot_sec_status',
 'snapshot_equity_valid',
 'snapshot_issued_shares',
 'snapshot_total_market_val',
 'snapshot_net_asset',
 'snapshot_net_profit',
 'snapshot_earning_per_share',
 'snapshot_outstanding_shares',
 'snapshot_circular_market_val',
 'snapshot_net_asset_per_share',
 'snapshot_ey_ratio',
 'snapshot_pe_ratio',
 'snapshot_pb_ratio',
 'snapshot_pe_ttm_ratio',
 'snapshot_dividend_ttm',
 'snapshot_dividend_ratio_ttm',
 'snapshot_dividend_lfy',
 'snapshot_dividend_lfy_ratio',
 'snapshot_wrt_valid',
 'snapshot_wrt_conversion_ratio',
 'snapshot_wrt_type',
 'snapshot_wrt_strike_price',
 'snapshot_wrt_maturity_date',
 'snapshot_wrt_end_trade',
 'snapshot_wrt_recovery_price',
 'snapshot_wrt_street_vol',
 'snapshot_wrt_issue_vol',
 'snapshot_wrt_street_ratio',
 'snapshot_wrt_delta',
 'snapshot_wrt_implied_volatility',
 'snapshot_wrt_premium',
 'snapshot_wrt_leverage',
 'snapshot_wrt_ipop',
 'snapshot_wrt_break_even_point',
 'snapshot_wrt_conversion_price',
 'snapshot_wrt_price_recovery_ratio',
 'snapshot_wrt_score',
 'snapshot_wrt_upper_strike_price',
 'snapshot_wrt_lower_strike_price',
 'snapshot_wrt_inline_price_status',
 'snapshot_wrt_issuer_code',
 'snapshot_option_valid',
 'snapshot_option_type',
 'snapshot_strike_time',
 'snapshot_option_strike_price',
 'snapshot_option_contract_size',
 'snapshot_option_open_interest',
 'snapshot_option_implied_volatility',
 'snapshot_option_premium',
 'snapshot_option_delta',
 'snapshot_option_gamma',
 'snapshot_option_vega',
 'snapshot_option_theta',
 'snapshot_option_rho',
 'snapshot_option_net_open_interest',
 'snapshot_option_expiry_date_distance',
 'snapshot_option_contract_nominal_value',
 'snapshot_option_owner_lot_multiplier',
 'snapshot_option_area_type',
 'snapshot_option_contract_multiplier',
 'snapshot_index_option_type',
 'snapshot_index_valid',
 'snapshot_index_raise_count',
 'snapshot_index_fall_count',
 'snapshot_index_equal_count',
 'snapshot_plate_valid',
 'snapshot_plate_raise_count',
 'snapshot_plate_fall_count',
 'snapshot_plate_equal_count',
 'snapshot_future_valid',
 'snapshot_future_last_settle_price',
 'snapshot_future_position',
 'snapshot_future_position_change',
 'snapshot_future_main_contract',
 'snapshot_future_last_trade_time',
 'snapshot_trust_valid',
 'snapshot_trust_dividend_yield',
 'snapshot_trust_aum',
 'snapshot_trust_outstanding_units',
 'snapshot_trust_netassetvalue',
 'snapshot_trust_premium',
 'snapshot_trust_assetclass',
 'snapshot_pre_price',
 'snapshot_pre_high_price',
 'snapshot_pre_low_price',
 'snapshot_pre_volume',
 'snapshot_pre_turnover',
 'snapshot_pre_change_val',
 'snapshot_pre_change_rate',
 'snapshot_pre_amplitude',
 'snapshot_after_price',
 'snapshot_after_high_price',
 'snapshot_after_low_price',
 'snapshot_after_change_val',
 'snapshot_after_change_rate',
 'snapshot_after_amplitude',
 'snapshot_overnight_price',
 'snapshot_overnight_high_price',
 'snapshot_overnight_low_price',
 'snapshot_overnight_volume',
 'snapshot_overnight_turnover',
 'snapshot_overnight_change_val',
 'snapshot_overnight_change_rate',
 'snapshot_overnight_amplitude',
 'moomoo_l2_available',
 'moomoo_l2_score',
 'moomoo_l2_imbalance',
 'moomoo_l2_persistence',
 'moomoo_l2_positive_count',
 'moomoo_l2_negative_count',
 'moomoo_bid_levels',
 'moomoo_ask_levels',
 'moomoo_bid_volume_50',
 'moomoo_ask_volume_50',
 'moomoo_bid_volume_50_weighted',
 'moomoo_ask_volume_50_weighted',
 'moomoo_l2_decay_lambda',
 'moomoo_l2_exp_imbalance',
 'moomoo_bid_volume_top3',
 'moomoo_ask_volume_top3',
 'moomoo_l2_top3_imbalance',
 'moomoo_bid_volume_top10',
 'moomoo_ask_volume_top10',
 'moomoo_l2_top10_imbalance',
 'coiled_l2_imbalance_diff_2bar',
 'moomoo_tape_available',
 'moomoo_tape_source',
 'moomoo_tape_tick_count',
 'moomoo_tape_score',
 'moomoo_delta_score',
 'moomoo_buy_burst_score',
 'moomoo_sell_burst_score',
 'moomoo_buyer_absorption_score',
 'moomoo_seller_absorption_score',
 'moomoo_aggr_buy_volume',
 'moomoo_aggr_sell_volume',
 'moomoo_tape_delta',
 'moomoo_tape_delta_ratio',
 'moomoo_tape_delta_change',
 'moomoo_tape_velocity',
 'moomoo_block_trade_count',
 'moomoo_block_buy_volume',
 'moomoo_block_sell_volume',
 'moomoo_block_delta',
 'moomoo_block_delta_score',
 'moomoo_bear_reversal_confidence',
 'moomoo_bull_continuation_confidence',
 'moomoo_cf_available',
 'moomoo_cf_rows',
 'moomoo_cf_last_valid_time',
 'moomoo_cf_item_time',
 'moomoo_cf_in_flow',
 'moomoo_cf_super_in_flow',
 'moomoo_cf_big_in_flow',
 'moomoo_cf_mid_in_flow',
 'moomoo_cf_sml_in_flow',
 'moomoo_cf_main_in_flow',
 'moomoo_cf_whale_flow',
 'moomoo_cf_support_flow',
 'moomoo_cf_in_flow_delta_1m',
 'moomoo_cf_big_flow_delta_1m',
 'moomoo_cf_super_flow_delta_1m',
 'moomoo_cf_whale_flow_delta_1m',
 'moomoo_cf_in_flow_slope_3m',
 'moomoo_cf_big_flow_slope_3m',
 'moomoo_cf_whale_flow_slope_3m',
 'ibkr_available',
 'ibkr_bid',
 'ibkr_ask',
 'ibkr_last',
 'ibkr_close',
 'ibkr_bid_size',
 'ibkr_ask_size',
 'ibkr_last_size',
 'ibkr_volume',
 'ibkr_high',
 'ibkr_low',
 'ibkr_spread',
 'ibkr_mid',
 'ibkr_snapshot_time_ny',
 'ibkr_position_qty',
 'strategy_equity',
 'strategy_equity_floor',
 'strategy_halted',
 'last',
 'bid',
 'ask',
 'spread',
 'moomoo_spread_pct',
 'spread_score',
 'orderbook_score',
 'orderbook_imbalance',
 'tape_score',
 'delta_score',
 'buy_burst_score',
 'sell_burst_score',
 'buyer_absorption_score',
 'seller_absorption_score',
 'bear_day_reversal_confidence',
 'bull_continuation_confidence',
 'volume_ratio',
 'structure_score',
 'momentum_score',
 'vwap_score',
 'orderflow_score',
 'bias_score',
 'entry_score',
 'smart_money_composite_score',
 'smart_money_gate_ok',
 'smart_money_reason',
 'reversal_reclaim_score',
 'reversal_reclaim_ok',
 'reversal_reason',
 'bars_since_reversal_low',
 'recovery_from_reversal_low_pct',
 'selloff_from_reversal_high_pct',
 'micro',
 'decision',
 'reason',
 'regime',
 'coiled_entry_ready',
 'coiled_break_even_locked',
 'coiled_peak_return_pct',
 'cycle_ts_ny',
 'feature_bar_time_ny',
 'feature_bar_age_sec',
 'ibkr_vs_cycle_age_sec',
 'ibkr_vs_bar_lag_sec',
 'alignment_warning',
 'mf_tape_event_count',
 'mf_push_tape_event_count',
 'mf_buy_trade_count',
 'mf_sell_trade_count',
 'mf_buy_volume',
 'mf_sell_volume',
 'mf_unknown_volume',
 'mf_total_tape_volume',
 'mf_tape_delta_volume',
 'mf_tape_delta_ratio',
 'mf_block_buy_volume',
 'mf_block_sell_volume',
 'mf_block_delta_volume',
 'mf_trade_price_std',
 'mf_trade_size_mean',
 'mf_trade_size_max',
 'mf_l2_poll_count',
 'mf_l2_level_rows',
 'mf_push_orderbook_event_count',
 'mf_push_quote_event_count',
 'mf_push_kline_event_count',
 'mf_l2_imbalance_mean',
 'mf_l2_imbalance_min',
 'mf_l2_imbalance_max',
 'mf_l2_imbalance_std',
 'mf_l2_top3_imbalance_mean',
 'mf_l2_top3_imbalance_min',
 'mf_l2_top3_imbalance_max',
 'mf_l2_top3_positive_persistence',
 'mf_l2_top3_negative_persistence',
 'mf_l2_top3_longest_same_side_run',
 'mf_l2_top10_imbalance_mean',
 'mf_spread_pct_mean',
 'mf_spread_pct_max',
 'mf_bid_top3_mean',
 'mf_ask_top3_mean',
 'mf_inside_depth_bias',
 'feature_row_type',
 'feature_minute_key_utc',
 'feature_emit_ts_ny']

if len(FEATURE_FIELDS) != 433 or len(set(FEATURE_FIELDS)) != 433:
    raise RuntimeError("FEATURE_FIELDS invariant failed: expected 433 unique columns")

def audit_feature_schema(row: Dict[str, Any], log_file: Path) -> Tuple[List[str], List[str]]:
    """Report missing/extra feature keys without changing the trading decision.

    Missing canonical fields are safe for serialization: append_csv writes blank cells
    in their fixed locations. Extra fields are reported loudly so schema evolution is
    never silent; they are not allowed to alter the frozen 433-column file layout.
    """
    expected = set(FEATURE_FIELDS)
    actual = set(row.keys())
    missing = [k for k in FEATURE_FIELDS if k not in actual]
    extra = sorted(actual - expected)
    if missing or extra:
        log(
            log_file,
            f"FEATURE_SCHEMA_AUDIT expected=433 actual_keys={len(actual)} "
            f"missing={len(missing)} extra={len(extra)} "
            f"missing_keys={missing[:20]} extra_keys={extra[:20]}"
        )
    return missing, extra


def build_row(symbol: str, f: Dict[str, Any], snap: Dict[str, Any], l2: Dict[str, Any], tape: Dict[str, Any], cf: Dict[str, Any], ib: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {"ts": ts_iso(), "symbol": symbol.upper()}
    row.update(f); row.update(snap); row.update(l2); row.update(tape); row.update(cf); row.update(ib)

    # Aliases for old schemas. V3 EXECUTION-PARITY REPAIR:
    # IBKR is the price authority for strategy/execution. Moomoo quote fields remain
    # preserved in their explicit snapshot_* columns for research/audit.
    last = first_positive(row.get("ibkr_last"), row.get("close"), row.get("snapshot_last_price"))
    bid = first_positive(row.get("ibkr_bid"), row.get("snapshot_bid_price"))
    ask = first_positive(row.get("ibkr_ask"), row.get("snapshot_ask_price"))
    spread = ask - bid if ask > 0 and bid > 0 else 0.0
    row["last"] = last
    row["bid"] = bid
    row["ask"] = ask
    row["spread"] = spread
    # Preserve the historical Moomoo spread feature under its original name.
    mbid=first_positive(row.get("snapshot_bid_price")); mask=first_positive(row.get("snapshot_ask_price")); mlast=first_positive(row.get("snapshot_last_price"),row.get("close"))
    mspread=mask-mbid if mask>0 and mbid>0 else 0.0
    row["moomoo_spread_pct"] = (mspread / mlast * 100.0) if mlast > 0 and mspread > 0 else 0.0
    row["ibkr_spread_pct"] = (spread / last * 100.0) if last > 0 and spread > 0 else 0.0
    row["spread_score"] = clamp(100.0 - row["ibkr_spread_pct"] * 500.0)
    row["orderbook_score"] = row.get("moomoo_l2_score", 50.0)
    row["orderbook_imbalance"] = row.get("moomoo_l2_imbalance", 0.0)
    row["tape_score"] = row.get("moomoo_tape_score", 50.0)
    row["delta_score"] = row.get("moomoo_delta_score", 50.0)
    row["buy_burst_score"] = row.get("moomoo_buy_burst_score", 50.0)
    row["sell_burst_score"] = row.get("moomoo_sell_burst_score", 50.0)
    row["buyer_absorption_score"] = row.get("moomoo_buyer_absorption_score", 50.0)
    row["seller_absorption_score"] = row.get("moomoo_seller_absorption_score", 50.0)
    row["bear_day_reversal_confidence"] = row.get("moomoo_bear_reversal_confidence", 50.0)
    row["bull_continuation_confidence"] = row.get("moomoo_bull_continuation_confidence", 50.0)
    row["volume_ratio"] = row.get("rel_vol_30", 1.0)
    row["structure_score"] = clamp((safe_float(row.get("day_position"), 50.0) * 0.35) + (safe_float(row.get("close_location"), 50.0) * 0.35) + (safe_float(row.get("ema8_reclaimed"), 0.0) * 0.30))
    row["momentum_score"] = clamp(50.0 + safe_float(row.get("slope_5"), 0.0) * 5.0 + safe_float(row.get("control_delta"), 0.0) * 0.5)
    row["vwap_score"] = clamp(50.0 + safe_float(row.get("dist_vwap_pct"), 0.0) * 10.0)
    row["orderflow_score"] = clamp((safe_float(row.get("tape_score"), 50.0) + safe_float(row.get("delta_score"), 50.0)) / 2.0)
    row["bias_score"] = clamp((row["structure_score"] + row["momentum_score"] + row["vwap_score"] + safe_float(row.get("orderbook_score"), 50.0)) / 4.0)
    row["entry_score"] = clamp((row["bias_score"] * 0.35) + (row["orderflow_score"] * 0.35) + (safe_float(row.get("bull_continuation_confidence"), 50.0) * 0.30))
    row["smart_money_composite_score"] = clamp((safe_float(row.get("moomoo_cf_whale_flow_delta_1m"), 0.0) / 100000.0) + row["entry_score"])
    row["smart_money_gate_ok"] = 1 if row["entry_score"] >= 70 else 0
    row["smart_money_reason"] = "record_only_no_trade"
    row["reversal_reclaim_score"] = clamp((safe_float(row.get("bounce_from_low_pct"), 0.0) * 10.0) + safe_float(row.get("ema8_reclaimed"), 0.0) * 0.4 + safe_float(row.get("higher_low_proxy"), 0.0) * 0.2)
    row["reversal_reclaim_ok"] = 1 if row["reversal_reclaim_score"] >= 70 else 0
    row["reversal_reason"] = "record_only"
    row["bars_since_reversal_low"] = row.get("low_age_bars", 0.0)
    row["recovery_from_reversal_low_pct"] = row.get("bounce_from_low_pct", 0.0)
    row["selloff_from_reversal_high_pct"] = row.get("drop_from_high_pct", 0.0)
    row["market"] = safe_float(row.get("market"), 0.0)
    row["micro"] = row.get("orderflow_score", 50.0)
    row["decision"] = "RECORD_ONLY"
    row["reason"] = "data_capture_only_no_trading"
    row["regime"] = "RECORDER"
    row["rs_qqq_5m"] = safe_float(row.get("rs_qqq_5m"), 0.0)
    row["stock_ret_5m"] = safe_float(row.get("stock_ret_5m"), row.get("slope_5", 0.0))
    row["qqq_ret_5m"] = safe_float(row.get("qqq_ret_5m"), 0.0)
    row["coiled_entry_ready"] = 1 if (safe_float(row.get("control_delta")) >= 15 and safe_float(row.get("coiled_l2_imbalance_diff_2bar")) >= 0.05 and safe_float(row.get("noise_score")) <= 85) else 0
    row["coiled_break_even_locked"] = 0
    row["coiled_peak_return_pct"] = 0

    # Timestamp co-alignment diagnostics. These are for later ML hygiene, not trading.
    cycle_dt = parse_any_ts_ny(row.get("ts")) or now_ny()
    bar_dt = parse_any_ts_ny(row.get("bar_time_ny")) or parse_any_ts_ny(row.get("bar_time_utc"))
    ib_dt = parse_any_ts_ny(row.get("ibkr_snapshot_time_ny")) or cycle_dt
    row["cycle_ts_ny"] = cycle_dt.isoformat()
    row["feature_bar_time_ny"] = bar_dt.isoformat() if bar_dt else ""
    row["ibkr_snapshot_time_ny"] = ib_dt.isoformat() if ib_dt else ""
    row["feature_bar_age_sec"] = (cycle_dt - bar_dt).total_seconds() if bar_dt else 0.0
    row["ibkr_vs_cycle_age_sec"] = (cycle_dt - ib_dt).total_seconds() if ib_dt else 0.0
    row["ibkr_vs_bar_lag_sec"] = (ib_dt - bar_dt).total_seconds() if (ib_dt and bar_dt) else 0.0
    row["alignment_warning"] = 1 if (bar_dt and abs(row["ibkr_vs_bar_lag_sec"]) > 75) else 0
    return row



class MinuteFeatureAccumulator:
    """RAM-only microstructure accumulator for one closed-minute feature row.

    v7 deliberately avoids raw CSV event stores. Push/poll events update this object,
    and once a 1-minute RTH bar is closed we emit compact engineered features then reset.
    """
    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.tape_rows: List[Dict[str, Any]] = []
        self.l2_rows: List[Dict[str, Any]] = []
        self.l2_snaps: List[Dict[str, Any]] = []
        self.snapshot_rows: List[Dict[str, Any]] = []
        self.ib_rows: List[Dict[str, Any]] = []
        self.push_orderbook_count = 0
        self.push_quote_count = 0
        self.push_kline_count = 0
        self.push_tape_count = 0

    def add_snapshot(self, snap: Dict[str, Any]) -> None:
        if snap:
            self.snapshot_rows.append(dict(snap))

    def add_ibkr(self, snap: Dict[str, Any]) -> None:
        if snap:
            self.ib_rows.append(dict(snap))

    def add_l2_snapshot(self, l2: Dict[str, Any]) -> None:
        if l2:
            self.l2_snaps.append(dict(l2))

    def add_l2_rows(self, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self.l2_rows.extend(dict(r) for r in rows)

    def add_tape_rows(self, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self.tape_rows.extend(dict(r) for r in rows)

    def add_push_rows(self, push_rows: Dict[str, List[Dict[str, Any]]]) -> None:
        tape = push_rows.get("tape", []) or []
        orderbook = push_rows.get("orderbook", []) or []
        quote = push_rows.get("quote", []) or []
        kline = push_rows.get("kline", []) or []
        self.push_tape_count += len(tape)
        self.push_orderbook_count += len(orderbook)
        self.push_quote_count += len(quote)
        self.push_kline_count += len(kline)
        self.add_tape_rows(tape)

    @staticmethod
    def _series(vals: List[float]) -> pd.Series:
        return pd.Series([v for v in vals if v is not None and not pd.isna(v)], dtype="float64")

    @staticmethod
    def _safe_stat(vals: List[float], fn: str) -> float:
        s = MinuteFeatureAccumulator._series(vals)
        if s.empty:
            return 0.0
        if fn == "mean": return safe_float(s.mean(), 0.0)
        if fn == "std": return safe_float(s.std(ddof=0), 0.0)
        if fn == "min": return safe_float(s.min(), 0.0)
        if fn == "max": return safe_float(s.max(), 0.0)
        if fn == "sum": return safe_float(s.sum(), 0.0)
        return 0.0

    def _signed_tape(self) -> Tuple[float, float, float, int, int, float, float]:
        buy_vol = sell_vol = unknown_vol = 0.0
        buy_cnt = sell_cnt = 0
        block_buy = block_sell = 0.0
        prev_px = None
        for r in self.tape_rows:
            px = safe_float(r.get("price"), 0.0)
            vol = safe_float(r.get("volume"), 0.0)
            side = str(r.get("derived_side", "")).lower()
            direction = str(r.get("direction", "")).lower()
            sign = 0
            if "buy" in side or "buy" in direction or "ask" in direction or "up" in direction:
                sign = 1
            elif "sell" in side or "sell" in direction or "bid" in direction or "down" in direction:
                sign = -1
            elif prev_px is not None:
                sign = 1 if px > prev_px else (-1 if px < prev_px else 0)
            if sign > 0:
                buy_vol += vol; buy_cnt += 1
            elif sign < 0:
                sell_vol += vol; sell_cnt += 1
            else:
                unknown_vol += vol
            dollar_value = safe_float(r.get("dollar_value"), px * vol)
            is_block = dollar_value >= 250000 or vol >= 5000 or safe_float(r.get("is_block_250k"), 0) > 0 or safe_float(r.get("is_block_5000sh"), 0) > 0
            if is_block and sign > 0: block_buy += vol
            if is_block and sign < 0: block_sell += vol
            if px > 0: prev_px = px
        return buy_vol, sell_vol, unknown_vol, buy_cnt, sell_cnt, block_buy, block_sell

    def _run_persistence(self, vals: List[float]) -> Tuple[float, float, float]:
        if not vals:
            return 0.0, 0.0, 0.0
        pos = [1 if v > 0 else 0 for v in vals]
        neg = [1 if v < 0 else 0 for v in vals]
        def longest(bits):
            best = cur = 0
            for b in bits:
                cur = cur + 1 if b else 0
                best = max(best, cur)
            return best
        return float(sum(pos) / len(vals)), float(sum(neg) / len(vals)), float(max(longest(pos), longest(neg)))

    def summary(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        buy_vol, sell_vol, unk_vol, buy_cnt, sell_cnt, block_buy, block_sell = self._signed_tape()
        total_vol = buy_vol + sell_vol + unk_vol
        delta = buy_vol - sell_vol
        out.update({
            "mf_tape_event_count": float(len(self.tape_rows)),
            "mf_push_tape_event_count": float(self.push_tape_count),
            "mf_buy_trade_count": float(buy_cnt),
            "mf_sell_trade_count": float(sell_cnt),
            "mf_buy_volume": round(buy_vol, 4),
            "mf_sell_volume": round(sell_vol, 4),
            "mf_unknown_volume": round(unk_vol, 4),
            "mf_total_tape_volume": round(total_vol, 4),
            "mf_tape_delta_volume": round(delta, 4),
            "mf_tape_delta_ratio": round(delta / total_vol, 6) if total_vol > 0 else 0.0,
            "mf_block_buy_volume": round(block_buy, 4),
            "mf_block_sell_volume": round(block_sell, 4),
            "mf_block_delta_volume": round(block_buy - block_sell, 4),
        })
        prices = [safe_float(r.get("price"), np.nan) for r in self.tape_rows]
        sizes = [safe_float(r.get("volume"), np.nan) for r in self.tape_rows]
        out["mf_trade_price_std"] = round(self._safe_stat(prices, "std"), 6)
        out["mf_trade_size_mean"] = round(self._safe_stat(sizes, "mean"), 4)
        out["mf_trade_size_max"] = round(self._safe_stat(sizes, "max"), 4)

        l2imb = [safe_float(s.get("moomoo_l2_imbalance"), np.nan) for s in self.l2_snaps]
        top3 = [safe_float(s.get("moomoo_l2_top3_imbalance"), np.nan) for s in self.l2_snaps]
        top10 = [safe_float(s.get("moomoo_l2_top10_imbalance"), np.nan) for s in self.l2_snaps]
        spread = [safe_float(s.get("moomoo_spread_pct"), np.nan) for s in self.l2_snaps]
        pos_persist, neg_persist, longest_run = self._run_persistence(top3)
        out.update({
            "mf_l2_poll_count": float(len(self.l2_snaps)),
            "mf_l2_level_rows": float(len(self.l2_rows)),
            "mf_push_orderbook_event_count": float(self.push_orderbook_count),
            "mf_push_quote_event_count": float(self.push_quote_count),
            "mf_push_kline_event_count": float(self.push_kline_count),
            "mf_l2_imbalance_mean": round(self._safe_stat(l2imb, "mean"), 6),
            "mf_l2_imbalance_min": round(self._safe_stat(l2imb, "min"), 6),
            "mf_l2_imbalance_max": round(self._safe_stat(l2imb, "max"), 6),
            "mf_l2_imbalance_std": round(self._safe_stat(l2imb, "std"), 6),
            "mf_l2_top3_imbalance_mean": round(self._safe_stat(top3, "mean"), 6),
            "mf_l2_top3_imbalance_min": round(self._safe_stat(top3, "min"), 6),
            "mf_l2_top3_imbalance_max": round(self._safe_stat(top3, "max"), 6),
            "mf_l2_top3_positive_persistence": round(pos_persist, 6),
            "mf_l2_top3_negative_persistence": round(neg_persist, 6),
            "mf_l2_top3_longest_same_side_run": round(longest_run, 2),
            "mf_l2_top10_imbalance_mean": round(self._safe_stat(top10, "mean"), 6),
            "mf_spread_pct_mean": round(self._safe_stat(spread, "mean"), 6),
            "mf_spread_pct_max": round(self._safe_stat(spread, "max"), 6),
        })
        bid_top3 = [safe_float(s.get("moomoo_bid_top3"), np.nan) for s in self.l2_snaps]
        ask_top3 = [safe_float(s.get("moomoo_ask_top3"), np.nan) for s in self.l2_snaps]
        out["mf_bid_top3_mean"] = round(self._safe_stat(bid_top3, "mean"), 4)
        out["mf_ask_top3_mean"] = round(self._safe_stat(ask_top3, "mean"), 4)
        out["mf_inside_depth_bias"] = round((out["mf_bid_top3_mean"] - out["mf_ask_top3_mean"]) / max(out["mf_bid_top3_mean"] + out["mf_ask_top3_mean"], 1e-9), 6)
        return out




class IBKRFastBarFeed:
    """IBKR 1-minute TRADES bars for the frozen V2 price/peer state.

    Historical subscriptions use keepUpToDate=True so the fast trading path does not wait
    for Moomoo polling or the 433-column recorder. Only CLOSED RTH bars are exposed.
    """
    def __init__(self, executor, symbols: List[str], log_file: Path):
        self.executor = executor
        self.ib = executor.ib
        self.log_file = log_file
        self.symbols = list(dict.fromkeys([str(x).upper() for x in symbols]))
        self.contracts: Dict[str, Any] = {}
        self.bars: Dict[str, Any] = {}
        # Timing audit: wall/perf timestamp captured by the IBKR bar update callback.
        self.bar_callback_wall: Dict[str, Dict[str, datetime]] = {}
        self.bar_callback_perf: Dict[str, Dict[str, float]] = {}
        self._bar_handlers: Dict[str, Any] = {}

    def start(self) -> None:
        for sym in self.symbols:
            c = self.executor.contract if sym == self.executor.symbol else Stock(sym, "SMART", "USD")
            if sym != self.executor.symbol:
                self.ib.qualifyContracts(c)
            self.contracts[sym] = c
            try:
                b = self.ib.reqHistoricalData(
                    c, endDateTime="", durationStr="1 D", barSizeSetting="1 min",
                    whatToShow="TRADES", useRTH=True, formatDate=2, keepUpToDate=True)
                self.bars[sym] = b
                self.bar_callback_wall.setdefault(sym, {})
                self.bar_callback_perf.setdefault(sym, {})
                def _on_bar_update(bars, has_new_bar, _sym=sym):
                    # keepUpToDate=True: when a new bar appears, bars[-2] is the bar that just closed.
                    try:
                        if not has_new_bar or len(bars) < 2:
                            return
                        cb_wall = now_ny(); cb_perf = time.perf_counter()
                        closed_bar = bars[-2]
                        dt = getattr(closed_bar, "date", None)
                        if dt is None:
                            return
                        ts = pd.Timestamp(dt)
                        if ts.tzinfo is None:
                            ts = ts.tz_localize(NY_TZ).tz_convert("UTC")
                        else:
                            ts = ts.tz_convert("UTC")
                        key = ts.isoformat()
                        self.bar_callback_wall[_sym][key] = cb_wall
                        self.bar_callback_perf[_sym][key] = cb_perf
                        # Bound diagnostics to the current session.
                        if len(self.bar_callback_wall[_sym]) > 500:
                            oldest = next(iter(self.bar_callback_wall[_sym]))
                            self.bar_callback_wall[_sym].pop(oldest, None); self.bar_callback_perf[_sym].pop(oldest, None)
                    except Exception as e:
                        log(self.log_file, f"FAST_IBKR_BAR_CALLBACK warning {_sym}: {e}")
                b.updateEvent += _on_bar_update
                self._bar_handlers[sym] = _on_bar_update
                log(self.log_file, f"FAST_IBKR_BARS subscribed {sym} rows={len(b)} callbackTiming=ON")
            except Exception as e:
                log(self.log_file, f"FAST_IBKR_BARS subscribe failed {sym}: {e}")
        self.ib.sleep(0.5)

    def recover_symbol(self, sym: str) -> bool:
        """Recreate only the stalled historical-data subscription; never disconnect execution."""
        if not self.ib.isConnected():
            log(self.log_file, f"FAST_IBKR_RECOVERY_DEFERRED {sym} broker disconnected")
            return False
        old=self.bars.get(sym)
        try:
            if old is not None:
                handler=self._bar_handlers.get(sym)
                if handler is not None: old.updateEvent -= handler
                self.ib.cancelHistoricalData(old)
            c=self.contracts[sym]
            new=self.ib.reqHistoricalData(c,endDateTime="",durationStr="1 D",barSizeSetting="1 min",
                whatToShow="TRADES",useRTH=True,formatDate=2,keepUpToDate=True)
            self.bars[sym]=new
            def on_update(bars, has_new_bar, _sym=sym):
                if has_new_bar and len(bars)>=2:
                    ts=pd.Timestamp(bars[-2].date)
                    ts=ts.tz_localize(NY_TZ).tz_convert("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
                    self.bar_callback_wall[_sym][ts.isoformat()]=now_ny()
                    self.bar_callback_perf[_sym][ts.isoformat()]=time.perf_counter()
            new.updateEvent += on_update
            self._bar_handlers[sym]=on_update
            log(self.log_file,f"FAST_IBKR_RECOVERY_SUBSCRIBED {sym} rows={len(new)}")
            return True
        except Exception as e:
            log(self.log_file,f"FAST_IBKR_RECOVERY_FAILED {sym}: {e}")
            return False

    def stop(self) -> None:
        for b in list(self.bars.values()):
            try: self.ib.cancelHistoricalData(b)
            except Exception: pass

    @staticmethod
    def _df_from_bars(bars: Any) -> pd.DataFrame:
        rows=[]
        for b in list(bars or []):
            dt=getattr(b,"date",None)
            if dt is None: continue
            ts=pd.Timestamp(dt)
            if ts.tzinfo is None:
                ts=ts.tz_localize(NY_TZ).tz_convert("UTC")
            else:
                ts=ts.tz_convert("UTC")
            rows.append({"date":ts,"open":safe_float(getattr(b,"open",0)),"high":safe_float(getattr(b,"high",0)),
                         "low":safe_float(getattr(b,"low",0)),"close":safe_float(getattr(b,"close",0)),
                         "volume":safe_float(getattr(b,"volume",0)),
                         "turnover":safe_float(getattr(b,"average",0))*safe_float(getattr(b,"volume",0))})
        if not rows: return pd.DataFrame()
        return pd.DataFrame(rows).sort_values("date").drop_duplicates("date",keep="last")

    def closed(self, sym: str, asof: Optional[datetime]=None) -> pd.DataFrame:
        df=self._df_from_bars(self.bars.get(sym))
        if df.empty: return df
        floor=pd.Timestamp(rth_minute_floor(asof).astimezone(pytz.UTC))
        return df[df["date"] < floor].copy()

    def latest_closed_key(self, sym: str, asof: Optional[datetime]=None) -> Optional[str]:
        """Cheap new-minute detector; avoids rebuilding pandas histories between closes."""
        bars=self.bars.get(sym)
        if not bars: return None
        floor=pd.Timestamp(rth_minute_floor(asof).astimezone(pytz.UTC))
        for b in reversed(list(bars)[-4:]):
            dt=getattr(b,"date",None)
            if dt is None: continue
            ts=pd.Timestamp(dt)
            if ts.tzinfo is None: ts=ts.tz_localize(NY_TZ).tz_convert("UTC")
            else: ts=ts.tz_convert("UTC")
            if ts < floor: return ts.isoformat()
        return None

    @staticmethod
    def _ret_n(df: pd.DataFrame, n: int) -> float:
        if df is None or len(df) <= n: return 0.0
        c=pd.to_numeric(df["close"],errors="coerce").dropna()
        if len(c)<=n or safe_float(c.iloc[-n-1])<=0: return 0.0
        return (safe_float(c.iloc[-1])/safe_float(c.iloc[-n-1])-1.0)*100.0

    @staticmethod
    def _ret_n_lag(df: pd.DataFrame, n: int, lag: int) -> float:
        """n-bar return ending `lag` closed bars before the latest closed bar.

        V2.1 uses this only for QQQ fresh-synchronization research authority.
        Example n=5, lag=3: close[-4] / close[-9] - 1.
        """
        if df is None or n <= 0 or lag < 0:
            return 0.0
        c=pd.to_numeric(df.get("close"),errors="coerce").dropna()
        needed=n+lag+1
        if len(c)<needed:
            return 0.0
        end_idx=-1-lag
        base_idx=end_idx-n
        end_px=safe_float(c.iloc[end_idx],0.0); base_px=safe_float(c.iloc[base_idx],0.0)
        if end_px<=0 or base_px<=0:
            return 0.0
        return (end_px/base_px-1.0)*100.0

    def callback_timing(self, sym: str, minute_key: str) -> Tuple[Optional[datetime], Optional[float]]:
        return (self.bar_callback_wall.get(sym, {}).get(minute_key),
                self.bar_callback_perf.get(sym, {}).get(minute_key))

    def _closed_bar_rows(self, sym: str, asof: Optional[datetime]=None) -> List[Dict[str, float]]:
        """Lightweight closed-RTH bar view for the trading thread; no pandas history rebuild."""
        bars = self.bars.get(sym)
        if not bars:
            return []
        floor = pd.Timestamp(rth_minute_floor(asof).astimezone(pytz.UTC))
        rows = []
        for b in list(bars):
            dt = getattr(b, "date", None)
            if dt is None:
                continue
            ts = pd.Timestamp(dt)
            if ts.tzinfo is None:
                ts = ts.tz_localize(NY_TZ).tz_convert("UTC")
            else:
                ts = ts.tz_convert("UTC")
            if ts >= floor:
                continue
            ny = ts.tz_convert(NY_TZ)
            if not (RTH_START <= ny.time() < RTH_END):
                continue
            rows.append({"date": ts, "open": safe_float(getattr(b,"open",0)),
                         "high": safe_float(getattr(b,"high",0)), "low": safe_float(getattr(b,"low",0)),
                         "close": safe_float(getattr(b,"close",0)), "volume": safe_float(getattr(b,"volume",0)),
                         "average": safe_float(getattr(b,"average",0))})
        return rows

    @staticmethod
    def _ret_rows(rows: List[Dict[str,float]], n: int, lag: int=0) -> float:
        if n <= 0 or lag < 0 or len(rows) < n + lag + 1:
            return 0.0
        end = rows[-1-lag]["close"]; base = rows[-1-lag-n]["close"]
        return (end/base-1.0)*100.0 if end > 0 and base > 0 else 0.0

    def p7_trend_cleanliness(self, symbol: str, asof: Optional[datetime]=None) -> Optional[float]:
        """Same 90-row directional/path definition as FeatureEngine.live_features.

        Returns None for fewer than two valid completed RTH bars; never substitutes
        a favourable default. Uses the independent IBKR completed-bar stream.
        """
        rows = self._closed_bar_rows(symbol, asof)
        closes = [safe_float(r.get("close"), 0.0) for r in rows[-90:]]
        if len(closes) < 2 or any(x <= 0 for x in closes):
            return None
        rets = [(closes[i]/closes[i-1]-1.0) for i in range(1,len(closes))]
        path = sum(abs(r) for r in rets)
        return clamp(abs(sum(rets))/max(path,1e-9)*100.0)

    def fast_row(self, feat_engine: FeatureEngine, asof: Optional[datetime]=None,
                 l2_context: Optional[Dict[str,Any]]=None) -> Tuple[Optional[str], Dict[str,Any]]:
        """Minimal exact V2/V2.1 decision row. Full 433-feature work remains async."""
        rows = self._closed_bar_rows(self.executor.symbol, asof)
        if not rows:
            return None, {}
        latest = rows[-1]["date"]
        day_key = latest.tz_convert(NY_TZ).date()
        day = [r for r in rows if r["date"].tz_convert(NY_TZ).date() == day_key]
        r = day[-1]
        close = r["close"]
        vols = [x["volume"] for x in day]
        typical = [(x["high"]+x["low"]+x["close"])/3.0 for x in day]
        cum_vol = sum(vols); cum_pv = sum(t*v for t,v in zip(typical,vols))
        vwap = cum_pv/cum_vol if cum_vol > 0 else close
        day_high = max(x["high"] for x in day); day_low = min(x["low"] for x in day)
        day_range = day_high-day_low
        day_pos = ((close-day_low)/day_range*100.0) if day_range > 0 else 50.0
        dist_vwap = (close/vwap-1.0)*100.0 if vwap > 0 else 0.0
        above=[]; run_pv=0.0; run_vol=0.0
        for x in day:
            xv=x["volume"]; xt=(x["high"]+x["low"]+x["close"])/3.0
            run_pv += xt*xv; run_vol += xv
            xvwap = run_pv/run_vol if run_vol > 0 else x["close"]
            above.append(1.0 if x["close"] > xvwap else 0.0)
        tail20 = above[-20:]
        above20 = (sum(tail20)/len(tail20)*100.0) if len(tail20) >= 5 else 50.0
        f: Dict[str,Any] = {
            "bar_time_utc": latest.isoformat(), "bar_time_ny": latest.tz_convert(NY_TZ).isoformat(),
            "feature_minute_key_utc": latest.isoformat(), "open": r["open"], "high": r["high"],
            "low": r["low"], "close": close, "volume": r["volume"], "vwap": vwap,
            "day_position": day_pos, "dist_vwap_pct": dist_vwap,
            "drop_from_high_pct": (close/day_high-1.0)*100.0 if day_high > 0 else 0.0,
            "above_vwap_ratio_20": above20,
            "stock_ret_5m": self._ret_rows(rows,5), "slope_5": self._ret_rows(rows,5),
        }
        # Keep the other slope names available for recorder parity/diagnostics at negligible cost.
        for n in (3,10,20,60): f[f"slope_{n}"] = self._ret_rows(rows,n)
        peer_rows = {sym:self._closed_bar_rows(sym,asof) for sym in self.symbols if sym != self.executor.symbol}
        qqq_rows=peer_rows.get("QQQ",[]); qqq5=self._ret_rows(qqq_rows,5); spy5=self._ret_rows(peer_rows.get("SPY",[]),5)
        f["qqq_ret_5m"]=qqq5; f["qqq_ret_5m_lag3"]=self._ret_rows(qqq_rows,5,3); f["spy_ret_5m"]=spy5
        f["rs_qqq_5m"]=f["stock_ret_5m"]-qqq5; f["rs_spy_5m"]=f["stock_ret_5m"]-spy5
        for sym,prows in peer_rows.items():
            f[f"peer_{sym.lower()}_ret_1m"]=self._ret_rows(prows,1)
            f[f"peer_{sym.lower()}_ret_5m"]=self._ret_rows(prows,5)
            f[f"peer_{sym.lower()}_lead_vs_smci_5m"]=f[f"peer_{sym.lower()}_ret_5m"]-f["stock_ret_5m"]
        q=self.executor.fresh_l1(wait_sec=0.0)
        bid=safe_float(q.get("bid"),0); ask=safe_float(q.get("ask"),0); last=safe_float(q.get("last"),close)
        f.update({"ibkr_available":1.0 if q.get("ok") else 0.0,"ibkr_bid":bid,"ibkr_ask":ask,"ibkr_last":last,
                  "ibkr_bid_size":safe_float(q.get("bid_size"),0),"ibkr_ask_size":safe_float(q.get("ask_size"),0),
                  "ibkr_snapshot_time_ny":q.get("quote_ts_ny",ts_iso()),"ibkr_spread":ask-bid if ask>0 and bid>0 else 0.0,
                  "ibkr_mid":(ask+bid)/2 if ask>0 and bid>0 else 0.0,
                  "ibkr_spread_pct":((ask-bid)/last*100.0) if last>0 and ask>bid>0 else 0.0})
        if l2_context: f.update(l2_context)
        return latest.isoformat(), f


class AsyncMoomooSampler:
    """Continuously collect rich Moomoo research data without blocking the fast IBKR path."""
    def __init__(self, moo: MoomooRecorder, feat_engine: FeatureEngine, args, executor, log_file: Path):
        self.moo=moo; self.feat_engine=feat_engine; self.args=args; self.executor=executor; self.log_file=log_file
        self.lock=threading.RLock(); self.stop_event=threading.Event(); self.thread=None
        self.raw_k=pd.DataFrame(); self.snap={}; self.l2={}; self.tape={}; self.cf={}; self.acc=MinuteFeatureAccumulator()
        self.minute_acc={}; self.last_moo_success=0.0; self.reconnect_after=0.0
    def start(self):
        self.thread=threading.Thread(target=self._run,name="moomoo-recorder",daemon=True); self.thread.start()
    def stop(self):
        self.stop_event.set()
        if self.thread: self.thread.join(timeout=3.0)
    def _minute_key(self, ts=None):
        dt=parse_any_ts_ny(ts) if ts is not None else now_ny()
        return dt.replace(second=0,microsecond=0).isoformat() if dt else None

    def _acc_for(self,key):
        if key not in self.minute_acc: self.minute_acc[key]=MinuteFeatureAccumulator()
        return self.minute_acc[key]

    def pop_minute(self,minute_key):
        dt=parse_any_ts_ny(minute_key)
        key=self._minute_key(dt) if dt else None
        with self.lock:
            acc=self.minute_acc.pop(key,None)
            # Retain late arriving buckets for a short period; no shared reset.
            cutoff=now_ny()-timedelta(minutes=8)
            for old in list(self.minute_acc):
                d=parse_any_ts_ny(old)
                if d and d<cutoff: del self.minute_acc[old]
        if acc is None: return {k: "" for k in MinuteFeatureAccumulator().summary()}
        result=acc.summary()
        if not acc.tape_rows:
            for k in result:
                if k.startswith("mf_") and ("tape" in k or "trade" in k or "volume" in k or "block" in k): result[k]=""
        if not acc.l2_snaps:
            for k in result:
                if k.startswith("mf_") and ("l2" in k or "depth" in k or "spread" in k): result[k]=""
        return result

    def _run(self):
        failures=0
        while not self.stop_event.is_set():
            try:
                if not self.moo.connected and time.time()>=self.reconnect_after:
                    self.moo.close()
                    self.moo.connect()
                    self.reconnect_after=time.time()+min(120,5*2**min(failures,4))
                raw=self.moo.get_live_1m(count=self.args.kline_count)
                snap=self.moo.get_snapshot()
                l2,l2rows=self.moo.get_l2()
                tape,taperows=self.moo.get_tape(num=self.args.ticker_count)
                push=self.moo.drain_push_rows()
                cf=self.moo.get_capital_flow()
                key=self._minute_key()
                valid_l2=(safe_float(l2.get("moomoo_l2_available"),0)==1 and len(l2rows)>0)
                valid_tape=bool(taperows or push.get("tape"))
                with self.lock:
                    if raw is not None and not raw.empty: self.raw_k=raw.copy()
                    if snap: self.snap=dict(snap)
                    if l2: self.l2=dict(l2)
                    if valid_l2:
                        self.l2_polled_wall=time.time()
                        acc=self._acc_for(key);acc.add_l2_snapshot(l2);acc.add_l2_rows(l2rows)
                    if tape: self.tape=dict(tape)
                    if valid_tape: self.tape_polled_wall=time.time()
                    if cf: self.cf=dict(cf)
                    if snap: self._acc_for(key).add_snapshot(snap)
                    # Each tick is assigned to its own observed minute, not the writer's minute.
                    for row in list(taperows)+list(push.get("tape",[])):
                        rk=self._minute_key(row.get("ts"))
                        if rk: self._acc_for(rk).add_tape_rows([row])
                    for typ in ("orderbook","quote","kline"):
                        for row in push.get(typ,[]):
                            rk=self._minute_key(row.get("ts"))
                            if rk:
                                ac=self._acc_for(rk)
                                if typ=="orderbook":ac.push_orderbook_count+=1
                                elif typ=="quote":ac.push_quote_count+=1
                                else:ac.push_kline_count+=1
                    if valid_tape or valid_l2: self.last_moo_success=time.time();failures=0
                if time.time()-self.last_moo_success>90 and time.time()>=self.reconnect_after:
                    failures+=1;self.moo.close();self.moo.connected=False
                    self.reconnect_after=time.time()+min(120,5*2**min(failures,4))
                    log(self.log_file,f"MOOMOO_RECOVERY_REQUEST failures={failures} retryAt={self.reconnect_after:.0f}")
            except Exception as e:
                failures+=1
                log(self.log_file,f"MOOMOO_SAMPLER warning: {e}")
                if failures>=3:
                    self.moo.connected=False
            self.stop_event.wait(max(0.5,float(self.args.poll_seconds)))
    def fast_context(self) -> Dict[str,Any]:
        """Small cached context for the trading thread; deliberately does NOT copy raw_k."""
        with self.lock:
            return {"l2":dict(self.l2),"acc":self.acc.summary(), "l2_polled_wall": getattr(self, "l2_polled_wall", 0.0)}

    def fast_tape_context(self) -> Dict[str, Any]:
        with self.lock:
            return {k:self.tape.get(k) for k in ("moomoo_buy_burst_score", "moomoo_sell_burst_score") if k in self.tape}

    def tape_health(self) -> Dict[str, Any]:
        # Observed SDK push or successful polling, never fabricated from L2 state.
        with self.lock:
            last = max(getattr(self, "tape_polled_wall", 0.0),
                       getattr(self.moo, "last_smci_tape_push_wall", 0.0))
        age = time.time() - last if last > 0 else float("inf")
        return {"fresh": bool(0 <= age <= 30.0), "age_sec": age}

    def snapshot(self) -> Dict[str,Any]:
        # Full recorder snapshot. Only the async feature-writer should call this in steady state.
        with self.lock:
            return {"raw_k":self.raw_k.copy(),"snap":dict(self.snap),"l2":dict(self.l2),"tape":dict(self.tape),"cf":dict(self.cf),"acc":self.acc.summary()}
    def reset_acc(self):
        with self.lock: self.acc.reset()

class AsyncFeatureWriter:
    """Recorder-only worker. Heavy pandas/Moomoo/CSV work never runs on the V2 trading thread."""
    def __init__(self, symbol, peers, feat_engine, moo, moo_sampler, executor, feature_file, decision_file, log_file):
        self.symbol=symbol; self.peers=list(peers); self.feat_engine=feat_engine; self.moo=moo
        self.moo_sampler=moo_sampler; self.executor=executor; self.feature_file=feature_file
        self.decision_file=decision_file; self.log_file=log_file
        self.gate_audit_file = decision_file.with_name(decision_file.name.replace("_decisions.csv", "_v21_gate_audit.csv"))
        self.q=queue.Queue(); self.stop_event=threading.Event(); self.thread=None
        self.pending={}; self.written=set(); self.state_lock=threading.RLock(); self.last_feature_row={}; self._last_schema_audit=None; self._last_missing_summary=None

    def start(self):
        self.thread=threading.Thread(target=self._run,name="feature-writer",daemon=True); self.thread.start()

    def submit(self, minute_key: str, item: Dict[str,Any]) -> None:
        self.q.put((minute_key, item))

    def pending_count(self) -> int:
        return self.q.qsize()+len(self.pending)

    def latest_row(self) -> Dict[str,Any]:
        with self.state_lock: return dict(self.last_feature_row)

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=180.0)
            if self.thread.is_alive():
                log(self.log_file, f"FEATURE_WRITER_DRAIN_TIMEOUT pending={self.pending_count()} "
                    "WARNING: some final feature rows may be missing")

    def _run(self):
        while not self.stop_event.is_set() or not self.q.empty() or self.pending:
            try:
                while True:
                    pk,item=self.q.get_nowait(); self.pending[pk]=item
            except queue.Empty:
                pass
            if not self.pending:
                self.stop_event.wait(0.05); continue
            try:
                sampler=self.moo_sampler.snapshot()
                raw_k=sampler.get("raw_k",pd.DataFrame())
                moo_bars=filter_rth_bars(self.feat_engine.prepare_moomoo_bars(raw_k)) if raw_k is not None and not raw_k.empty else pd.DataFrame()
                for pk in list(self.pending.keys()):
                    target=pd.Timestamp(pk)
                    available=(not moo_bars.empty and (pd.to_datetime(moo_bars["date"],utc=True)==target).any())
                    pdt=parse_any_ts_ny(pk)
                    overdue=bool(pdt and (now_ny()-(pdt+timedelta(minutes=1))).total_seconds()>75)
                    if not available and not overdue and not self.stop_event.is_set(): continue
                    item=self.pending.pop(pk); fastbase=item["fast_f"]
                    if available:
                        upto=moo_bars[pd.to_datetime(moo_bars["date"],utc=True)<=target].copy()
                        mf=self.feat_engine.live_features(self.feat_engine.add_features(upto)) if not upto.empty else {}
                    else:
                        mf={}
                    mf.update(self.moo.get_peer_tape_context(lookback_sec=45.0)); mf.update(self.moo.get_push_buffer_health()); mf.update(fastbase)
                    ibsnap=dict(item.get("ibsnap",{}))
                    row=build_row(self.symbol,mf,sampler.get("snap",{}),sampler.get("l2",{}),sampler.get("tape",{}),sampler.get("cf",{}),ibsnap)
                    minute_stats=self.moo_sampler.pop_minute(pk)
                    row.update(minute_stats)
                    if minute_stats.get("mf_l2_poll_count", "") in ("",0,0.0):
                        row["moomoo_l2_available"]=0.0
                        for field in FEATURE_FIELDS:
                            if field.startswith("moomoo_l2_") and field!="moomoo_l2_available":row[field]=""
                    if minute_stats.get("mf_tape_event_count", "") in ("",0,0.0):
                        row["moomoo_tape_available"]=0.0
                        for field in FEATURE_FIELDS:
                            if field.startswith("moomoo_tape_") and field!="moomoo_tape_available":row[field]=""
                    row["feature_row_type"]="CLOSED_1M_RTH_COMPACT"; row["feature_minute_key_utc"]=pk; row["feature_emit_ts_ny"]=ts_iso()
                    row["stock_ret_5m"]=safe_float(fastbase.get("stock_ret_5m")); row["qqq_ret_5m"]=safe_float(fastbase.get("qqq_ret_5m")); row["spy_ret_5m"]=safe_float(fastbase.get("spy_ret_5m"))
                    for ps in self.peers:
                        for suffix in ["ret_1m","ret_5m","lead_vs_smci_5m"]:
                            k=f"peer_{ps.lower()}_{suffix}"; row[k]=safe_float(fastbase.get(k),safe_float(row.get(k),0.0))
                    for k in ["open","high","low","close","vwap","day_position","dist_vwap_pct","slope_3","slope_5","slope_10","slope_20","slope_60","bar_time_utc","bar_time_ny"]:
                        if k in fastbase: row[k]=fastbase[k]
                    row["last"]=first_positive(ibsnap.get("ibkr_last"),fastbase.get("close")); row["bid"]=safe_float(ibsnap.get("ibkr_bid")); row["ask"]=safe_float(ibsnap.get("ibkr_ask")); row["spread"]=max(0.0,row["ask"]-row["bid"])
                    row.pop("ibkr_spread_pct",None)
                    decision_row=add_decision_columns(row,item["meta"],item["action"],item["reason"],item["exec_state"])
                    decision_row["decision_latency_ms"]=item["decision_latency_ms"]; decision_row["decision_calc_ms"]=item["decision_calc_ms"]
                    decision_row["bar_observed_ms"]=item.get("bar_observed_ms",0.0); decision_row["callback_delay_ms"]=item.get("callback_delay_ms",-1.0); decision_row["callback_to_observed_ms"]=item.get("callback_to_observed_ms",-1.0); decision_row["fast_row_ms"]=item.get("fast_row_ms",0.0)
                    decision_row["state_ms"]=item.get("state_ms",0.0); decision_row["execution_ms"]=item.get("execution_ms",0.0)
                    missing = [k for k in FEATURE_FIELDS if k not in row]
                    extra = sorted(set(row) - set(FEATURE_FIELDS))
                    schema_sig=(tuple(missing), tuple(extra))
                    if schema_sig != self._last_schema_audit:
                        audit_feature_schema(row,self.log_file)
                        self._last_schema_audit=schema_sig
                    append_csv(self.feature_file,row,fieldnames=FEATURE_FIELDS)
                    append_csv(self.decision_file,decision_row,fieldnames=DECISION_FIELDS)
                    if missing and tuple(missing) != self._last_missing_summary:
                        log(self.log_file, f"FEATURE_MISSING_VALUES minute={pk} blank_count={len(missing)} first_fields={missing[:12]} (blank cells preserved)")
                        self._last_missing_summary=tuple(missing)
                    gate_meta = item.get("meta", {})
                    audit = {"minute_key":pk, "action":item["action"], "reason":item["reason"],
                             "brain_entry_state":gate_meta.get("entry_state", ""),
                             "brain_entry_transition":gate_meta.get("entry_transition", ""),
                             "brain_entry_state_prev":gate_meta.get("entry_state_prev", ""),
                             "brain_dynamic_regime":gate_meta.get("dynamic_regime", ""),
                             "stock_ret_5m":fastbase.get("stock_ret_5m", ""),
                             "qqq_ret_5m":fastbase.get("qqq_ret_5m", ""),
                             "qqq_ret_5m_lag3":fastbase.get("qqq_ret_5m_lag3", "")}
                    for k in V21_AUDIT_META_KEYS:
                        audit["brain_"+k] = gate_meta.get(k, "")
                    append_csv(self.gate_audit_file,audit,fieldnames=V21_AUDIT_FIELDS)
                    self.written.add(pk)
                    with self.state_lock: self.last_feature_row=dict(row)
                    # Per-minute accumulator already consumed; never reset shared data.
                    log(self.log_file,f"FEATURE_MINUTE_WRITTEN_V3_ASYNC minute={pk} source={'MOOMOO+IBKR' if available else 'IBKR+CACHED_MOOMOO_FALLBACK'} cols={len(FEATURE_FIELDS)}")
            except Exception as e:
                log(self.log_file,f"FEATURE_WRITER warning: {e}\n{traceback.format_exc()}")
            self.stop_event.wait(0.05)

# ============================================================
# SMCI 15-SESSION TRIGGER BRAIN V2 + DYNAMIC REGIME + OPENING RANGE + IBKR EXECUTION
# ============================================================
# Research basis (frozen architecture):
#   Discovery block:   2026-07-06 .. 2026-07-10
#   Validation block:  2026-07-13 .. 2026-07-17
#   Validation block:  2026-07-20 .. 2026-07-24 (Jul-23 partial)
#
# Core observations intentionally translated into authority:
#   1) sector/market peer regime first (QQQ/NVDA/SMH)
#   2) SMCI 5m impulse persistence
#   3) entry location: avoid extreme chase / bad VWAP extension
#   4) synchronized weakness = long veto / exit
#   5) positive L2 displayed bids are NOT a standalone long signal
#   6) tape/control/block metrics are context/telemetry, not standalone authority
#
# Lead/lag features are computed and logged, but by default remain confidence
# modifiers rather than hard gates because the 15-session study did not establish
# a stable standalone threshold for them.

DEFAULT_SMCI_RET_5M_MIN = 0.40
DEFAULT_CORE_PEER_MIN = 0.01
DEFAULT_CORE_PEER_AVG_MIN = 0.08
DEFAULT_MIN_POSITIVE_CORE_PEERS = 2
DEFAULT_DAY_POSITION_MAX = 95.0
DEFAULT_DIST_VWAP_MIN = -1.00
DEFAULT_DIST_VWAP_MAX = 2.50
DEFAULT_SLOPE5_MIN = 0.0

DEFAULT_EARLY_EXIT_SMCI_RET_5M = 0.0
DEFAULT_BREAKDOWN_SMCI_RET_5M = -0.05
DEFAULT_BREAKDOWN_MIN_NEG_PEERS = 2
DEFAULT_HARD_STOP_PCT = 0.50
DEFAULT_TRAIL_ARM_PCT = 0.80
DEFAULT_TRAIL_GIVEBACK_PCT = 0.35
DEFAULT_MAX_SPREAD_PCT = 0.20
DEFAULT_L2_TRAP_POSITIVE = 0.15

# V2.1 frozen research gate: fresh QQQ synchronization + acceptable structure.
DEFAULT_V21_QQQ_5M_LAG3_MAX = 0.03
DEFAULT_V21_ABOVE_VWAP_RATIO_20_MIN = 50.0
DEFAULT_V21_RECOVERY_DIST_VWAP_MIN = -0.75
DEFAULT_V21_RECOVERY_DAY_POSITION_MIN = 25.0
DEFAULT_V21_RECOVERY_DROP_FROM_HIGH_MIN = -2.70

NO_NEW_ENTRY_AFTER = dtime(15, 45)
FORCE_FLATTEN_AFTER = dtime(15, 50)
TARGET_CANCEL_AFTER = dtime(15, 49, 45)
SLOPE20_LIMIT = -0.7135
SLOPE5_LIMIT = 0.0


def _pct_ret(current: float, base: float) -> float:
    if current <= 0 or base <= 0:
        return 0.0
    return (current / base - 1.0) * 100.0


def _is_valid_price(x: Any) -> bool:
    v = safe_float(x, 0.0)
    return v > 0 and math.isfinite(v)


TRADING_EVENT_FIELDS = ['ts_ny', 'event_type', 'symbol', 'mode', 'execution_state', 'position_qty', 'avg_cost', 'strategy_equity', 'strategy_equity_floor', 'realized_pnl', 'trades_taken', 'open_symbol_orders', 'halted', 'halt_reason', 'action', 'aon', 'avg_fill_price', 'commission', 'filled', 'intended_qty', 'limit_price', 'message', 'order_id', 'order_ref', 'qty', 'reason', 'remaining', 'status']

ORDER_STATUS_LEDGER_FIELDS = ['ts_ny','symbol','order_id','perm_id','client_id','order_ref','purpose','strategy_trade_id','status','filled','remaining','avg_fill_price','last_fill_price','why_held','position_qty','open_symbol_orders','message']
EXECUTION_LEDGER_FIELDS = ['ts_ny','symbol','exec_id','order_id','perm_id','order_ref','purpose','strategy_trade_id','side','shares','price','cum_qty','avg_price','exchange','exec_time','position_qty','message']

class IBKRExecutionEngine:
    """IBKR paper/live execution truth with strict full-size-or-zero policy.

    Strategy entry and normal strategy exit orders use All-Or-None (AON).
    The engine never accepts a partial strategy position as a valid steady state.
    If an unexpected partial/orphan state occurs, an emergency flatten-to-zero
    repair is attempted and new trading is operationally halted for inspection.

    Protective catastrophe stops are deliberately NOT AON: risk safety has
    priority over the strategy's AON preference. Any partial protective-stop fill
    is immediately reconciled toward zero and logged as an execution anomaly.
    """
    def __init__(self, symbol: str, host: str, port: int, client_id: int, log_file: Path,
                 event_file: Path, mode: str, live_ack: bool,
                 account_capital: float, capital_usage_pct: float,
                 strategy_equity_floor: float,
                 hard_stop_pct: float, trail_arm_pct: float, trail_giveback_pct: float,
                 entry_timeout_sec: float, aon_retries: int, max_trades: int,
                 max_daily_loss_usd: float):
        self.symbol = symbol.upper()
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.log_file = log_file
        self.event_file = event_file
        self.mode = mode.lower().strip()
        self.live_ack = bool(live_ack)
        self.initial_strategy_capital = float(account_capital)
        self.capital_usage_pct = float(capital_usage_pct)
        self.strategy_equity_floor = float(strategy_equity_floor)
        self.hard_stop_pct = float(hard_stop_pct)
        self.trail_arm_pct = float(trail_arm_pct)
        self.trail_giveback_pct = float(trail_giveback_pct)
        self.entry_timeout_sec = float(entry_timeout_sec)
        self.aon_retries = max(1, int(aon_retries))
        self.max_trades = int(max_trades)
        self.max_daily_loss_usd = float(max_daily_loss_usd)
        self.marketable_limit_allowance_bps = 8.0
        self.ib = None
        self.contract = None
        self.ticker = None
        self.connected = False
        self.active_stop_trade = None
        self.target_trade = None
        self.active_trade: Optional[Dict[str, Any]] = None
        self.trade_counter = 0
        self.realized_pnl = 0.0
        self.execution_state = "DISCONNECTED"
        self.operational_halt = False
        self.halt_reason = ""

        self.order_registry: Dict[int, Dict[str, Any]] = {}
        self.processed_exec_ids = set()
        self._registry_lock = threading.RLock()
        self._intent_counter = 0
        self.current_strategy_trade_id = ""
        stem = self.event_file.stem.replace("_trading_events", "")
        self.order_status_ledger_file = self.event_file.with_name(f"{stem}_ibkr_order_status_ledger.csv")
        self.execution_ledger_file = self.event_file.with_name(f"{stem}_ibkr_execution_ledger.csv")
        self._callbacks_registered = False
        self._protective_watchdog_last_ok_ts = 0.0

    @property
    def strategy_equity(self) -> float:
        return self.initial_strategy_capital + self.realized_pnl

    def _event(self, event_type: str, **kw) -> None:
        row = {k: "" for k in TRADING_EVENT_FIELDS}
        row.update({
            "ts_ny": ts_iso(), "event_type": event_type, "symbol": self.symbol,
            "mode": self.mode, "execution_state": self.execution_state,
            "position_qty": self.position_qty() if self.connected else 0,
            "avg_cost": self.position_avg_cost() if self.connected else 0.0,
            "strategy_equity": round(self.strategy_equity, 4),
            "strategy_equity_floor": round(self.strategy_equity_floor, 4),
            "realized_pnl": round(self.realized_pnl, 4),
            "trades_taken": self.trade_counter,
            "open_symbol_orders": self.open_symbol_order_count() if self.connected else 0,
            "halted": int(self.operational_halt or self.strategy_equity <= self.strategy_equity_floor),
            "halt_reason": self.halt_reason,
        })
        row.update({k: v for k, v in kw.items() if k in row})
        append_csv(self.event_file, row, fieldnames=TRADING_EVENT_FIELDS)

    def _next_strategy_trade_id(self, phase: str = "TRADE") -> str:
        self._intent_counter += 1
        return f"{self.symbol}_{today_str()}_{phase}_{self._intent_counter:04d}"

    def _registry_info(self, order_id: int) -> Dict[str, Any]:
        with self._registry_lock:
            return dict(self.order_registry.get(int(order_id or 0), {}))

    def _register_order(self, trade, purpose: str, strategy_trade_id: str = "") -> None:
        try:
            order = trade.order
            oid = int(getattr(order, "orderId", 0) or 0)
            if oid <= 0:
                return
            with self._registry_lock:
                old = self.order_registry.get(oid, {})
                self.order_registry[oid] = {
                    "order_id": oid,
                    "perm_id": int(getattr(order, "permId", 0) or old.get("perm_id", 0) or 0),
                    "client_id": int(getattr(order, "clientId", self.client_id) or self.client_id),
                    "order_ref": str(getattr(order, "orderRef", "") or ""),
                    "purpose": purpose,
                    "strategy_trade_id": strategy_trade_id or self.current_strategy_trade_id,
                    "status": _trade_status(trade),
                    "exec_filled_qty": int(old.get("exec_filled_qty", 0) or 0),
                    "exec_notional": float(old.get("exec_notional", 0.0) or 0.0),
                    "processed_exec_ids": set(old.get("processed_exec_ids", set()) or set()),
                    "registered_ts": old.get("registered_ts", ts_iso()),
                }
            self._event("ORDER_REGISTERED", order_id=oid,
                        order_ref=str(getattr(order, "orderRef", "") or ""),
                        status=_trade_status(trade), reason=purpose,
                        message=f"strategy_trade_id={strategy_trade_id or self.current_strategy_trade_id}")
        except Exception as e:
            log(self.log_file, f"order registry warning: {e}")

    def _registry_exec_stats(self, order_id: int) -> Tuple[int, float]:
        with self._registry_lock:
            rec = self.order_registry.get(int(order_id or 0), {})
            qty = int(rec.get("exec_filled_qty", 0) or 0)
            notional = float(rec.get("exec_notional", 0.0) or 0.0)
        return qty, (notional / qty if qty > 0 and notional > 0 else 0.0)

    def _on_order_status_event(self, trade) -> None:
        """Audit callback only: records facts and never submits/cancels orders."""
        try:
            order, st = trade.order, trade.orderStatus
            oid = int(getattr(order, "orderId", 0) or 0)
            info = self._registry_info(oid)
            with self._registry_lock:
                if oid in self.order_registry:
                    self.order_registry[oid]["status"] = str(getattr(st, "status", "") or "")
                    perm = int(getattr(order, "permId", 0) or 0)
                    if perm:
                        self.order_registry[oid]["perm_id"] = perm
            append_csv(self.order_status_ledger_file, {
                "ts_ny": ts_iso(), "symbol": self.symbol, "order_id": oid,
                "perm_id": int(getattr(order, "permId", 0) or info.get("perm_id", 0) or 0),
                "client_id": int(getattr(order, "clientId", self.client_id) or self.client_id),
                "order_ref": str(getattr(order, "orderRef", "") or info.get("order_ref", "")),
                "purpose": info.get("purpose", "UNREGISTERED"),
                "strategy_trade_id": info.get("strategy_trade_id", ""),
                "status": str(getattr(st, "status", "") or ""),
                "filled": safe_float(getattr(st, "filled", 0.0), 0.0),
                "remaining": safe_float(getattr(st, "remaining", 0.0), 0.0),
                "avg_fill_price": safe_float(getattr(st, "avgFillPrice", 0.0), 0.0),
                "last_fill_price": safe_float(getattr(st, "lastFillPrice", 0.0), 0.0),
                "why_held": str(getattr(st, "whyHeld", "") or ""),
                "position_qty": self.position_qty() if self.connected else 0,
                "open_symbol_orders": self.open_symbol_order_count() if self.connected else 0,
                "message": "ASYNC_ORDER_STATUS",
            }, fieldnames=ORDER_STATUS_LEDGER_FIELDS)
        except Exception as e:
            log(self.log_file, f"orderStatus callback warning: {e}")

    def _on_exec_details_event(self, trade, fill) -> None:
        """Deduplicated fill callback using immutable IBKR execId."""
        try:
            ex = fill.execution
            exec_id = str(getattr(ex, "execId", "") or "")
            if not exec_id:
                return
            with self._registry_lock:
                if exec_id in self.processed_exec_ids:
                    return
                self.processed_exec_ids.add(exec_id)
            oid = int(getattr(ex, "orderId", 0) or getattr(trade.order, "orderId", 0) or 0)
            shares = int(round(abs(safe_float(getattr(ex, "shares", 0.0), 0.0))))
            price = safe_float(getattr(ex, "price", 0.0), 0.0)
            with self._registry_lock:
                rec = self.order_registry.setdefault(oid, {
                    "order_id": oid, "perm_id": int(getattr(trade.order, "permId", 0) or 0),
                    "client_id": self.client_id, "order_ref": str(getattr(trade.order, "orderRef", "") or ""),
                    "purpose": "UNREGISTERED", "strategy_trade_id": "", "status": _trade_status(trade),
                    "exec_filled_qty": 0, "exec_notional": 0.0, "processed_exec_ids": set(),
                    "registered_ts": ts_iso(),
                })
                seen = rec.setdefault("processed_exec_ids", set())
                if exec_id not in seen:
                    seen.add(exec_id)
                    rec["exec_filled_qty"] = int(rec.get("exec_filled_qty", 0) or 0) + shares
                    rec["exec_notional"] = float(rec.get("exec_notional", 0.0) or 0.0) + shares * price
                info = dict(rec)
            append_csv(self.execution_ledger_file, {
                "ts_ny": ts_iso(), "symbol": self.symbol, "exec_id": exec_id, "order_id": oid,
                "perm_id": int(getattr(trade.order, "permId", 0) or info.get("perm_id", 0) or 0),
                "order_ref": str(getattr(trade.order, "orderRef", "") or info.get("order_ref", "")),
                "purpose": info.get("purpose", "UNREGISTERED"),
                "strategy_trade_id": info.get("strategy_trade_id", ""),
                "side": str(getattr(ex, "side", "") or ""), "shares": shares, "price": price,
                "cum_qty": safe_float(getattr(ex, "cumQty", 0.0), 0.0),
                "avg_price": safe_float(getattr(ex, "avgPrice", 0.0), 0.0),
                "exchange": str(getattr(ex, "exchange", "") or ""),
                "exec_time": str(getattr(ex, "time", "") or ""),
                "position_qty": self.position_qty() if self.connected else 0,
                "message": "ASYNC_EXECUTION_DEDUPED",
            }, fieldnames=EXECUTION_LEDGER_FIELDS)
        except Exception as e:
            log(self.log_file, f"execDetails callback warning: {e}")

    def _register_ibkr_callbacks(self) -> None:
        if self._callbacks_registered or self.ib is None:
            return
        self.ib.orderStatusEvent += self._on_order_status_event
        self.ib.execDetailsEvent += self._on_exec_details_event
        self._callbacks_registered = True
        log(self.log_file, "IBKR async audit callbacks registered.")

    def connect(self) -> bool:
        if IB is None or Stock is None or MarketOrder is None:
            log(self.log_file, "ib_insync/order classes unavailable; execution disabled.")
            return False
        if self.mode == "live" and not self.live_ack:
            raise RuntimeError("LIVE mode blocked: pass --i-understand-live explicitly.")
        try:
            self.ib = IB()
            self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=8)
            self._register_ibkr_callbacks()
            self.contract = Stock(self.symbol, "SMART", "USD")
            self.ib.qualifyContracts(self.contract)
            # V3: streaming IBKR L1 is the execution-price authority.
            # Moomoo remains a rich research source (L2/tape/capital-flow) only.
            self.ticker = self.ib.reqMktData(self.contract, "", False, False)
            self.ib.sleep(0.25)
            self.connected = True
            self.execution_state = "FLAT_CONFIRMED" if self.position_qty() == 0 else "POSITION_PRESENT"
            self._event("IBKR_CONNECTED", message=f"{self.host}:{self.port} clientId={self.client_id}")
            log(self.log_file, f"IBKR EXECUTION connected mode={self.mode} {self.host}:{self.port} clientId={self.client_id}")
            return True
        except Exception as e:
            log(self.log_file, f"IBKR execution connect failed: {e}")
            self.connected = False
            self.execution_state = "DISCONNECTED"
            return False

    def close(self):
        try:
            self._event("IBKR_DISCONNECTING")
        except Exception:
            pass
        try:
            if self.ib:
                self.ib.disconnect()
        except Exception:
            pass
        self.connected = False
        self.execution_state = "DISCONNECTED"

    def snapshot(self) -> Dict[str, Any]:
        """IBKR L1 + broker-state snapshot. V3 uses fresh IBKR L1 for execution pricing."""
        out = {
            "ibkr_available": 0.0,
            "ibkr_bid": 0.0, "ibkr_ask": 0.0, "ibkr_last": 0.0, "ibkr_close": 0.0,
            "ibkr_bid_size": 0.0, "ibkr_ask_size": 0.0, "ibkr_last_size": 0.0,
            "ibkr_volume": 0.0, "ibkr_high": 0.0, "ibkr_low": 0.0,
            "ibkr_spread": 0.0, "ibkr_mid": 0.0,
            "ibkr_snapshot_time_ny": ts_iso(),
        }
        if not self.connected or self.ib is None:
            return out
        try:
            self.ib.sleep(0.1)
            if self.ticker is not None:
                bid = safe_float(getattr(self.ticker, "bid", 0.0))
                ask = safe_float(getattr(self.ticker, "ask", 0.0))
                last = safe_float(getattr(self.ticker, "last", 0.0))
                close = safe_float(getattr(self.ticker, "close", 0.0))
                # ibkr_available means a usable L1 quote is present, not merely that the
                # broker socket is connected. Broker/account fields below remain available
                # even while market-data permissions are warming up.
                market_valid = any(v > 0 for v in (bid, ask, last, close))
                out.update({
                    "ibkr_available": 1.0 if market_valid else 0.0,
                    "ibkr_bid": bid,
                    "ibkr_ask": ask,
                    "ibkr_last": last,
                    "ibkr_close": close,
                    "ibkr_bid_size": safe_float(getattr(self.ticker, "bidSize", 0.0)),
                    "ibkr_ask_size": safe_float(getattr(self.ticker, "askSize", 0.0)),
                    "ibkr_last_size": safe_float(getattr(self.ticker, "lastSize", 0.0)),
                    "ibkr_volume": safe_float(getattr(self.ticker, "volume", 0.0)),
                    "ibkr_high": safe_float(getattr(self.ticker, "high", 0.0)),
                    "ibkr_low": safe_float(getattr(self.ticker, "low", 0.0)),
                    "ibkr_spread": ask - bid if ask > 0 and bid > 0 else 0.0,
                    "ibkr_mid": (ask + bid) / 2.0 if ask > 0 and bid > 0 else 0.0,
                    "ibkr_snapshot_time_ny": ts_iso(),
                })
            out.update({
                "ibkr_position_qty": self.position_qty(),
                "strategy_equity": self.strategy_equity,
                "strategy_equity_floor": "",
                "strategy_halted": int(self.operational_halt or self.strategy_equity <= self.strategy_equity_floor),
            })
        except Exception as e:
            log(self.log_file, f"IBKR diagnostic/broker-state snapshot failed: {e}")
        return out

    def snapshot_nowait(self) -> Dict[str, Any]:
        """Read the already-streaming IBKR L1/broker cache without sleeping or making a request.

        Recorder-only helper. It must never delay the V2 decision path.
        """
        out = {
            "ibkr_available": 0.0,
            "ibkr_bid": 0.0, "ibkr_ask": 0.0, "ibkr_last": 0.0, "ibkr_close": 0.0,
            "ibkr_bid_size": 0.0, "ibkr_ask_size": 0.0, "ibkr_last_size": 0.0,
            "ibkr_volume": 0.0, "ibkr_high": 0.0, "ibkr_low": 0.0,
            "ibkr_spread": 0.0, "ibkr_mid": 0.0,
            "ibkr_snapshot_time_ny": ts_iso(),
        }
        try:
            if self.ticker is not None:
                bid = safe_float(getattr(self.ticker, "bid", 0.0))
                ask = safe_float(getattr(self.ticker, "ask", 0.0))
                last = safe_float(getattr(self.ticker, "last", 0.0))
                close = safe_float(getattr(self.ticker, "close", 0.0))
                out.update({
                    "ibkr_available": 1.0 if any(v > 0 for v in (bid, ask, last, close)) else 0.0,
                    "ibkr_bid": bid, "ibkr_ask": ask, "ibkr_last": last, "ibkr_close": close,
                    "ibkr_bid_size": safe_float(getattr(self.ticker, "bidSize", 0.0)),
                    "ibkr_ask_size": safe_float(getattr(self.ticker, "askSize", 0.0)),
                    "ibkr_last_size": safe_float(getattr(self.ticker, "lastSize", 0.0)),
                    "ibkr_volume": safe_float(getattr(self.ticker, "volume", 0.0)),
                    "ibkr_high": safe_float(getattr(self.ticker, "high", 0.0)),
                    "ibkr_low": safe_float(getattr(self.ticker, "low", 0.0)),
                    "ibkr_spread": ask - bid if ask > 0 and bid > 0 else 0.0,
                    "ibkr_mid": (ask + bid) / 2.0 if ask > 0 and bid > 0 else 0.0,
                })
            out.update({
                "ibkr_position_qty": self.position_qty(),
                "strategy_equity": self.strategy_equity,
                "strategy_equity_floor": "",
                "strategy_halted": int(self.operational_halt or self.strategy_equity <= self.strategy_equity_floor),
            })
        except Exception as e:
            log(self.log_file, f"IBKR nowait snapshot warning: {e}")
        return out

    def position_qty(self) -> int:
        if not self.connected or self.ib is None or self.contract is None:
            return 0
        try:
            for p in self.ib.positions():
                if getattr(p.contract, "conId", 0) == getattr(self.contract, "conId", -1):
                    return int(round(float(p.position)))
            return 0
        except Exception as e:
            log(self.log_file, f"position truth failed: {e}")
            return 0

    def position_avg_cost(self) -> float:
        if not self.connected or self.ib is None or self.contract is None:
            return 0.0
        try:
            for p in self.ib.positions():
                if getattr(p.contract, "conId", 0) == getattr(self.contract, "conId", -1):
                    return safe_float(getattr(p, "avgCost", 0.0), 0.0)
        except Exception:
            pass
        return 0.0

    def open_symbol_trades(self):
        if not self.connected or self.ib is None or self.contract is None:
            return []
        out = []
        try:
            for tr in list(self.ib.openTrades()):
                c = getattr(tr, "contract", None)
                if c is not None and getattr(c, "conId", 0) == getattr(self.contract, "conId", -1):
                    out.append(tr)
        except Exception as e:
            log(self.log_file, f"openTrades truth failed: {e}")
        return out

    def open_symbol_order_count(self) -> int:
        return len(self.open_symbol_trades())

    def strategy_halted(self) -> Tuple[bool, str]:
        return (bool(self.operational_halt), self.halt_reason or "OPERATIONAL_HALT")

    def can_trade(self) -> Tuple[bool, str]:
        if not self.connected or self.ib is None or not self.ib.isConnected():
            return False, "IBKR_NOT_CONNECTED"
        halted, why = self.strategy_halted()
        if halted:
            return False, why
        if self.trade_counter >= self.max_trades:
            return False, "MAX_TRADES_REACHED"
        if self.execution_state not in {"FLAT_CONFIRMED"}:
            return False, f"EXECUTION_STATE_{self.execution_state}"
        if self.position_qty() != 0:
            return False, "POSITION_NOT_ZERO"
        if self.open_symbol_order_count() != 0:
            return False, "OPEN_SYMBOL_ORDER_EXISTS"
        return True, "OK"

    @staticmethod
    def _commission_from_trade(trade) -> float:
        total = 0.0
        try:
            for fill in list(getattr(trade, "fills", []) or []):
                cr = getattr(fill, "commissionReport", None)
                if cr is not None:
                    total += safe_float(getattr(cr, "commission", 0.0), 0.0)
        except Exception:
            pass
        return total

    def _wait_trade(self, trade, timeout: float) -> Tuple[float, int, str]:
        """Wait only for broker-terminal states; combine status, Trade fills and exec ledger."""
        start = time.time()
        while time.time() - start < timeout:
            self.ib.sleep(0.10)
            status = _trade_status(trade)
            oid = int(getattr(trade.order, "orderId", 0) or 0)
            filled_status = int(round(safe_float(getattr(trade.orderStatus, "filled", 0.0), 0.0)))
            filled_exec = _trade_filled_from_execs(trade)
            filled_ledger, avg_ledger = self._registry_exec_stats(oid)
            filled = max(filled_status, filled_exec, filled_ledger)
            avg_status = safe_float(getattr(trade.orderStatus, "avgFillPrice", 0.0), 0.0)
            avg_exec = _trade_avg_fill_from_execs(trade)
            avg = avg_ledger if avg_ledger > 0 else (avg_exec if avg_exec > 0 else avg_status)
            if status in IBKR_TERMINAL_STATES:
                return avg, filled, status
        status = _trade_status(trade)
        oid = int(getattr(trade.order, "orderId", 0) or 0)
        filled_status = int(round(safe_float(getattr(trade.orderStatus, "filled", 0.0), 0.0)))
        filled_exec = _trade_filled_from_execs(trade)
        filled_ledger, avg_ledger = self._registry_exec_stats(oid)
        filled = max(filled_status, filled_exec, filled_ledger)
        avg_status = safe_float(getattr(trade.orderStatus, "avgFillPrice", 0.0), 0.0)
        avg_exec = _trade_avg_fill_from_execs(trade)
        avg = avg_ledger if avg_ledger > 0 else (avg_exec if avg_exec > 0 else avg_status)
        return avg, filled, status

    def _cancel_trade(self, trade, why: str = "", timeout: float = 5.0) -> Tuple[float, int, str]:
        """Request cancellation and wait until the order is truly terminal.

        If the order fills while cancellation is pending, that Fill is broker truth and
        is returned to the caller; no retry may be submitted until this returns.
        """
        try:
            status_before = _trade_status(trade)
            if status_before not in IBKR_TERMINAL_STATES:
                self.ib.cancelOrder(trade.order)
            avg, filled, status = self._wait_trade(trade, timeout)
            self._event("ORDER_CANCEL", order_id=getattr(trade.order, "orderId", ""),
                        action=getattr(trade.order, "action", ""), qty=getattr(trade.order, "totalQuantity", ""),
                        filled=filled, status=status, avg_fill_price=avg, reason=why)
            return avg, filled, status
        except Exception as e:
            log(self.log_file, f"cancel order warning: {e}")
            return 0.0, 0, _trade_status(trade)

    def _cancel_all_symbol_orders(self, exclude_trade=None, timeout: float = 8.0) -> bool:
        """Cancel every open SMCI order and wait for terminal confirmation."""
        if not self.connected or self.ib is None:
            return False
        trades = []
        for tr in self.open_symbol_trades():
            if exclude_trade is not None and tr is exclude_trade:
                continue
            trades.append(tr)
            try:
                if _trade_status(tr) not in IBKR_TERMINAL_STATES:
                    self.ib.cancelOrder(tr.order)
                    self._event("ORDER_CANCEL_REQUEST", order_id=getattr(tr.order, "orderId", ""),
                                action=getattr(tr.order, "action", ""), qty=getattr(tr.order, "totalQuantity", ""),
                                status=_trade_status(tr), reason="CANCEL_ALL_SYMBOL_ORDERS")
            except Exception:
                pass

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.ib.sleep(0.10)
            unresolved = [tr for tr in trades if _trade_status(tr) not in IBKR_TERMINAL_STATES]
            if not unresolved:
                return self.open_symbol_order_count() == 0
        return self.open_symbol_order_count() == 0

    def _shares_for_price(self, px: float) -> int:
        """Maximum whole shares using current compounded strategy equity."""
        if px <= 0:
            return 0
        budget = max(0.0, self.strategy_equity * self.capital_usage_pct)
        qty = max(0, int(budget // px))
        # Reserve a realistic fixed-plan commission estimate for capacity only.
        while qty > 0:
            est_comm = max(1.0, qty * 0.005)
            if qty * px + est_comm <= budget:
                break
            qty -= 1
        return qty

    @staticmethod
    def _set_aon(order, order_ref: str = ""):
        order.allOrNone = True
        order.tif = "DAY"
        order.outsideRth = False
        if order_ref:
            order.orderRef = order_ref
        return order

    def fresh_l1(self, wait_sec: float = 0.35) -> Dict[str, Any]:
        """Return the freshest in-memory IBKR L1 immediately before an execution decision.

        No feature-row snapshot is reused. The short sleep only yields to ib_insync so pending
        socket updates can be processed; it is not a polling/data-construction delay.
        """
        if not self.connected or self.ib is None or self.ticker is None:
            return {"ok": False, "error": "IBKR_L1_UNAVAILABLE"}
        self.ib.sleep(max(0.0, min(float(wait_sec), 0.50)))
        bid = safe_float(getattr(self.ticker, "bid", 0.0), 0.0)
        ask = safe_float(getattr(self.ticker, "ask", 0.0), 0.0)
        last = safe_float(getattr(self.ticker, "last", 0.0), 0.0)
        now = now_ny()
        # ib_insync Ticker.time is the last market-data update timestamp when supplied by IBKR.
        qtime = getattr(self.ticker, "time", None)
        qdt = parse_any_ts_ny(qtime) if qtime is not None else None
        age_ms = max(0.0, (now - qdt).total_seconds() * 1000.0) if qdt else 0.0
        quote_ok = bool((bid > 0 and ask > 0) or last > 0)
        if qdt is not None and age_ms > 3000.0:
            quote_ok = False
        return {
            "ok": quote_ok,
            "bid": bid, "ask": ask, "last": last,
            "bid_size": safe_float(getattr(self.ticker, "bidSize", 0.0), 0.0),
            "ask_size": safe_float(getattr(self.ticker, "askSize", 0.0), 0.0),
            "quote_ts_ny": qdt.isoformat() if qdt else now.isoformat(),
            "quote_age_ms": age_ms,
        }

    def _place_aon_marketable_limit(self, action: str, qty: int, timeout: float,
                                    order_ref: str, purpose: str, strategy_trade_id: str,
                                    allowance_bps: float = 8.0):
        """Bounded AON marketable LIMIT using a fresh IBKR quote on every attempt."""
        q = self.fresh_l1()
        if not q.get("ok") or (q.get("ask", 0) <= 0 and q.get("bid", 0) <= 0):
            return None, 0.0, 0, "NO_FRESH_IBKR_L1", q
        side_px = q["ask"] if action.upper() == "BUY" else q["bid"]
        if side_px <= 0:
            side_px = q.get("last", 0.0)
        tick = 0.01
        allowance = max(tick, side_px * max(0.0, allowance_bps) / 10000.0)
        limit_px = side_px + allowance if action.upper() == "BUY" else max(tick, side_px - allowance)
        limit_px = round(limit_px + (1e-9 if action.upper()=="BUY" else -1e-9), 2)
        order = LimitOrder(action, qty, limit_px, tif="DAY", outsideRth=False)
        self._set_aon(order, order_ref)
        tr = self.ib.placeOrder(self.contract, order)
        self._register_order(tr, purpose, strategy_trade_id)
        self._event("ORDER_SUBMIT", order_id=getattr(order, "orderId", ""), order_ref=order_ref,
                    action=action, qty=qty, intended_qty=qty, status=getattr(tr.orderStatus, "status", ""),
                    aon=1, limit_price=limit_px, reason=purpose,
                    message=f"FRESH_IBKR bid={q['bid']:.4f} ask={q['ask']:.4f} last={q['last']:.4f} ageMs={q['quote_age_ms']:.1f}")
        avg, filled, status = self._wait_trade(tr, timeout)
        self._event("ORDER_STATUS", order_id=getattr(order, "orderId", ""), order_ref=order_ref,
                    action=action, qty=qty, intended_qty=qty, filled=filled, remaining=max(0,qty-filled),
                    status=status, aon=1, limit_price=limit_px, avg_fill_price=avg, reason=purpose)
        return tr, avg, filled, status, q

    def _place_aon_market(self, action: str, qty: int, timeout: float,
                          order_ref: str, purpose: str = "AON_STRATEGY_ORDER",
                          strategy_trade_id: str = ""):
        """Submit a full-size All-or-None MARKET order.

        Moomoo remains the strategy/market-data authority. IBKR market quotes are not
        required to construct the order. The actual IBKR execution fill price is the
        authoritative entry/exit price recorded by the engine.
        """
        order = MarketOrder(action, qty, tif="DAY", outsideRth=False)
        self._set_aon(order, order_ref)
        tr = self.ib.placeOrder(self.contract, order)
        self._register_order(tr, purpose, strategy_trade_id)
        self._event("ORDER_SUBMIT", order_id=getattr(order, "orderId", ""),
                    order_ref=order_ref, action=action, qty=qty, intended_qty=qty,
                    status=getattr(tr.orderStatus, "status", ""), aon=1,
                    limit_price="", reason=purpose,
                    message="AON MARKET order; IBKR actual fill price is execution truth")
        avg, filled, status = self._wait_trade(tr, timeout)
        remaining = max(0, qty - filled)
        self._event("ORDER_STATUS", order_id=getattr(order, "orderId", ""),
                    order_ref=order_ref, action=action, qty=qty, intended_qty=qty,
                    filled=filled, remaining=remaining, status=status, aon=1,
                    limit_price="", avg_fill_price=avg, reason=purpose)
        return tr, avg, filled, status

    def _broker_flat_stable(self, checks: int = 2, interval: float = 0.50) -> bool:
        """Require repeated broker snapshots showing zero position and zero open symbol orders."""
        for _ in range(max(1, checks)):
            self.ib.sleep(interval)
            if self.position_qty() != 0 or self.open_symbol_order_count() != 0:
                return False
        return True

    def _emergency_flatten_to_zero(self, reason: str, max_attempts: int = 1) -> Dict[str, Any]:
        """Serialized safety repair: cancel first, then at most one live repair order at a time.

        If the broker cannot prove a stable zero state, halt and do not chase the position
        with repeated overlapping repair orders.
        """
        self.execution_state = "EMERGENCY_FLATTEN"
        fills = []

        # First kill any strategy orders and wait for terminal confirmation.
        self._cancel_all_symbol_orders(timeout=8.0)
        self.ib.sleep(0.25)

        qty = self.position_qty()
        if qty != 0:
            action = "SELL" if qty > 0 else "BUY"
            order = MarketOrder(action, abs(qty), tif="DAY", outsideRth=False)
            order.orderRef = "SMCI_EMERGENCY_ZERO_1"
            tr = self.ib.placeOrder(self.contract, order)
            self._register_order(tr, "EMERGENCY_REPAIR", self.current_strategy_trade_id)
            self._event("EMERGENCY_ORDER_SUBMIT", order_id=getattr(order, "orderId", ""),
                        order_ref=order.orderRef, action=action, qty=abs(qty), intended_qty=abs(qty),
                        aon=0, reason=reason)

            avg, filled, status = self._wait_trade(tr, 10.0)
            fills.append((action, filled, avg, self._commission_from_trade(tr)))
            self._event("EMERGENCY_ORDER_STATUS", order_id=getattr(order, "orderId", ""),
                        order_ref=order.orderRef, action=action, qty=abs(qty), intended_qty=abs(qty),
                        filled=filled, status=status, avg_fill_price=avg, aon=0, reason=reason,
                        commission=self._commission_from_trade(tr))

            # If still not terminal, request cancel and wait. Do not submit another repair order.
            if status not in IBKR_TERMINAL_STATES:
                avg2, filled2, status2 = self._cancel_trade(tr, "EMERGENCY_REPAIR_CANCEL", timeout=8.0)
                avg = avg2 if avg2 > 0 else avg
                filled = max(filled, filled2)
                status = status2

        # Require stable flat/no-orders broker truth.
        ok = self._broker_flat_stable(checks=2, interval=0.50)
        final_qty = self.position_qty()
        total_filled = sum(int(x[1]) for x in fills if int(x[1]) > 0)
        weighted_notional = sum(float(x[1]) * float(x[2]) for x in fills if int(x[1]) > 0 and float(x[2]) > 0)
        avg_fill = weighted_notional / total_filled if total_filled > 0 and weighted_notional > 0 else 0.0
        total_commission = sum(float(x[3]) for x in fills)

        if ok:
            self.execution_state = "FLAT_CONFIRMED"
        else:
            self.execution_state = "UNRESOLVED_POSITION"
            self.operational_halt = True
            self.halt_reason = (
                f"UNRESOLVED_BROKER_STATE_AFTER_SINGLE_REPAIR:"
                f"pos={final_qty}:openOrders={self.open_symbol_order_count()}"
            )
        log(self.log_file, f"EMERGENCY_FLATTEN reason={reason} final_qty={final_qty} ok={ok} avg={avg_fill:.4f} commission={total_commission:.2f}")
        return {"ok": ok, "final_qty": final_qty, "fills": fills,
                "avg_fill_price": avg_fill, "commission": total_commission}

    def ensure_flat_and_no_orders(self, reason: str = "RECONCILE_FLAT") -> bool:
        """Return True only after broker proves a stable zero position and zero open SMCI orders."""
        self.execution_state = "RECONCILING"
        cancelled = self._cancel_all_symbol_orders(timeout=8.0)
        self.ib.sleep(0.25)

        qty = self.position_qty()
        if qty != 0:
            res = self._emergency_flatten_to_zero(reason)
            if not res["ok"]:
                return False

        # Final cancel sweep in case a late callback resurrected an order.
        self._cancel_all_symbol_orders(timeout=8.0)
        ok = self._broker_flat_stable(checks=2, interval=0.50)

        self.execution_state = "FLAT_CONFIRMED" if ok else "RECONCILE_FAILED"
        if not ok:
            self.operational_halt = True
            self.halt_reason = (
                f"FLAT_RECONCILIATION_FAILED:"
                f"pos={self.position_qty()}:openOrders={self.open_symbol_order_count()}"
            )
        return ok

    def place_protective_stop(self, qty: int, entry_px: float) -> float:
        raise RuntimeError("PROTECTIVE_STOPS_DISABLED_IN_SLOPE20")

    def _protective_stop_active(self, intended_qty: int = 0) -> Tuple[bool, str]:
        if not self.connected or self.ib is None:
            return False, "IBKR_NOT_CONNECTED"
        stop_oid = int(getattr(getattr(self.active_stop_trade, "order", None), "orderId", 0) or 0)
        candidates = []
        try:
            for tr in list(self.ib.openTrades()):
                c = getattr(tr, "contract", None)
                if c is None or getattr(c, "conId", 0) != getattr(self.contract, "conId", -1):
                    continue
                order = tr.order
                oid = int(getattr(order, "orderId", 0) or 0)
                info = self._registry_info(oid)
                is_stop = (oid == stop_oid or info.get("purpose") == "PROTECTIVE_STOP"
                           or str(getattr(order, "orderRef", "") or "").startswith("SMCI_PROTECTIVE_STOP"))
                if not is_stop:
                    continue
                status = _trade_status(tr)
                action = str(getattr(order, "action", "") or "").upper()
                oqty = int(round(safe_float(getattr(order, "totalQuantity", 0.0), 0.0)))
                if action == "SELL" and status in {"PendingSubmit", "PreSubmitted", "Submitted"}:
                    candidates.append((tr, oqty, status))
        except Exception as e:
            return False, f"STOP_SCAN_ERROR:{e}"
        if len(candidates) != 1:
            return False, f"PROTECTIVE_STOP_COUNT_{len(candidates)}"
        _, oqty, status = candidates[0]
        if intended_qty > 0 and oqty != intended_qty:
            return False, f"PROTECTIVE_STOP_QTY_MISMATCH:{oqty}!={intended_qty}"
        return True, status

    def protective_stop_watchdog(self) -> bool:
        return True  # Slope20 has no protective stop.

    def ensure_target(self) -> bool:
        """Place a single native +1% DAY limit SELL using actual IBKR fill price."""
        if not self.active_trade or not self.connected or now_ny().time() >= TARGET_CANCEL_AFTER:
            return False
        qty = self.position_qty()
        if qty <= 0: return False
        if self.target_trade is not None:
            status = _trade_status(self.target_trade)
            if status not in IBKR_TERMINAL_STATES: return True
            if status == "Filled": return True  # reconciliation handles the resulting flat state
            self.operational_halt=True
            self.halt_reason=f"TARGET_NOT_WORKING:{status}"
            self._event("SLOPE20_TARGET_FAILURE", reason=self.halt_reason, halted=1)
            return False
        # Never create a second SELL if any SELL is already pending at broker.
        for tr in self.open_symbol_trades():
            if str(getattr(tr.order,"action","")).upper()=="SELL":
                self.operational_halt=True
                self.halt_reason="UNKNOWN_SELL_ORDER_PREVENTS_TARGET"
                return False
        entry = safe_float(self.active_trade.get("entry_price"),0.0)
        if entry<=0 or qty!=int(self.active_trade.get("qty",0)):
            self.operational_halt=True; self.halt_reason="TARGET_ENTRY_OR_QTY_UNVERIFIED"
            return False
        from decimal import Decimal, ROUND_UP
        target = float((Decimal(str(entry))*Decimal("1.01")/Decimal("0.01")).to_integral_value(rounding=ROUND_UP)*Decimal("0.01"))
        order = LimitOrder("SELL",qty,target,tif="DAY",outsideRth=False)
        self._set_aon(order)
        order.orderRef=f"SMCI_SLOPE20_TARGET_{self.current_strategy_trade_id}"[:64]
        self.target_trade=self.ib.placeOrder(self.contract,order)
        self._register_order(self.target_trade,"SLOPE20_TARGET",self.current_strategy_trade_id)
        self.active_trade["target_price"]=target
        self._event("SLOPE20_TARGET_SUBMIT",action="SELL",qty=qty,intended_qty=qty,limit_price=target,
                    order_id=getattr(order,"orderId",""),order_ref=order.orderRef,reason="PLUS_ONE_PERCENT_FROM_ACTUAL_FILL")
        log(self.log_file,f"SLOPE20_NATIVE_TARGET qty={qty} entry={entry:.4f} target={target:.4f}")
        return True

    def cancel_target_for_liquidation(self, timeout: float=8.0) -> Tuple[bool,str]:
        """Do not sell again until the existing target is terminal and broker truth checked."""
        tr=self.target_trade
        if tr is None:
            # An orphan SELL can still exist after reconnect; refuse conflicting exit.
            if any(str(getattr(x.order,"action","")).upper()=="SELL" for x in self.open_symbol_trades()):
                return False,"UNTRACKED_SELL_ORDER"
            return True,"NO_TARGET"
        status=_trade_status(tr)
        if status not in IBKR_TERMINAL_STATES:
            _avg,_filled,status=self._cancel_trade(tr,"SLOPE20_EOD_TARGET_CANCEL",timeout=timeout)
        if status not in IBKR_TERMINAL_STATES:
            return False,f"TARGET_NOT_TERMINAL:{status}"
        self.ib.sleep(0.10)
        if self.open_symbol_order_count():
            return False,"OTHER_OPEN_ORDERS"
        self._event("SLOPE20_TARGET_DISARMED",reason=status,position_qty=self.position_qty())
        return True,status

    def enter_long(self, reference: Dict[str, Any], reason: str) -> Dict[str, Any]:
        result = {"ok": False, "action": "BUY", "reason": reason}
        can, why = self.can_trade()
        if not can:
            result["error"] = why
            return result
        # V3: fresh IBKR L1 is the sole execution/sizing price authority.
        q0 = self.fresh_l1()
        bid = safe_float(q0.get("bid"), 0.0); ask = safe_float(q0.get("ask"), 0.0); last = safe_float(q0.get("last"), 0.0)
        px_ref = ask if ask > 0 else last
        if not q0.get("ok") or px_ref <= 0:
            result["error"] = "IBKR_PRICE_UNAVAILABLE"
            return result
        qty = self._shares_for_price(px_ref)
        if qty <= 0:
            result["error"] = "ZERO_SHARES"
            return result
        spread = max(0.0, ask-bid) if ask > 0 and bid > 0 else max(px_ref*0.0005, 0.01)
        self.execution_state = "ENTRY_PENDING"
        self.current_strategy_trade_id = self._next_strategy_trade_id("ENTRY")
        self._event("ENTRY_BEGIN", action="BUY", intended_qty=qty, qty=qty, reason=reason, aon=1,
                    message=f"strategy_trade_id={self.current_strategy_trade_id}")
        try:
            total_commission = 0.0
            for attempt in range(1, self.aon_retries+1):
                placed = self._place_aon_marketable_limit(
                    "BUY", qty, self.entry_timeout_sec, f"SMCI_AON_ENTRY_{attempt}",
                    purpose="ENTRY", strategy_trade_id=self.current_strategy_trade_id,
                    allowance_bps=self.marketable_limit_allowance_bps)
                tr, avg, filled, status, fresh_q = placed
                if tr is None:
                    result["error"] = status
                    return result
                total_commission += self._commission_from_trade(tr)
                if filled == qty and status == "Filled":
                    self.ib.sleep(0.15)
                    actual_qty = self.position_qty()
                    if actual_qty != qty:
                        self.operational_halt = True
                        self.halt_reason = f"ENTRY_BROKER_QTY_MISMATCH intended={qty} actual={actual_qty}"
                        self._event("ENTRY_QTY_MISMATCH", intended_qty=qty, position_qty=actual_qty,
                                    halted=1, halt_reason=self.halt_reason)
                        self._emergency_flatten_to_zero("ENTRY_QTY_MISMATCH")
                        result["error"] = self.halt_reason
                        return result
                    entry_px = avg if avg > 0 else self.position_avg_cost()
                    if entry_px <= 0:
                        entry_px = px_ref
                    self._pending_entry_reason = reason
                    try:
                        stop_px = 0.0
                    finally:
                        self._pending_entry_reason = ""
                    self.trade_counter += 1
                    self.execution_state = "LONG_CONFIRMED"
                    self.active_trade = {
                        "trade_no": self.trade_counter,
                        "strategy_trade_id": self.current_strategy_trade_id,
                        "entry_ts": ts_iso(), "entry_price": entry_px, "qty": qty,
                        "intended_qty": qty, "entry_reason": reason,
                        "moomoo_entry_bid": safe_float(reference.get("snapshot_bid_price"),0.0),
                        "moomoo_entry_ask": safe_float(reference.get("snapshot_ask_price"),0.0),
                        "moomoo_entry_last": safe_float(reference.get("snapshot_last_price"),0.0),
                        "moomoo_entry_reference_price": safe_float(reference.get("snapshot_ask_price"),0.0),
                        "ibkr_entry_bid": bid, "ibkr_entry_ask": ask, "ibkr_entry_last": last,
                        "ibkr_entry_fill_price": entry_px,
                        "highest": entry_px, "lowest": entry_px,
                        "mfe_pct": 0.0, "mae_pct": 0.0, "trail_armed": False,
                        "hard_stop_price": stop_px, "entry_commission": total_commission,
                        "strategy_equity_before": self.strategy_equity,
                    }
                    self._event("ENTRY_CONFIRMED_FULL", action="BUY", qty=qty, intended_qty=qty,
                                filled=qty, remaining=0, status="Filled", avg_fill_price=entry_px,
                                aon=1, commission=total_commission, reason=reason,
                                message=f"IBKR_REF bid={bid:.4f} ask={ask:.4f} last={last:.4f} ref={px_ref:.4f}; IBKR_FILL={entry_px:.4f}")
                    log(self.log_file, f"EXEC AON BUY FULL qty={qty} avg={entry_px:.4f} stop={stop_px:.4f} strategy_equity={self.strategy_equity:.2f} reason={reason}")
                    result.update({"ok": True, "qty": qty, "entry_price": entry_px,
                                   "hard_stop": stop_px, "status": status,
                                   "entry_commission": total_commission})
                    return result
                # AON should be 0 or full. Any partial is an anomaly: zero it and halt.
                # Timed out/non-terminal: request cancel and WAIT for terminal confirmation.
                cancel_avg, cancel_filled, cancel_status = self._cancel_trade(
                    tr, f"AON_ENTRY_RETRY_{attempt}", timeout=8.0
                )
                if cancel_avg > 0:
                    avg = cancel_avg
                filled = max(filled, cancel_filled)
                status = cancel_status

                # If it filled while cancellation was pending, broker truth wins.
                if status == "Filled" or filled >= qty or self.position_qty() == qty:
                    entry_px = avg if avg > 0 else self.position_avg_cost()
                    if entry_px <= 0:
                        entry_px = px_ref
                    total_commission += self._commission_from_trade(tr)
                    self._pending_entry_reason = reason
                    try:
                        stop_px = 0.0
                    finally:
                        self._pending_entry_reason = ""
                    self.trade_counter += 1
                    self.execution_state = "LONG_CONFIRMED"
                    self.active_trade = {
                        "trade_no": self.trade_counter,
                        "strategy_trade_id": self.current_strategy_trade_id,
                        "entry_ts": ts_iso(), "entry_price": entry_px, "qty": qty,
                        "intended_qty": qty, "entry_reason": reason,
                        "moomoo_entry_bid": safe_float(reference.get("snapshot_bid_price"),0.0),
                        "moomoo_entry_ask": safe_float(reference.get("snapshot_ask_price"),0.0),
                        "moomoo_entry_last": safe_float(reference.get("snapshot_last_price"),0.0),
                        "moomoo_entry_reference_price": safe_float(reference.get("snapshot_ask_price"),0.0),
                        "ibkr_entry_bid": bid, "ibkr_entry_ask": ask, "ibkr_entry_last": last,
                        "ibkr_entry_fill_price": entry_px,
                        "highest": entry_px, "lowest": entry_px,
                        "mfe_pct": 0.0, "mae_pct": 0.0, "trail_armed": False,
                        "hard_stop_price": stop_px, "entry_commission": total_commission,
                        "strategy_equity_before": self.strategy_equity,
                    }
                    self._event("ENTRY_CONFIRMED_FULL_AFTER_CANCEL_RACE", action="BUY", qty=qty,
                                intended_qty=qty, filled=qty, remaining=0, status="Filled",
                                avg_fill_price=entry_px, aon=1, commission=total_commission,
                                reason=reason)
                    result.update({"ok": True, "qty": qty, "entry_price": entry_px,
                                   "hard_stop": stop_px, "status": "Filled",
                                   "entry_commission": total_commission})
                    return result

                # Any partial/orphan state is an execution anomaly: halt and make one controlled repair.
                broker_pos = self.position_qty()
                if 0 < filled < qty or broker_pos != 0:
                    self.operational_halt = True
                    self.halt_reason = f"UNEXPECTED_PARTIAL_AON_ENTRY filled={filled}/{qty} broker_pos={broker_pos}"
                    self._event("AON_PARTIAL_ANOMALY", action="BUY", qty=qty, intended_qty=qty,
                                filled=filled, status=status, aon=1, halted=1, halt_reason=self.halt_reason)
                    self._emergency_flatten_to_zero("AON_ENTRY_PARTIAL_REPAIR")
                    result["error"] = self.halt_reason
                    return result

                # Retry is permitted only after this exact order is terminal and broker is still flat.
                if status not in IBKR_TERMINAL_STATES:
                    self.operational_halt = True
                    self.halt_reason = f"AON_ENTRY_ORDER_NOT_TERMINAL_AFTER_CANCEL:{status}"
                    result["error"] = self.halt_reason
                    return result
                if not self._broker_flat_stable(checks=2, interval=0.25):
                    self.operational_halt = True
                    self.halt_reason = "AON_ENTRY_NOT_STABLY_FLAT_BEFORE_RETRY"
                    result["error"] = self.halt_reason
                    return result

            # No full fill after retries. Only report clean abort if broker proves stable flat/no-orders.
            flat_ok = self.ensure_flat_and_no_orders("AON_ENTRY_NOT_FILLED")
            if not flat_ok:
                result["error"] = self.halt_reason or "AON_ENTRY_ABORT_RECONCILIATION_FAILED"
                return result
            result["error"] = "AON_ENTRY_NOT_FILLED_FULL_SIZE"
            self._event("ENTRY_ABORTED_ZERO", action="BUY", qty=qty, intended_qty=qty,
                        filled=0, remaining=qty, aon=1, reason=result["error"])
            return result
        except Exception as e:
            self.operational_halt = True
            self.halt_reason = f"ENTRY_EXCEPTION:{e}"
            result["error"] = str(e)
            log(self.log_file, f"ENTRY ERROR: {e}\n{traceback.format_exc()}")
            try: self.ensure_flat_and_no_orders("ENTRY_EXCEPTION_REPAIR")
            except Exception: pass
            return result

    def update_trade_excursions(self, market_px: float) -> Dict[str, Any]:
        if not self.active_trade or market_px <= 0:
            return {}
        t = self.active_trade
        t["highest"] = max(float(t.get("highest", market_px)), market_px)
        t["lowest"] = min(float(t.get("lowest", market_px)), market_px)
        entry = float(t["entry_price"])
        t["mfe_pct"] = _pct_ret(t["highest"], entry)
        t["mae_pct"] = _pct_ret(t["lowest"], entry)
        if t["mfe_pct"] >= self.trail_arm_pct:
            t["trail_armed"] = True
        current_ret = _pct_ret(market_px, entry)
        giveback = t["mfe_pct"] - current_ret
        return {"current_ret_pct": current_ret, "mfe_pct": t["mfe_pct"], "mae_pct": t["mae_pct"],
                "trail_armed": int(t["trail_armed"]), "trail_giveback_pct": giveback}

    def update_trade_excursions_bar(self, high_px: float, low_px: float, close_px: float) -> Dict[str, Any]:
        """Backtest-parity excursion update from the newly CLOSED IBKR one-minute bar."""
        if not self.active_trade:
            return {}
        t=self.active_trade; entry=float(t.get("entry_price",0.0))
        if entry <= 0: return {}
        if high_px > 0: t["highest"] = max(float(t.get("highest",entry)), high_px)
        if low_px > 0: t["lowest"] = min(float(t.get("lowest",entry)), low_px)
        t["mfe_pct"]=_pct_ret(float(t["highest"]),entry); t["mae_pct"]=_pct_ret(float(t["lowest"]),entry)
        if t["mfe_pct"] >= self.trail_arm_pct: t["trail_armed"] = True
        current_ret=_pct_ret(close_px,entry) if close_px>0 else 0.0
        giveback=t["mfe_pct"]-current_ret
        return {"current_ret_pct":current_ret,"mfe_pct":t["mfe_pct"],"mae_pct":t["mae_pct"],
                "trail_armed":int(bool(t.get("trail_armed"))),"trail_giveback_pct":giveback}

    def trailing_exit_triggered(self, market_px: float) -> Tuple[bool, Dict[str, Any]]:
        st = self.update_trade_excursions(market_px)
        if not st or not self.active_trade:
            return False, st
        return bool(self.active_trade.get("trail_armed") and st.get("trail_giveback_pct", 0.0) >= self.trail_giveback_pct), st

    def _finalize_trade_record(self, exit_px: float, exit_commission: float, reason: str,
                               exit_status: str = "Filled") -> Dict[str, Any]:
        t = dict(self.active_trade or {})
        entry_px = safe_float(t.get("entry_price"), self.position_avg_cost())
        qty = int(t.get("qty", 0))
        gross_pnl = (exit_px-entry_px)*qty
        total_commission = safe_float(t.get("entry_commission",0.0),0.0)+exit_commission
        pnl = gross_pnl-total_commission
        self.realized_pnl += pnl
        if getattr(self,"equity_state_file",None):
            try:
                state_path=Path(self.equity_state_file)
                state_path.parent.mkdir(parents=True,exist_ok=True)
                state={"equity":self.strategy_equity,"last_trade_id":t.get("strategy_trade_id",""),"updated_ny":ts_iso()}
                tmp=state_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(state,indent=2),encoding="utf-8")
                tmp.replace(state_path)
            except Exception as e:
                self.operational_halt=True
                self.halt_reason=f"EQUITY_PERSISTENCE_FAILED:{e}"
                log(self.log_file,f"CRITICAL {self.halt_reason}")
        record = {
            **t, "exit_ts": ts_iso(), "exit_price": exit_px, "exit_reason": reason,
            "gross_pnl": gross_pnl, "commission": total_commission, "pnl": pnl,
            "return_pct": _pct_ret(exit_px, entry_px), "exit_status": exit_status,
            "strategy_equity_after": self.strategy_equity,
            "strategy_equity_floor": "",
            "trading_halted_after": int(self.operational_halt),
        }
        self.active_trade = None
        self.active_stop_trade = None
        self.target_trade = None
        self.execution_state = "FLAT_CONFIRMED"
        self._event("TRADE_CLOSED", action="SELL", qty=qty, filled=qty, remaining=0,
                    avg_fill_price=exit_px, status=exit_status, commission=total_commission,
                    reason=reason, message=f"pnl={pnl:.2f} return={record['return_pct']:.4f}%")
        return record

    def sync_and_reconcile(self, market_px: float = 0.0) -> Optional[Dict[str, Any]]:
        """Continuous broker-truth reconciliation.

        Returns a closed trade record if an external/protective event made us flat.
        Unexpected partial/orphan states are repaired toward zero and trading is halted.
        """
        if not self.connected:
            return None
        # Once execution safety has hard-halted, do not generate repeated automatic repair orders.
        # Broker state is still observed/logged, but exposure-changing actions require an explicit
        # execution path rather than a new repair on every heartbeat/main-loop cycle.
        if self.operational_halt and self.execution_state in {"UNRESOLVED_POSITION", "RECONCILE_FAILED"}:
            return None
        qty = self.position_qty()
        # Active strategy trade exists.
        if self.active_trade:
            intended = int(self.active_trade.get("qty", 0))
            if qty == intended:
                self.execution_state = "LONG_CONFIRMED"
                if now_ny().time() < TARGET_CANCEL_AFTER: self.ensure_target()
                return None
            if qty == 0:
                # Protective stop/manual flatten completed. Gather stop fill if available.
                stop_avg = 0.0; stop_commission = 0.0; status = "EXTERNAL_ZERO"
                if self.target_trade is not None:
                    stop_avg = _trade_avg_fill_from_execs(self.target_trade) or safe_float(getattr(self.target_trade.orderStatus,"avgFillPrice",0.0))
                    stop_commission = self._commission_from_trade(self.target_trade)
                    status = _trade_status(self.target_trade) or "TARGET_FILLED"
                try:
                    if self.active_stop_trade is not None and self.target_trade is None:
                        self.ib.sleep(0.10)
                        stop_avg = safe_float(getattr(self.active_stop_trade.orderStatus,"avgFillPrice",0.0),0.0)
                        status = str(getattr(self.active_stop_trade.orderStatus,"status","")) or status
                        stop_commission = self._commission_from_trade(self.active_stop_trade)
                except Exception:
                    pass
                exit_px = stop_avg if stop_avg > 0 else (market_px if market_px > 0 else safe_float(self.active_trade.get("hard_stop_price"), self.active_trade.get("entry_price",0.0)))
                rec = self._finalize_trade_record(exit_px, stop_commission, "SLOPE20_TARGET_OR_EXTERNAL_FLATTEN", status)
                log(self.log_file, f"BROKER RECONCILE position zero; external/stop exit pnl={rec.get('pnl',0):.2f}")
                return rec
            # Any other qty while active is an unexpected partial/excess position.
            self.operational_halt = True
            self.halt_reason = f"BROKER_POSITION_MISMATCH active_intended={intended} actual={qty}"
            self._event("POSITION_MISMATCH", intended_qty=intended, position_qty=qty,
                        halted=1, halt_reason=self.halt_reason)
            self._cancel_all_symbol_orders()
            self._emergency_flatten_to_zero("ACTIVE_POSITION_MISMATCH_REPAIR")
            # Use current market as conservative close record after repair.
            exit_px = market_px if market_px > 0 else safe_float(self.active_trade.get("entry_price"),0.0)
            rec = self._finalize_trade_record(exit_px, 0.0, "POSITION_MISMATCH_REPAIRED_TO_ZERO", "REPAIRED")
            return rec
        # No active trade: any broker position is orphaned and must be zeroed.
        if qty != 0:
            self.operational_halt = True
            self.halt_reason = f"ORPHAN_POSITION_WITHOUT_ACTIVE_TRADE:{qty}"
            self._event("ORPHAN_POSITION", position_qty=qty, halted=1, halt_reason=self.halt_reason)
            self._cancel_all_symbol_orders()
            self._emergency_flatten_to_zero("ORPHAN_POSITION_REPAIR")
            return None
        # Flat with orders = orphan orders, cancel them.
        if self.open_symbol_order_count() > 0:
            self._event("ORPHAN_ORDERS_WHILE_FLAT", reason="CANCEL_ORPHAN_ORDERS")
            self._cancel_all_symbol_orders()
        self.execution_state = "FLAT_CONFIRMED"
        return None

    def _disarm_protective_stop_before_exit(self, intended_qty: int, timeout: float = 8.0) -> Tuple[bool, str]:
        # Compatibility name: cancel Slope20 TARGET, not a stop.
        return self.cancel_target_for_liquidation(timeout=timeout)

    def exit_long(self, reference: Dict[str, Any], reason: str, force_zero: bool = False) -> Dict[str, Any]:
        result = {"ok": False, "action": "SELL", "reason": reason}
        qty = self.position_qty()
        if qty <= 0:
            result["error"] = "NO_LONG_POSITION"
            return result
        intended = int((self.active_trade or {}).get("qty", qty))
        if qty != intended:
            self.operational_halt = True
            self.halt_reason = f"EXIT_START_QTY_MISMATCH intended={intended} actual={qty}"
            self._event("EXIT_QTY_MISMATCH", intended_qty=intended, position_qty=qty,
                        halted=1, halt_reason=self.halt_reason)
            self._cancel_all_symbol_orders()
            self._emergency_flatten_to_zero("EXIT_START_QTY_MISMATCH")
            result["error"] = self.halt_reason
            return result
        try:
            # Strictly serialize protective stop vs normal AON SELL.
            # Never submit a strategy SELL until the stop is proven cancelled and the long remains intact.
            disarm_ok, disarm_reason = self._disarm_protective_stop_before_exit(intended, timeout=2.0)
            if not disarm_ok:
                self.operational_halt = True
                self.execution_state = "EXIT_PROTECTION_UNRESOLVED"
                self.halt_reason = f"EXIT_BLOCKED_PROTECTIVE_STOP_UNRESOLVED:{disarm_reason}"
                self._event("EXIT_BLOCKED_PROTECTIVE_STOP_UNRESOLVED",
                            intended_qty=intended, position_qty=self.position_qty(),
                            halted=1, halt_reason=self.halt_reason, reason=disarm_reason)
                result["error"] = self.halt_reason
                return result

            # If the protective stop itself already closed the position, finalize from broker truth
            # without sending another SELL.
            if self.position_qty() == 0:
                self.execution_state = "FLAT_CONFIRMED"
                result["ok"] = True
                result["action"] = "STOP_ALREADY_EXITED"
                result["reason"] = disarm_reason
                return result

            # Clean up any other non-protective symbol orders, then prove none remain.
            if not self._cancel_all_symbol_orders(timeout=2.0):
                self.operational_halt = True
                self.execution_state = "EXIT_ORDERS_UNRESOLVED"
                self.halt_reason = f"EXIT_BLOCKED_OPEN_ORDERS_AFTER_STOP_DISARM:{self.open_symbol_order_count()}"
                result["error"] = self.halt_reason
                return result

            if self.position_qty() != intended:
                self.operational_halt = True
                self.execution_state = "EXIT_POSITION_UNRESOLVED"
                self.halt_reason = f"EXIT_BLOCKED_POSITION_CHANGED_AFTER_STOP_DISARM:{self.position_qty()}!={intended}"
                result["error"] = self.halt_reason
                return result

            self.execution_state = "EXIT_PENDING"
            if self.active_trade:
                self.current_strategy_trade_id = str(self.active_trade.get("strategy_trade_id", "") or self.current_strategy_trade_id)
            self._event("EXIT_BEGIN", action="SELL", qty=qty, intended_qty=qty, reason=reason, aon=1,
                        message=f"strategy_trade_id={self.current_strategy_trade_id}")
            # V3: fresh IBKR L1 is the sole normal-exit price authority.
            q0 = self.fresh_l1()
            bid = safe_float(q0.get("bid"),0.0); ask = safe_float(q0.get("ask"),0.0); last = safe_float(q0.get("last"),0.0)
            if not q0.get("ok") or (last <= 0 and bid <= 0):
                if force_zero:
                    return self._emergency_flatten_to_zero(reason)
                raise RuntimeError("IBKR_EXIT_PRICE_UNAVAILABLE")
            spread = max(0.0, ask-bid) if ask > 0 and bid > 0 else max(first_positive(last, bid)*0.0005,0.01)
            total_commission = 0.0
            for attempt in range(1, self.aon_retries+1):
                placed = self._place_aon_marketable_limit(
                    "SELL", qty, self.entry_timeout_sec, f"SMCI_AON_EXIT_{attempt}",
                    purpose="EXIT", strategy_trade_id=self.current_strategy_trade_id,
                    allowance_bps=self.marketable_limit_allowance_bps)
                tr, avg, filled, status, fresh_q = placed
                if tr is None:
                    raise RuntimeError(status)
                total_commission += self._commission_from_trade(tr)
                if filled == qty and status == "Filled":
                    # Do not infer failure from one immediate position snapshot.
                    # Wait for broker position propagation and require stable zero twice.
                    deadline = time.time() + 5.0
                    stable_zero = False
                    while time.time() < deadline:
                        self.ib.sleep(0.10)
                        if self.position_qty() == 0:
                            self.ib.sleep(0.25)
                            if self.position_qty() == 0:
                                stable_zero = True
                                break
                    if not stable_zero:
                        self.operational_halt = True
                        self.execution_state = "EXIT_POSITION_UNRESOLVED"
                        self.halt_reason = f"EXIT_FULL_FILL_BUT_POSITION_NOT_STABLY_ZERO:{self.position_qty()}"
                        self._event("EXIT_NOT_ZERO", intended_qty=qty, filled=filled,
                                    position_qty=self.position_qty(), halted=1, halt_reason=self.halt_reason)
                        # One serialized repair only; never chain repairs.
                        self._emergency_flatten_to_zero("EXIT_FULL_FILL_POSITION_REPAIR")
                    exit_px = avg if avg > 0 else (bid if bid > 0 else last)
                    if self.active_trade is not None:
                        self.active_trade["moomoo_exit_bid"] = bid
                        self.active_trade["moomoo_exit_ask"] = ask
                        self.active_trade["moomoo_exit_last"] = last
                        self.active_trade["moomoo_exit_reference_price"] = bid if bid > 0 else last
                        self.active_trade["ibkr_exit_fill_price"] = exit_px
                    rec = self._finalize_trade_record(exit_px, total_commission, reason, status)
                    log(self.log_file, f"EXEC AON SELL FULL qty={qty} avg={exit_px:.4f} pnl={rec['pnl']:.2f} strategy_equity={self.strategy_equity:.2f} reason={reason}")
                    result.update({"ok": True, "qty": qty, "exit_price": exit_px,
                                   "pnl": rec["pnl"], "return_pct": rec["return_pct"],
                                   "trade_record": rec})
                    return result
                cancel_avg, cancel_filled, cancel_status = self._cancel_trade(
                    tr, f"AON_EXIT_RETRY_{attempt}", timeout=8.0)
                if cancel_avg > 0:
                    avg = cancel_avg
                filled = max(filled, cancel_filled)
                status = cancel_status
                broker_pos = self.position_qty()

                # A SELL may fill while PendingCancel. Broker truth wins; never retry into zero.
                if status == "Filled" or filled >= qty or broker_pos == 0:
                    deadline = time.time() + 5.0
                    stable_zero = False
                    while time.time() < deadline:
                        self.ib.sleep(0.10)
                        broker_pos = self.position_qty()
                        if broker_pos == 0:
                            self.ib.sleep(0.25)
                            if self.position_qty() == 0:
                                stable_zero = True
                                break
                    if not stable_zero:
                        self.operational_halt = True
                        self.execution_state = "EXIT_POSITION_UNRESOLVED"
                        self.halt_reason = f"EXIT_CANCEL_RACE_POSITION_NOT_STABLY_ZERO:{self.position_qty()}"
                        self._emergency_flatten_to_zero("EXIT_CANCEL_RACE_MISMATCH_REPAIR")
                    exit_px = avg if avg > 0 else (bid if bid > 0 else last)
                    if self.active_trade is not None:
                        self.active_trade["moomoo_exit_bid"] = bid
                        self.active_trade["moomoo_exit_ask"] = ask
                        self.active_trade["moomoo_exit_last"] = last
                        self.active_trade["moomoo_exit_reference_price"] = bid if bid > 0 else last
                        self.active_trade["ibkr_exit_fill_price"] = exit_px
                    rec = self._finalize_trade_record(exit_px, total_commission, reason, "Filled")
                    result.update({"ok": True, "qty": qty, "exit_price": exit_px,
                                   "pnl": rec["pnl"], "return_pct": rec["return_pct"],
                                   "trade_record": rec})
                    return result

                if 0 < filled < qty or (0 < broker_pos < qty):
                    self.operational_halt = True
                    self.halt_reason = f"UNEXPECTED_PARTIAL_AON_EXIT filled={filled}/{qty} broker_pos={broker_pos}"
                    self._event("AON_PARTIAL_ANOMALY", action="SELL", qty=qty, intended_qty=qty,
                                filled=filled, status=status, aon=1, halted=1, halt_reason=self.halt_reason)
                    em = self._emergency_flatten_to_zero("AON_EXIT_PARTIAL_REPAIR")
                    total_commission += safe_float(em.get("commission"), 0.0)
                    exit_px = safe_float(em.get("avg_fill_price"), 0.0) or (avg if avg > 0 else (bid if bid > 0 else last))
                    rec = self._finalize_trade_record(exit_px, total_commission, "AON_EXIT_PARTIAL_REPAIRED_TO_ZERO", "REPAIRED")
                    result.update({"ok": True, "qty": qty, "exit_price": exit_px,
                                   "pnl": rec["pnl"], "return_pct": rec["return_pct"],
                                   "trade_record": rec})
                    return result

                if status not in IBKR_TERMINAL_STATES:
                    self.operational_halt = True
                    self.execution_state = "EXIT_ORDER_UNRESOLVED"
                    self.halt_reason = f"AON_EXIT_ORDER_NOT_TERMINAL_AFTER_CANCEL:{status}"
                    result["error"] = self.halt_reason
                    return result
                if self.position_qty() != qty:
                    self.operational_halt = True
                    self.halt_reason = f"AON_EXIT_NOT_STABLY_LONG_BEFORE_RETRY:{self.position_qty()}!={qty}"
                    result["error"] = self.halt_reason
                    return result
            if force_zero:
                em = self._emergency_flatten_to_zero(f"FORCE_ZERO_AFTER_AON_EXIT_FAIL:{reason}")
                exit_px = safe_float(em.get("avg_fill_price"), 0.0) or (bid if bid > 0 else last)
                total_commission += safe_float(em.get("commission"), 0.0)
                rec = self._finalize_trade_record(exit_px, total_commission, f"{reason}_EMERGENCY_ZERO", "EMERGENCY")
                result.update({"ok": True, "qty": qty, "exit_price": exit_px,
                               "pnl": rec["pnl"], "return_pct": rec["return_pct"],
                               "trade_record": rec})
                return result
            # Normal exit did not fill fully: restore protection and remain long.
            self.execution_state = "LONG_CONFIRMED"
            entry_px = safe_float((self.active_trade or {}).get("entry_price"), self.position_avg_cost())
            self.ensure_target()
            result["error"] = "AON_EXIT_NOT_FILLED_FULL_SIZE_TARGET_RESTORED"
            self._event("EXIT_ABORTED_STILL_LONG", action="SELL", qty=qty, intended_qty=qty,
                        filled=0, remaining=qty, aon=1, reason=result["error"])
            return result
        except Exception as e:
            self.operational_halt = True
            self.halt_reason = f"EXIT_EXCEPTION:{e}"
            result["error"] = str(e)
            log(self.log_file, f"EXIT ERROR: {e}\n{traceback.format_exc()}")
            # If still long, ensure it has protection.
            try:
                q = self.position_qty()
                if q > 0 and self.open_symbol_order_count() == 0:
                    self.ensure_target()
            except Exception:
                pass
            return result

    def health_snapshot(self) -> Dict[str, Any]:
        qty = self.position_qty() if self.connected else 0
        halted, why = self.strategy_halted()
        return {
            "connected": int(bool(self.connected and self.ib is not None and self.ib.isConnected())),
            "execution_state": self.execution_state,
            "position_qty": qty,
            "avg_cost": self.position_avg_cost() if qty else 0.0,
            "open_symbol_orders": self.open_symbol_order_count() if self.connected else 0,
            "strategy_equity": self.strategy_equity,
            "strategy_equity_floor": "",
            "realized_pnl": self.realized_pnl,
            "trades_taken": self.trade_counter,
            "halted": int(halted),
            "halt_reason": why or self.halt_reason,
            "active_stop": int(self.active_stop_trade is not None),
            "registered_orders": len(self.order_registry),
            "processed_exec_ids": len(self.processed_exec_ids),
            "protective_stop_valid": "",  # deliberately not applicable to no-stop Slope20
        }

V21_AUDIT_META_KEYS = ['positive_slope', 'day_location_ok', 'vwap_location_ok', 'spread_ok', 'v21_fresh_qqq_sync', 'v21_structure_ok', 'dynamic_regime_not_bearish', 'core_peer_positive_count', 'core_peer_avg_5m', 'v21_trend_structure', 'v21_recovery_structure', 'entry_score_v15', 'regime_stock_ret_5m', 'regime_peer_positive_count', 'regime_peer_avg_5m']
V21_AUDIT_FIELDS = ['minute_key', 'action', 'reason', 'brain_entry_state', 'brain_entry_transition', 'brain_entry_state_prev', 'brain_dynamic_regime', 'stock_ret_5m', 'qqq_ret_5m', 'qqq_ret_5m_lag3', 'brain_positive_slope', 'brain_day_location_ok', 'brain_vwap_location_ok', 'brain_spread_ok', 'brain_v21_fresh_qqq_sync', 'brain_v21_structure_ok', 'brain_dynamic_regime_not_bearish', 'brain_core_peer_positive_count', 'brain_core_peer_avg_5m', 'brain_v21_trend_structure', 'brain_v21_recovery_structure', 'brain_entry_score_v15', 'brain_regime_stock_ret_5m', 'brain_regime_peer_positive_count', 'brain_regime_peer_avg_5m']

DECISION_FIELDS = ['ts', 'symbol', 'bar_time_utc', 'bar_time_ny', 'close', 'open', 'high', 'low', 'volume', 'turnover', 'vwap', 'ema_8', 'ema_21', 'ema_50', 'ema_100', 'range', 'body', 'close_location', 'day_position', 'dist_vwap_pct', 'control_delta', 'buyer_pressure', 'seller_pressure', 'rel_vol_30', 'rel_range_30', 'rel_body_30', 'green_ratio_10', 'red_ratio_10', 'above_vwap_ratio_20', 'bull_stack_ratio_20', 'bear_stack_ratio_20', 'slope_3', 'slope_5', 'slope_10', 'slope_20', 'slope_60', 'normal_range', 'vol_pctile_today', 'range_pctile_today', 'body_pctile_today', 'drop_from_open_pct', 'drop_from_high_pct', 'bounce_from_low_pct', 'low_age_bars', 'higher_low_proxy', 'seller_pressure_change_5', 'buyer_pressure_change_5', 'ema8_reclaimed', 'ema8_hold_bars_2', 'vwap_hold_bars_2', 'two_bar_reclaim_hold', 'trend_cleanliness', 'noise_score', 'tf5_control', 'tf10_control', 'tf15_control', 'smci_push_tick_count_45s', 'smci_push_delta_45s', 'peer_nvda_push_tick_count_45s', 'peer_nvda_push_delta_45s', 'peer_nvda_delta_lead_vs_smci_45s', 'peer_nvda_push_corr_last15', 'peer_amd_push_tick_count_45s', 'peer_amd_push_delta_45s', 'peer_amd_delta_lead_vs_smci_45s', 'peer_amd_push_corr_last15', 'peer_smh_push_tick_count_45s', 'peer_smh_push_delta_45s', 'peer_smh_delta_lead_vs_smci_45s', 'peer_smh_push_corr_last15', 'peer_qqq_push_tick_count_45s', 'peer_qqq_push_delta_45s', 'peer_qqq_delta_lead_vs_smci_45s', 'peer_qqq_push_corr_last15', 'peer_spy_push_tick_count_45s', 'peer_spy_push_delta_45s', 'peer_spy_delta_lead_vs_smci_45s', 'peer_spy_push_corr_last15', 'push_smci_tape_buffer_len', 'push_smci_tape_buffer_maxlen', 'push_tape_dedup_len', 'push_tape_dedup_maxlen', 'push_orderbook_buffer_len', 'push_quote_buffer_len', 'push_kline_buffer_len', 'push_peer_nvda_tape_buffer_len', 'push_peer_nvda_tape_buffer_maxlen', 'push_peer_amd_tape_buffer_len', 'push_peer_amd_tape_buffer_maxlen', 'push_peer_smh_tape_buffer_len', 'push_peer_smh_tape_buffer_maxlen', 'push_peer_qqq_tape_buffer_len', 'push_peer_qqq_tape_buffer_maxlen', 'push_peer_spy_tape_buffer_len', 'push_peer_spy_tape_buffer_maxlen', 'feature_minute_key_utc', 'stock_ret_5m', 'qqq_ret_5m', 'qqq_ret_5m_lag3', 'spy_ret_5m', 'rs_qqq_5m', 'rs_spy_5m', 'peer_nvda_ret_1m', 'peer_nvda_ret_5m', 'peer_nvda_lead_vs_smci_5m', 'peer_amd_ret_1m', 'peer_amd_ret_5m', 'peer_amd_lead_vs_smci_5m', 'peer_smh_ret_1m', 'peer_smh_ret_5m', 'peer_smh_lead_vs_smci_5m', 'peer_qqq_ret_1m', 'peer_qqq_ret_5m', 'peer_qqq_lead_vs_smci_5m', 'peer_spy_ret_1m', 'peer_spy_ret_5m', 'peer_spy_lead_vs_smci_5m', 'ibkr_available', 'ibkr_bid', 'ibkr_ask', 'ibkr_last', 'ibkr_bid_size', 'ibkr_ask_size', 'ibkr_snapshot_time_ny', 'ibkr_spread', 'ibkr_mid', 'moomoo_l2_available', 'moomoo_l2_score', 'moomoo_l2_imbalance', 'moomoo_l2_persistence', 'moomoo_l2_positive_count', 'moomoo_l2_negative_count', 'moomoo_bid_levels', 'moomoo_ask_levels', 'moomoo_bid_volume_50', 'moomoo_ask_volume_50', 'moomoo_bid_volume_50_weighted', 'moomoo_ask_volume_50_weighted', 'moomoo_l2_decay_lambda', 'moomoo_l2_exp_imbalance', 'moomoo_bid_volume_top3', 'moomoo_ask_volume_top3', 'moomoo_l2_top3_imbalance', 'moomoo_bid_volume_top10', 'moomoo_ask_volume_top10', 'moomoo_l2_top10_imbalance', 'coiled_l2_imbalance_diff_2bar', 'mf_tape_event_count', 'mf_push_tape_event_count', 'mf_buy_trade_count', 'mf_sell_trade_count', 'mf_buy_volume', 'mf_sell_volume', 'mf_unknown_volume', 'mf_total_tape_volume', 'mf_tape_delta_volume', 'mf_tape_delta_ratio', 'mf_block_buy_volume', 'mf_block_sell_volume', 'mf_block_delta_volume', 'mf_trade_price_std', 'mf_trade_size_mean', 'mf_trade_size_max', 'mf_l2_poll_count', 'mf_l2_level_rows', 'mf_push_orderbook_event_count', 'mf_push_quote_event_count', 'mf_push_kline_event_count', 'mf_l2_imbalance_mean', 'mf_l2_imbalance_min', 'mf_l2_imbalance_max', 'mf_l2_imbalance_std', 'mf_l2_top3_imbalance_mean', 'mf_l2_top3_imbalance_min', 'mf_l2_top3_imbalance_max', 'mf_l2_top3_positive_persistence', 'mf_l2_top3_negative_persistence', 'mf_l2_top3_longest_same_side_run', 'mf_l2_top10_imbalance_mean', 'mf_spread_pct_mean', 'mf_spread_pct_max', 'mf_bid_top3_mean', 'mf_ask_top3_mean', 'mf_inside_depth_bias', 'moomoo_snapshot_available', 'snapshot_code', 'snapshot_name', 'snapshot_update_time', 'snapshot_last_price', 'snapshot_open_price', 'snapshot_high_price', 'snapshot_low_price', 'snapshot_prev_close_price', 'snapshot_volume', 'snapshot_turnover', 'snapshot_turnover_rate', 'snapshot_suspension', 'snapshot_listing_date', 'snapshot_lot_size', 'snapshot_price_spread', 'snapshot_stock_owner', 'snapshot_ask_price', 'snapshot_bid_price', 'snapshot_ask_vol', 'snapshot_bid_vol', 'snapshot_enable_margin', 'snapshot_mortgage_ratio', 'snapshot_long_margin_initial_ratio', 'snapshot_enable_short_sell', 'snapshot_short_sell_rate', 'snapshot_short_available_volume', 'snapshot_short_margin_initial_ratio', 'snapshot_amplitude', 'snapshot_avg_price', 'snapshot_bid_ask_ratio', 'snapshot_volume_ratio', 'snapshot_highest52weeks_price', 'snapshot_lowest52weeks_price', 'snapshot_highest_history_price', 'snapshot_lowest_history_price', 'snapshot_close_price_5min', 'snapshot_after_volume', 'snapshot_after_turnover', 'snapshot_sec_status', 'snapshot_equity_valid', 'snapshot_issued_shares', 'snapshot_total_market_val', 'snapshot_net_asset', 'snapshot_net_profit', 'snapshot_earning_per_share', 'snapshot_outstanding_shares', 'snapshot_circular_market_val', 'snapshot_net_asset_per_share', 'snapshot_ey_ratio', 'snapshot_pe_ratio', 'snapshot_pb_ratio', 'snapshot_pe_ttm_ratio', 'snapshot_dividend_ttm', 'snapshot_dividend_ratio_ttm', 'snapshot_dividend_lfy', 'snapshot_dividend_lfy_ratio', 'snapshot_wrt_valid', 'snapshot_wrt_conversion_ratio', 'snapshot_wrt_type', 'snapshot_wrt_strike_price', 'snapshot_wrt_maturity_date', 'snapshot_wrt_end_trade', 'snapshot_wrt_recovery_price', 'snapshot_wrt_street_vol', 'snapshot_wrt_issue_vol', 'snapshot_wrt_street_ratio', 'snapshot_wrt_delta', 'snapshot_wrt_implied_volatility', 'snapshot_wrt_premium', 'snapshot_wrt_leverage', 'snapshot_wrt_ipop', 'snapshot_wrt_break_even_point', 'snapshot_wrt_conversion_price', 'snapshot_wrt_price_recovery_ratio', 'snapshot_wrt_score', 'snapshot_wrt_upper_strike_price', 'snapshot_wrt_lower_strike_price', 'snapshot_wrt_inline_price_status', 'snapshot_wrt_issuer_code', 'snapshot_option_valid', 'snapshot_option_type', 'snapshot_strike_time', 'snapshot_option_strike_price', 'snapshot_option_contract_size', 'snapshot_option_open_interest', 'snapshot_option_implied_volatility', 'snapshot_option_premium', 'snapshot_option_delta', 'snapshot_option_gamma', 'snapshot_option_vega', 'snapshot_option_theta', 'snapshot_option_rho', 'snapshot_option_net_open_interest', 'snapshot_option_expiry_date_distance', 'snapshot_option_contract_nominal_value', 'snapshot_option_owner_lot_multiplier', 'snapshot_option_area_type', 'snapshot_option_contract_multiplier', 'snapshot_index_option_type', 'snapshot_index_valid', 'snapshot_index_raise_count', 'snapshot_index_fall_count', 'snapshot_index_equal_count', 'snapshot_plate_valid', 'snapshot_plate_raise_count', 'snapshot_plate_fall_count', 'snapshot_plate_equal_count', 'snapshot_future_valid', 'snapshot_future_last_settle_price', 'snapshot_future_position', 'snapshot_future_position_change', 'snapshot_future_main_contract', 'snapshot_future_last_trade_time', 'snapshot_trust_valid', 'snapshot_trust_dividend_yield', 'snapshot_trust_aum', 'snapshot_trust_outstanding_units', 'snapshot_trust_netassetvalue', 'snapshot_trust_premium', 'snapshot_trust_assetclass', 'snapshot_pre_price', 'snapshot_pre_high_price', 'snapshot_pre_low_price', 'snapshot_pre_volume', 'snapshot_pre_turnover', 'snapshot_pre_change_val', 'snapshot_pre_change_rate', 'snapshot_pre_amplitude', 'snapshot_after_price', 'snapshot_after_high_price', 'snapshot_after_low_price', 'snapshot_after_change_val', 'snapshot_after_change_rate', 'snapshot_after_amplitude', 'snapshot_overnight_price', 'snapshot_overnight_high_price', 'snapshot_overnight_low_price', 'snapshot_overnight_volume', 'snapshot_overnight_turnover', 'snapshot_overnight_change_val', 'snapshot_overnight_change_rate', 'snapshot_overnight_amplitude', 'moomoo_tape_available', 'moomoo_tape_source', 'moomoo_tape_tick_count', 'moomoo_tape_score', 'moomoo_delta_score', 'moomoo_buy_burst_score', 'moomoo_sell_burst_score', 'moomoo_buyer_absorption_score', 'moomoo_seller_absorption_score', 'moomoo_aggr_buy_volume', 'moomoo_aggr_sell_volume', 'moomoo_tape_delta', 'moomoo_tape_delta_ratio', 'moomoo_tape_delta_change', 'moomoo_tape_velocity', 'moomoo_block_trade_count', 'moomoo_block_buy_volume', 'moomoo_block_sell_volume', 'moomoo_block_delta', 'moomoo_block_delta_score', 'moomoo_bear_reversal_confidence', 'moomoo_bull_continuation_confidence', 'moomoo_cf_available', 'moomoo_cf_rows', 'moomoo_cf_last_valid_time', 'moomoo_cf_item_time', 'moomoo_cf_in_flow', 'moomoo_cf_super_in_flow', 'moomoo_cf_big_in_flow', 'moomoo_cf_mid_in_flow', 'moomoo_cf_sml_in_flow', 'moomoo_cf_main_in_flow', 'moomoo_cf_whale_flow', 'moomoo_cf_support_flow', 'moomoo_cf_in_flow_delta_1m', 'moomoo_cf_big_flow_delta_1m', 'moomoo_cf_super_flow_delta_1m', 'moomoo_cf_whale_flow_delta_1m', 'moomoo_cf_in_flow_slope_3m', 'moomoo_cf_big_flow_slope_3m', 'moomoo_cf_whale_flow_slope_3m', 'ibkr_close', 'ibkr_last_size', 'ibkr_volume', 'ibkr_high', 'ibkr_low', 'ibkr_position_qty', 'strategy_equity', 'strategy_equity_floor', 'strategy_halted', 'last', 'bid', 'ask', 'spread', 'moomoo_spread_pct', 'spread_score', 'orderbook_score', 'orderbook_imbalance', 'tape_score', 'delta_score', 'buy_burst_score', 'sell_burst_score', 'buyer_absorption_score', 'seller_absorption_score', 'bear_day_reversal_confidence', 'bull_continuation_confidence', 'volume_ratio', 'structure_score', 'momentum_score', 'vwap_score', 'orderflow_score', 'bias_score', 'entry_score', 'smart_money_composite_score', 'smart_money_gate_ok', 'smart_money_reason', 'reversal_reclaim_score', 'reversal_reclaim_ok', 'reversal_reason', 'bars_since_reversal_low', 'recovery_from_reversal_low_pct', 'selloff_from_reversal_high_pct', 'market', 'micro', 'decision', 'reason', 'regime', 'coiled_entry_ready', 'coiled_break_even_locked', 'coiled_peak_return_pct', 'cycle_ts_ny', 'feature_bar_time_ny', 'feature_bar_age_sec', 'ibkr_vs_cycle_age_sec', 'ibkr_vs_bar_lag_sec', 'alignment_warning', 'feature_row_type', 'feature_emit_ts_ny', 'brain_microdip_ready', 'brain_microdip_state', 'brain_microdip_transition', 'brain_microdip_slope5', 'brain_microdip_threshold', 'brain_microdip_qqq5', 'brain_microdip_l2', 'brain_microdip_history_samples', 'brain_microdip_l2_fresh', 'brain_smci_impulse', 'brain_peer_breadth', 'brain_peer_average', 'brain_positive_slope', 'brain_day_location_ok', 'brain_vwap_location_ok', 'brain_spread_ok', 'brain_v21_fresh_qqq_sync', 'brain_v21_structure_ok', 'brain_dynamic_regime_not_bearish', 'brain_entry_state', 'brain_entry_score_v15', 'brain_core_peer_positive_count', 'brain_core_peer_avg_5m', 'brain_qqq_5m', 'brain_nvda_5m', 'brain_smh_5m', 'brain_amd_5m', 'brain_v21_qqq_5m_lag3', 'brain_v21_qqq_5m_lag3_max', 'brain_v21_above_vwap_ratio_20', 'brain_v21_drop_from_high_pct', 'brain_v21_trend_structure', 'brain_v21_recovery_structure', 'brain_opening_range_ready', 'brain_opening_3m_high', 'brain_opening_3m_low', 'brain_opening_3m_width_pct', 'brain_above_opening_3m_high', 'brain_below_opening_3m_low', 'brain_dist_opening_3m_high_pct', 'brain_dist_opening_3m_low_pct', 'brain_dynamic_regime', 'brain_regime_stock_ret_5m', 'brain_regime_peer_positive_count', 'brain_regime_peer_negative_count', 'brain_regime_peer_avg_5m', 'brain_regime_l2_trap', 'brain_regime_extended', 'brain_regime_or_breakout', 'brain_lead_nvda', 'brain_lead_smh', 'brain_lead_qqq', 'brain_lead_amd', 'brain_lead_positive_core', 'brain_lead_negative_core', 'brain_lead_core_mean', 'brain_entry_transition', 'brain_entry_state_prev', 'brain_action', 'brain_reason', 'position_qty', 'active_entry_price', 'active_mfe_pct', 'active_mae_pct', 'active_current_return_pct', 'daily_realized_pnl', 'trades_taken', 'execution_state', 'trading_halted', 'decision_latency_ms', 'decision_calc_ms', 'bar_observed_ms', 'callback_delay_ms', 'callback_to_observed_ms', 'fast_row_ms', 'state_ms', 'execution_ms']

def add_decision_columns(row: Dict[str, Any], meta: Dict[str, Any], action: str, reason: str, exec_state: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for k, v in meta.items():
        out[f"brain_{k}"] = v
    out["brain_action"] = action
    out["brain_reason"] = reason
    out["position_qty"] = exec_state.get("position_qty", 0)
    out["active_entry_price"] = exec_state.get("entry_price", 0.0)
    out["active_mfe_pct"] = exec_state.get("mfe_pct", 0.0)
    out["active_mae_pct"] = exec_state.get("mae_pct", 0.0)
    out["active_current_return_pct"] = exec_state.get("current_ret_pct", 0.0)
    out["daily_realized_pnl"] = exec_state.get("daily_realized_pnl", 0.0)
    out["trades_taken"] = exec_state.get("trades_taken", 0)
    out["strategy_equity"] = exec_state.get("strategy_equity", 0.0)
    out["strategy_equity_floor"] = exec_state.get("strategy_equity_floor", 0.0)
    out["execution_state"] = exec_state.get("execution_state", "")
    out["trading_halted"] = exec_state.get("trading_halted", 0)
    return out


# === V2.2 MICRO-DIP ADDITION (isolated from frozen V2.1) ===
class Slope20Brain:
    """Original frozen Slope20, evaluated on a new completed IBKR one-minute bar."""
    def evaluate(self,row):
        try:
            s20=float(row["slope_20"]); s5=float(row["slope_5"])
            valid=math.isfinite(s20) and math.isfinite(s5)
        except (ValueError,TypeError,KeyError):
            s20=s5=float("nan"); valid=False
        fire=bool(valid and s20<=SLOPE20_LIMIT and s5<=SLOPE5_LIMIT)
        return fire,{"slope_20":s20,"slope_5":s5,"slope20_threshold":SLOPE20_LIMIT,
                     "slope5_threshold":SLOPE5_LIMIT,"slope20_signal":int(fire)}

class Slope20DailyGuard:
    """Fail-closed atomic daily entry-intent ledger; never permits a second entry on restart."""
    def __init__(self,path,symbol):
        self.path=Path(path); self.symbol=symbol
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.lock=threading.Lock()
    def _read(self):
        if not self.path.exists(): return {}
        return json.loads(self.path.read_text(encoding="utf-8"))
    def consumed(self,day):
        try: return day in self._read()
        except Exception: return True
    def reserve(self,day,minute):
        with self.lock:
            try:
                data=self._read()
                if day in data: return False
                data[day]={"minute":minute,"reserved_at":ts_iso(),"symbol":self.symbol}
                tmp=self.path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data,indent=2),encoding="utf-8")
                tmp.replace(self.path)
                return True
            except Exception: return False

def main():
    p = argparse.ArgumentParser(description="SMCI trigger engine V2.1: V2 + fresh QQQ synchronization + structure gate. Paper mode default.")
    p.add_argument("--symbol", default="SMCI")
    p.add_argument("--out-dir", default="SMCI_trading_engine_outputs")
    p.add_argument("--moomoo-host", default="127.0.0.1")
    p.add_argument("--moomoo-port", type=int, default=11111)
    p.add_argument("--market-prefix", default="US")
    p.add_argument("--l2-levels", type=int, default=50)
    p.add_argument("--ticker-count", type=int, default=300)
    p.add_argument("--kline-count", type=int, default=420)
    p.add_argument("--poll-seconds", type=float, default=5.0)
    p.add_argument("--peers", default="NVDA,AMD,SMH,QQQ,SPY")
    p.add_argument("--peer-refresh-seconds", type=float, default=20.0)
    p.add_argument("--wait-for-rth", action="store_true", default=True)
    p.add_argument("--no-wait-for-rth", dest="wait_for_rth", action="store_false")

    # Execution
    p.add_argument("--mode", choices=["paper", "live", "observe"], default="paper")
    p.add_argument("--i-understand-live", action="store_true")
    p.add_argument("--ibkr-host", default="127.0.0.1")
    p.add_argument("--ibkr-paper-port", type=int, default=4002)
    p.add_argument("--ibkr-live-port", type=int, default=4001)
    p.add_argument("--ibkr-client-id", type=int, default=8831)
    p.add_argument("--account-capital", type=float, default=10000.0, help="Strategy reference capital; compounds with realized P/L.")
    p.add_argument("--capital-usage-pct", type=float, default=1.0, help="Default 1.0 = maximum whole-share all-in capacity.")
    p.add_argument("--strategy-equity-floor", type=float, default=float("-inf"), help="Legacy schema only; disabled for Slope20.")
    p.add_argument("--max-trades", type=int, default=1)
    p.add_argument("--max-daily-loss-usd", type=float, default=float("-inf"), help="Legacy schema only; disabled for Slope20.")
    p.add_argument("--entry-timeout-sec", type=float, default=2.5)
    p.add_argument("--aon-retries", type=int, default=2, help="Full-size AON limit attempts before aborting an entry or restoring protection on a normal exit.")
    p.add_argument("--marketable-limit-allowance-bps", type=float, default=8.0, help="Fresh-IBKR AON marketable-limit allowance per attempt; bounded, never an unrestricted market order.")
    p.add_argument("--fast-loop-seconds", type=float, default=0.10, help="Fast IBKR V2 loop cadence. Rich Moomoo recording runs independently.")
    p.add_argument("--heartbeat-seconds", type=float, default=30.0)
    p.add_argument("--allow-existing-position", action="store_true", help="Dangerous: allow startup while an SMCI position already exists. Default is refuse to start trading.")

    # Frozen candidate parameters from the 15-session work.
    p.add_argument("--smci-ret-5m-min", type=float, default=DEFAULT_SMCI_RET_5M_MIN)
    p.add_argument("--core-peer-min", type=float, default=DEFAULT_CORE_PEER_MIN)
    p.add_argument("--core-peer-avg-min", type=float, default=DEFAULT_CORE_PEER_AVG_MIN)
    p.add_argument("--min-positive-core-peers", type=int, default=DEFAULT_MIN_POSITIVE_CORE_PEERS)
    p.add_argument("--day-position-max", type=float, default=DEFAULT_DAY_POSITION_MAX)
    p.add_argument("--dist-vwap-min", type=float, default=DEFAULT_DIST_VWAP_MIN)
    p.add_argument("--dist-vwap-max", type=float, default=DEFAULT_DIST_VWAP_MAX)
    p.add_argument("--slope5-min", type=float, default=DEFAULT_SLOPE5_MIN)
    p.add_argument("--max-spread-pct", type=float, default=DEFAULT_MAX_SPREAD_PCT)
    p.add_argument("--l2-trap-positive", type=float, default=DEFAULT_L2_TRAP_POSITIVE)
    p.add_argument("--hard-stop-pct", type=float, default=DEFAULT_HARD_STOP_PCT)
    p.add_argument("--trail-arm-pct", type=float, default=DEFAULT_TRAIL_ARM_PCT)
    p.add_argument("--trail-giveback-pct", type=float, default=DEFAULT_TRAIL_GIVEBACK_PCT)
    p.add_argument("--enable-leadlag-veto", action="store_true", help="Optional; disabled by default because lead/lag threshold was not independently validated.")
    p.add_argument("--leadlag-veto-threshold", type=float, default=-0.25)
    p.add_argument("--require-opening-range-breakout", action="store_true", help="Experimental hard gate. Default OFF: opening 3m range is confidence/context only until independently validated.")
    # V2.1 frozen research gate defaults. Exposed for audit/replay parity; tonight use defaults.
    p.add_argument("--v21-qqq-5m-lag3-max", type=float, default=DEFAULT_V21_QQQ_5M_LAG3_MAX)
    p.add_argument("--v21-above-vwap-ratio-20-min", type=float, default=DEFAULT_V21_ABOVE_VWAP_RATIO_20_MIN)
    p.add_argument("--v21-recovery-dist-vwap-min", type=float, default=DEFAULT_V21_RECOVERY_DIST_VWAP_MIN)
    p.add_argument("--v21-recovery-day-position-min", type=float, default=DEFAULT_V21_RECOVERY_DAY_POSITION_MIN)
    p.add_argument("--v21-recovery-drop-from-high-min", type=float, default=DEFAULT_V21_RECOVERY_DROP_FROM_HIGH_MIN)
    # Default shadow: never routes a Micro-Dip order. Paper opt-in only.
    p.add_argument("--moomoo-health-max-age-sec", type=float, default=15.0)
    args = p.parse_args()
    if args.mode == "live": p.error("Slope20 conversion is PAPER ONLY")
    if args.max_trades != 1: p.error("Slope20 enforces exactly one entry per NY day")
    if args.allow_existing_position: p.error("Existing-position adoption is not supported: flatten/reconcile manually before startup")
    if args.account_capital != 10000.0 or args.capital_usage_pct != 1.0:
        p.error("Frozen Slope20 capital is $10,000 initial and 100% allocation")



    symbol = args.symbol.upper().strip()
    out_dir = Path(args.out_dir) / symbol / today_str()
    out_dir.mkdir(parents=True, exist_ok=True)
    log_file = out_dir / f"{symbol}_{today_str()}_engine.log"
    feature_file = out_dir / f"{symbol}_{today_str()}_features_wide.csv"
    decision_file = out_dir / f"{symbol}_{today_str()}_decisions.csv"
    trade_file = out_dir / f"{symbol}_{today_str()}_trades.csv"
    trading_event_file = out_dir / f"{symbol}_{today_str()}_trading_events.csv"

    daily_state_file = Path(args.out_dir) / symbol / f"{symbol}_slope20_daily_entry_state.json"
    daily_guard = Slope20DailyGuard(daily_state_file, symbol)
    log(log_file, "SLOPE20 ONLY: closed-bar slope20<=-0.7135 slope5<=0; one entry/day; +1% native target; 15:50 NY flat; NO stop/loss breaker")
    log(log_file, "IBKR paper-only; Moomoo recorder independent of trading decisions")

    if args.mode == "live":
        raise SystemExit("LIVE DISABLED: this combined target-only experiment is paper-only.")
    if after_rth_now():
        log(log_file, "After 16:00 NY; exiting.")
        return
    if before_rth_now():
        if not args.wait_for_rth:
            log(log_file, "Before RTH and --no-wait-for-rth supplied; exiting.")
            return
        while before_rth_now():
            log(log_file, f"Waiting for 09:30 NY; seconds={seconds_until_rth_start():.0f}")
            time.sleep(min(60.0, max(1.0, seconds_until_rth_start())))

    feat_engine = FeatureEngine()
    peers = [x.strip().upper() for x in args.peers.split(",") if x.strip()]
    moo = MoomooRecorder(symbol, args.moomoo_host, args.moomoo_port, args.market_prefix, args.l2_levels, log_file, peer_symbols=peers)
    moo.connect()

    ib_port = args.ibkr_live_port if args.mode == "live" else args.ibkr_paper_port
    executor = None
    if args.mode in {"paper", "live"}:
        executor = IBKRExecutionEngine(
            symbol, args.ibkr_host, ib_port, args.ibkr_client_id, log_file, trading_event_file,
            args.mode, args.i_understand_live, args.account_capital, args.capital_usage_pct,
            args.strategy_equity_floor,
            args.hard_stop_pct, args.trail_arm_pct, args.trail_giveback_pct,
            args.entry_timeout_sec, args.aon_retries, args.max_trades, args.max_daily_loss_usd,
        )
        if not executor.connect():
            raise SystemExit("IBKR execution connection failed.")
        executor.marketable_limit_allowance_bps = float(args.marketable_limit_allowance_bps)
        equity_file=Path(args.out_dir)/symbol/f"{symbol}_slope20_equity_state.json"
        executor.equity_state_file=equity_file
        if equity_file.exists():
            try:
                saved=json.loads(equity_file.read_text(encoding="utf-8"))
                executor.initial_strategy_capital=float(saved["equity"])
                if not math.isfinite(executor.initial_strategy_capital) or executor.initial_strategy_capital<=0: raise ValueError("invalid equity")
                log(log_file,f"SLOPE20_COMPOUNDED_EQUITY loaded={executor.initial_strategy_capital:.2f}")
            except Exception as e:
                executor.close();moo.close()
                raise SystemExit(f"Refusing entries: compounded equity state invalid: {e}")
        existing_qty = executor.position_qty()
        if existing_qty != 0 and not args.allow_existing_position:
            executor.close()
            moo.close()
            raise SystemExit(f"Refusing to start with existing {symbol} position qty={existing_qty}. Flatten it manually before starting the strategy.")
        if existing_qty == 0 and executor.open_symbol_order_count() > 0:
            log(log_file, f"STARTUP RECONCILE: flat but {executor.open_symbol_order_count()} orphan {symbol} orders exist; cancelling them before trading.")
            executor._cancel_all_symbol_orders()
        executor.execution_state = "FLAT_CONFIRMED" if executor.position_qty() == 0 else "POSITION_PRESENT"
        # Fail closed if IBKR reports an earlier SMCI BUY today, even if the local
        # entry-intent file was deleted. Broker executions are authoritative.
        try:
            for fill in executor.ib.fills():
                contract=getattr(fill,"contract",None)
                execution=getattr(fill,"execution",None)
                when=getattr(fill,"time",None)
                dt=parse_any_ts_ny(when)
                if (contract is not None and getattr(contract,"symbol","").upper()==symbol
                    and execution is not None and str(getattr(execution,"side","")).upper() in ("BOT","BUY")
                    and dt is not None and dt.date().isoformat()==today_str()):
                    daily_guard.reserve(today_str(),"BROKER_EXECUTION_ALREADY_PRESENT")
                    log(log_file,"SLOPE20_DAILY_GUARD broker BUY already executed today; no further entries")
                    break
        except Exception as e:
            executor.operational_halt=True
            executor.halt_reason=f"DAILY_BROKER_EXECUTION_AUDIT_FAILED:{e}"
            log(log_file,f"CRITICAL {executor.halt_reason}")
    else:
        # observe mode still benefits from IBKR snapshot when available, but never orders.
        executor = IBKRExecutionEngine(
            symbol, args.ibkr_host, args.ibkr_paper_port, args.ibkr_client_id, log_file, trading_event_file,
            "paper", False, args.account_capital, args.capital_usage_pct,
            args.strategy_equity_floor,
            args.hard_stop_pct, args.trail_arm_pct, args.trail_giveback_pct,
            args.entry_timeout_sec, args.aon_retries, args.max_trades, args.max_daily_loss_usd,
        )
        executor.connect()
        executor.marketable_limit_allowance_bps = float(args.marketable_limit_allowance_bps)

    brain = Slope20Brain()
    last_feature_row: Dict[str, Any] = {}
    last_heartbeat_wall = 0.0
    last_fast_minute = ""
    last_ibkr_recovery_attempt=0.0
    ibkr_recovery_attempts=0
    last_feed_health_log=0.0

    # V3: IBKR closed 1m bars and fresh L1 run independently of the rich recorder.
    fast_feed = IBKRFastBarFeed(executor, [symbol] + peers, log_file)
    fast_feed.start()
    # Historical closed bars are never traded on startup.
    seed_key, _seed_row = fast_feed.fast_row(feat_engine, now_ny(), l2_context={})
    if seed_key:
        last_fast_minute = seed_key
        log(log_file, f"SLOPE20_STARTUP_SEED closedMinute={seed_key} noHistoricalEntry=1")
    moo_sampler = AsyncMoomooSampler(moo, feat_engine, args, executor, log_file)
    moo_sampler.start()
    feature_writer = AsyncFeatureWriter(symbol, peers, feat_engine, moo, moo_sampler, executor, feature_file, decision_file, log_file)
    feature_writer.start()
    log(log_file, "FAST_PATH_READY_V4_LATENCY_ACCURACY: minimal exact decision-row calculator active; full 433-column recorder remains async; IBKR bar callback timing active; stale startup transitions suppressed; terminal-stop exit sleeps removed.")

    try:
        while True:
            now = now_ny()
            if executor and executor.ib:
                # Yield briefly to the IB socket event loop without introducing feature work.
                executor.ib.sleep(0.0)
            # Allow the 15:59 bar to close at 16:00 and be recorded, without new orders.
            if now.time() >= dtime(16,0,12):
                log(log_file, "RTH recorder grace completed; exiting.")
                break
            if not is_rth_now(now) and now.time() < RTH_END:
                time.sleep(0.2); continue

            # Broker truth and target-fill reconciliation are continuous.
            qlive = executor.fresh_l1(wait_sec=0.0) if executor else {}
            live_px = first_positive(qlive.get("last"), qlive.get("bid"), qlive.get("ask"))
            if executor and args.mode in {"paper", "live"}:
                ext = executor.sync_and_reconcile(live_px)
                if ext:
                    append_csv(trade_file, ext)

            if executor and args.mode == "paper" and now.time() >= TARGET_CANCEL_AFTER and executor.position_qty() > 0:
                executor.cancel_target_for_liquidation(timeout=2.0)
            # Force flatten remains independent of recorder availability.
            if executor and args.mode == "paper" and now.time() >= FORCE_FLATTEN_AFTER and executor.position_qty() > 0:
                res = executor.exit_long(last_feature_row or {}, "RTH_FORCE_FLATTEN_1550", force_zero=True)
                if res.get("ok") and res.get("trade_record"): append_csv(trade_file, res["trade_record"])
                elif not res.get("ok"): log(log_file,f"CRITICAL EOD_EXIT_UNCONFIRMED {res.get('error')} position={executor.position_qty()}")
                time.sleep(0.1); continue

            # Cached Moomoo context only; no Moomoo network request on the trading thread.
            fast_ctx = moo_sampler.fast_context()
            l2_ctx = dict(fast_ctx.get("l2", {})); l2_ctx.update(fast_ctx.get("acc", {}))
            probe_now=now_ny()
            candidate_key=fast_feed.latest_closed_key(symbol, probe_now)
            # A healthy socket does not imply a healthy keepUpToDate subscription.
            expected_key=(probe_now-timedelta(minutes=1)).replace(second=0,microsecond=0)
            candidate_dt=parse_any_ts_ny(candidate_key) if candidate_key else None
            bar_age=(expected_key-candidate_dt).total_seconds() if candidate_dt else float("inf")
            if bar_age > 90.0 and time.monotonic()-last_ibkr_recovery_attempt >= min(120.0,15.0*2**min(ibkr_recovery_attempts,3)):
                last_ibkr_recovery_attempt=time.monotonic(); ibkr_recovery_attempts+=1
                log(log_file,f"FAST_IBKR_STALE_BAR ageSec={bar_age:.1f} attempts={ibkr_recovery_attempts} connected={executor.ib.isConnected() if executor else False}")
                if fast_feed.recover_symbol(symbol):
                    candidate_key=fast_feed.latest_closed_key(symbol,now_ny())
            elif bar_age <= 90.0:
                ibkr_recovery_attempts=0
            l2_age_health = time.time() - safe_float(fast_ctx.get("l2_polled_wall"), 0.0)
            l2_fresh_health = bool(0 <= l2_age_health <= args.moomoo_health_max_age_sec and
                                   safe_float(fast_ctx.get("l2", {}).get("moomoo_l2_available"), 0.0) == 1.0)
            # Tape is research context, not an entry gate. A missing summary is NOT a proof of stale tape.
            tape_health = moo_sampler.tape_health()
            if time.monotonic()-last_feed_health_log > 30.0 and (not l2_fresh_health or not tape_health["fresh"]):
                log(log_file, f"MOOMOO_DATA_HEALTH l2Fresh={int(l2_fresh_health)} "
                    f"l2AgeSec={l2_age_health:.1f} tapeFresh={int(tape_health['fresh'])} "
                    f"tapeAgeSec={tape_health['age_sec']:.1f} "
                    f"entryGate=MICRODIP_L2_ONLY tapeGate=NONE")
                last_feed_health_log=time.monotonic()
            minute_key=None; fast_f={}; fast_row_ms=0.0
            if bar_age > 90.0:
                candidate_key=None  # Never trade a recovered historical/stale minute.
            if candidate_key and candidate_key != last_fast_minute:
                fastrow_t0=time.perf_counter()
                minute_key, fast_f = fast_feed.fast_row(feat_engine, probe_now, l2_context=l2_ctx)
                fast_row_ms=(time.perf_counter()-fastrow_t0)*1000.0

            # -------- FAST V2 DECISION / EXECUTION PATH --------
            if minute_key and minute_key != last_fast_minute:
                observed_at=now_ny()
                decision_start = time.perf_counter()
                bar_dt = parse_any_ts_ny(fast_f.get("bar_time_ny"))
                expected_close = (bar_dt + timedelta(minutes=1)) if bar_dt else observed_at
                bar_observed_ms=max(0.0,(observed_at-expected_close).total_seconds()*1000.0)
                callback_wall, callback_perf = fast_feed.callback_timing(symbol, minute_key)
                callback_delay_ms = max(0.0, (callback_wall-expected_close).total_seconds()*1000.0) if callback_wall else -1.0
                callback_to_observed_ms = max(0.0, (observed_at-callback_wall).total_seconds()*1000.0) if callback_wall else -1.0

                fast_row = dict(fast_f); fast_row.update(l2_ctx)
                # The Moomoo sampler is asynchronous: fail closed if no recent L2 poll.
                l2_poll_age = time.time() - safe_float(fast_ctx.get("l2_polled_wall"), 0.0)
                l2_fresh = 0 <= l2_poll_age <= args.moomoo_health_max_age_sec
                # Moomoo L2/tape never gates Slope20.
                micro_meta = {}

                l1_t0=time.perf_counter(); q = executor.fresh_l1(wait_sec=0.0) if executor else {}; l1_ms=(time.perf_counter()-l1_t0)*1000.0
                ibid=safe_float(q.get("bid"),0); iask=safe_float(q.get("ask"),0); ilast=first_positive(q.get("last"),fast_row.get("close"))
                fast_row.update({"ibkr_bid":ibid,"ibkr_ask":iask,"ibkr_last":ilast,
                                 "ibkr_spread_pct":((iask-ibid)/ilast*100.0) if ilast>0 and iask>ibid>0 else 0.0,
                                 "bid":ibid,"ask":iask,"last":ilast})

                state_t0=time.perf_counter()
                position_qty = executor.position_qty() if executor else 0
                exec_state = {"position_qty":position_qty,"daily_realized_pnl":executor.realized_pnl if executor else 0.0,
                              "trades_taken":executor.trade_counter if executor else 0,"strategy_equity":executor.strategy_equity if executor else args.account_capital,
                              "strategy_equity_floor":executor.strategy_equity_floor if executor else args.strategy_equity_floor,
                              "execution_state":executor.execution_state if executor else "OBSERVE",
                              "trading_halted":int(executor.strategy_halted()[0]) if executor else 0}
                action="HOLD" if position_qty>0 else "WAIT"; reason="NO_TRIGGER"; meta=dict(micro_meta); execution_ms=0.0

                if position_qty > 0:
                    meta.update(brain.evaluate(fast_row)[1])
                    reason = "BROKER_NATIVE_TARGET_WORKING"
                    if executor and executor.target_trade is None:
                        executor.ensure_target()
                else:
                    buy_fire, entry_meta = brain.evaluate(fast_row)
                    meta.update(entry_meta)
                    can_new_entry_time = RTH_START <= observed_at.time() < NO_NEW_ENTRY_AFTER
                    can_exec,exec_why = executor.can_trade() if executor and args.mode == "paper" else (True,"OBSERVE")
                    if daily_guard.consumed(observed_at.date().isoformat()):
                        reason="ONE_ENTRY_ALREADY_CONSUMED"
                    elif buy_fire and not can_new_entry_time:
                        reason="ENTRY_AFTER_CUTOFF"
                    elif buy_fire and not can_exec:
                        reason=exec_why
                    elif buy_fire and can_new_entry_time:
                        if args.mode == "paper":
                            # Reserve before order submission: crashes cannot allow a second entry.
                            if daily_guard.reserve(observed_at.date().isoformat(), minute_key):
                                action,reason="BUY","SLOPE20_ORIGINAL"
                                ex_t0=time.perf_counter()
                                res=executor.enter_long(fast_row,reason)
                                execution_ms=(time.perf_counter()-ex_t0)*1000.0
                                if not res.get("ok"):
                                    action,reason="WAIT",f"ENTRY_EXECUTION_FAILED:{res.get('error','unknown')}"
                                else:
                                    executor.ensure_target()
                            else:
                                reason="DAILY_ENTRY_RESERVATION_FAILED"
                        else:
                            action,reason="BUY","OBSERVE_SLOPE20_SIGNAL"

                state_ms=(time.perf_counter()-state_t0)*1000.0-execution_ms
                calc_ms=(time.perf_counter()-decision_start)*1000.0
                decision_done=now_ny(); decision_latency_ms=max(0.0,(decision_done-expected_close).total_seconds()*1000.0)
                log(log_file,f"FAST_DECISION minute={minute_key} action={action} reason={reason} closeToDecisionMs={decision_latency_ms:.1f} callbackDelayMs={callback_delay_ms:.1f} callbackToObservedMs={callback_to_observed_ms:.1f} barObservedMs={bar_observed_ms:.1f} fastRowMs={fast_row_ms:.1f} l1Ms={l1_ms:.1f} stateMs={state_ms:.1f} execMs={execution_ms:.1f} calcMs={calc_ms:.1f} "
                             f"ibkrBid={ibid:.4f} ibkrAsk={iask:.4f} smci5={safe_float(fast_row.get('stock_ret_5m')):.3f}% "
                             f"qqq={safe_float(fast_row.get('qqq_ret_5m')):.3f}% qqqLag3={safe_float(fast_row.get('qqq_ret_5m_lag3')):.3f}% nvda={safe_float(fast_row.get('peer_nvda_ret_5m')):.3f}% smh={safe_float(fast_row.get('peer_smh_ret_5m')):.3f}% "
                             f"v21Fresh={int(bool(meta.get('v21_fresh_qqq_sync',0)))} v21Struct={int(bool(meta.get('v21_structure_ok',0)))} trendStruct={int(bool(meta.get('v21_trend_structure',0)))} recoveryStruct={int(bool(meta.get('v21_recovery_structure',0)))}")
                if bar_observed_ms > 2000.0:
                    log(log_file,f"FAST_LATENCY_ALERT minute={minute_key} stage=BAR_OBSERVED delayMs={bar_observed_ms:.1f} (use callbackDelayMs vs callbackToObservedMs vs fastRowMs to localize latency)")

                # Capture recorder inputs only; all heavy feature construction/CSV writing is async.
                ibsnap=executor.snapshot_nowait() if executor else {"ibkr_available":0.0}
                feature_writer.submit(minute_key,{"fast_f":dict(fast_f),"action":action,"reason":reason,"meta":dict(meta),"exec_state":dict(exec_state),
                                                  "decision_latency_ms":decision_latency_ms,"decision_calc_ms":calc_ms,"bar_observed_ms":bar_observed_ms,
                                                  "callback_delay_ms":callback_delay_ms,"callback_to_observed_ms":callback_to_observed_ms,
                                                  "fast_row_ms":fast_row_ms,"state_ms":state_ms,"execution_ms":execution_ms,"ibsnap":ibsnap})
                last_fast_minute=minute_key
                last_feature_row=dict(fast_row)

            if time.time()-last_heartbeat_wall >= max(5.0,args.heartbeat_seconds):
                health=executor.health_snapshot() if executor else {}; log(log_file,f"HEARTBEAT_V3 fastMinute={last_fast_minute or 'NONE'} pendingFeatures={feature_writer.pending_count()} pos={health.get('position_qty',0)} equity={safe_float(health.get('strategy_equity'),0):.2f} halted={health.get('halted',0)}")
                last_heartbeat_wall=time.time()

            time.sleep(max(0.02,float(args.fast_loop_seconds)))

    except KeyboardInterrupt:
        log(log_file, "STOP requested by user Ctrl+C")
    except Exception as e:
        log(log_file, f"FATAL ERROR: {e}\n{traceback.format_exc()}")
    finally:
        # Safety flatten on normal/abnormal shutdown during RTH in trading modes.
        try:
            if executor and args.mode in {"paper", "live"} and executor.position_qty() > 0:
                ref = dict(last_feature_row)
                ref.update(last_snapshot)
                res = executor.exit_long(ref, "ENGINE_SHUTDOWN_FLATTEN", force_zero=True)
                if res.get("ok") and res.get("trade_record"):
                    append_csv(trade_file, res["trade_record"])
        except Exception as e:
            log(log_file, f"Shutdown flatten warning: {e}")
        try:
            feature_writer.stop()
            if feature_file.exists():
                with feature_file.open(newline="",encoding="utf-8") as fh:
                    reader=csv.DictReader(fh)
                    keys=[r.get("feature_minute_key_utc","") for r in reader]
                    log(log_file,f"FEATURE_SESSION_AUDIT rows={len(keys)} uniqueMinutes={len(set(keys))} schemaCols={len(reader.fieldnames or [])} expectedCols={len(FEATURE_FIELDS)} duplicates={len(keys)-len(set(keys))}")
        except Exception as e: log(log_file,f"FEATURE_SESSION_AUDIT_ERROR {e}")
        try: moo_sampler.stop()
        except Exception: pass
        try: fast_feed.stop()
        except Exception: pass
        try: moo.close()
        except Exception: pass
        try:
            if executor: executor.close()
        except Exception: pass
        log(log_file, "CLOSED SMCI trading engine")


if __name__ == "__main__":
    main()