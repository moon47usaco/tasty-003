#Auto centered strike. Needs auto fallowing.
# file: app/main.py
"""
dxFeed + Tastytrade streamer with:
- KEEPALIVE echo + ws pings (stable).
- Center strike selection: uses env FOP_CENTER_STRIKE or auto-compute from underlying via dxLink.
- FIX: symbol normalization so FEED_DATA matches tracked symbols.
- Status shows RX/TX tail + quote match stats.

Run:
  pip install fastapi uvicorn pandas python-dotenv requests websockets==15.* certifi
  # Optional manual center:
  # export FOP_CENTER_STRIKE=4500
  # Auto-select controls:
  # export UNDERLYING_STREAMER_SYMBOL="/MESZ25:XCME"
  # export FOP_STRIKE_STEP=5
  # export FIRST_QUOTE_TIMEOUT_SEC=10
  # export UPDATE_ENV_CENTER=false
  # NEW: force limit price for testing, e.g. always place $0.11 limits:
  # export FORCE_LIMIT_PRICE=0.11
  uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import os
import re
import math
import json
import ssl
import asyncio
import threading
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Deque, Iterable
from collections import deque

import pandas as pd #
import requests
import uvicorn #
from dotenv import load_dotenv #
from fastapi import FastAPI, Request #

from websockets.asyncio.client import connect as ws_connect #
import websockets #

# --------------------------------------------------------------------------
# Load env & App
# --------------------------------------------------------------------------
load_dotenv()
app = FastAPI()

# --------------------------------------------------------------------------
# Tiny utils
# --------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _snap_to_step(price: float, step: float) -> float:
    if step <= 0:
        return price
    return round(price / step) * step

def _upsert_env_line(path: str, key: str, value: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            old = f.read()
    except FileNotFoundError:
        old = ""
    line = f"{key}={value}"
    pat = re.compile(rf"(?m)^\s*{re.escape(key)}\s*=.*$")
    new = pat.sub(line, old, count=1) if pat.search(old) else (old.rstrip() + ("\n" if old and not old.endswith("\n") else "") + line + "\n")
    fd, tmp = tempfile.mkstemp(prefix=".env.", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(new)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
TASTY_BASE = os.getenv("TASTY_BASE", "https://api.cert.tastyworks.com").rstrip("/")
TASTY_USERNAME = os.getenv("TASTY_USERNAME")
TASTY_PASSWORD = os.getenv("TASTY_PASSWORD")
REQ_TIMEOUT = int(os.getenv("REQUESTS_TIMEOUT", "20"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}
TASTY_SIGN_LIMIT_PRICE = os.getenv("TASTY_SIGN_LIMIT_PRICE", "false").lower() in {"1", "true", "yes", "y"}
TASTY_PRECHECK_DRY_RUN = os.getenv("TASTY_PRECHECK_DRY_RUN", "true").lower() in {"1", "true", "yes", "y"}
TASTY_USER_AGENT = os.getenv("TASTY_USER_AGENT", "CodeCopilot/1.0")

FOP_ROOT = os.getenv("FOP_ROOT", "MES").upper()
FOP_TARGET_EXP = os.getenv("FOP_TARGET_EXP", "").strip()
FOP_DTE_LEFT = int(os.getenv("FOP_DTE_LEFT", "1"))
FOP_DTE_RIGHT = int(os.getenv("FOP_DTE_RIGHT", "1"))
FOP_MAX_STRIKES = int(os.getenv("FOP_MAX_STRIKES", "7"))

# Center-strike selection (manual or auto)
_FOP_CENTER_STRIKE_S = os.getenv("FOP_CENTER_STRIKE", "").strip()
FOP_CENTER_STRIKE: Optional[float] = None
if _FOP_CENTER_STRIKE_S:
    try:
        FOP_CENTER_STRIKE = float(_FOP_CENTER_STRIKE_S)
    except ValueError:
        print(f"⚠️ Invalid FOP_CENTER_STRIKE={_FOP_CENTER_STRIKE_S!r}; ignoring")

UNDERLYING_STREAMER_SYMBOL = os.getenv("UNDERLYING_STREAMER_SYMBOL", "/MESZ25:XCME").strip()
FOP_STRIKE_STEP = float(os.getenv("FOP_STRIKE_STEP", "5"))
FIRST_QUOTE_TIMEOUT_SEC = float(os.getenv("FIRST_QUOTE_TIMEOUT_SEC", "10"))
UPDATE_ENV_CENTER = os.getenv("UPDATE_ENV_CENTER", "false").lower() in {"1", "true", "yes", "y"}
ENV_PATH = os.getenv("ENV_PATH", ".env")

# NEW: force limit price for testing (overrides incoming prices everywhere)
_FORCE_LIMIT_PRICE_S = os.getenv("FORCE_LIMIT_PRICE", "").strip()
FORCE_LIMIT_PRICE: Optional[float] = None
if _FORCE_LIMIT_PRICE_S:
    try:
        FORCE_LIMIT_PRICE = float(_FORCE_LIMIT_PRICE_S)
    except ValueError:
        print(f"⚠️ Invalid FORCE_LIMIT_PRICE={_FORCE_LIMIT_PRICE_S!r}; ignoring")

ASK_MIN = float(os.getenv("ASK_MIN", "6.5"))
ASK_MAX = float(os.getenv("ASK_MAX", "8.0"))

DEBUG_DXLINK = os.getenv("DEBUG_DXLINK", "0").lower() in {"1", "true", "yes", "y"}
DXLINK_IDLE_SEC = int(os.getenv("DXLINK_IDLE_SEC", "5"))
REST_POLL_SEC = int(os.getenv("REST_POLL_SEC", "2"))
REST_POLL_ROUNDS = int(os.getenv("REST_POLL_ROUNDS", "15"))
SUBSCRIBE_UNDERLYING = os.getenv("SUBSCRIBE_UNDERLYING", "true").lower() in {"1", "true", "yes", "y"}
DXLINK_READY_TIMEOUT = int(os.getenv("DXLINK_READY_TIMEOUT", "30"))
DXLINK_SSL_NO_VERIFY = os.getenv("DXLINK_SSL_NO_VERIFY", "false").lower() in {"1", "true", "yes", "y"}
DXLINK_DATA_FORMAT = os.getenv("DXLINK_DATA_FORMAT", "").strip().upper()  # "", "JSON", "COMPACT"
FEED_SETUP_ACK_TIMEOUT = int(os.getenv("FEED_SETUP_ACK_TIMEOUT", "8"))
DXLINK_KEEPALIVE_SEC = int(os.getenv("DXLINK_KEEPALIVE_SEC", "20"))
WS_PING_INTERVAL_SEC = int(os.getenv("WS_PING_INTERVAL_SEC", str(max(5, DXLINK_KEEPALIVE_SEC))))
WS_PING_TIMEOUT_SEC = int(os.getenv("WS_PING_TIMEOUT_SEC", "10"))

# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------
selected_call: Optional[Dict[str, Any]] = None
selected_put: Optional[Dict[str, Any]] = None

live_quotes: Dict[str, Dict[str, Any]] = {}  # key: variant symbol -> shared rec
account_headers: Optional[Dict[str, str]] = None
account_number: Optional[str] = None

_last_chain_rows: int = 0
_last_chain_sample: List[Dict[str, Any]] = []
_last_chain_dbg: Dict[str, Any] = {}

_last_quote_time: float = 0.0
_underlying_future_symbol: Optional[str] = None

dx_client: Optional["DXLinkClient"] = None

# quote match diagnostics
_quotes_rx_total: int = 0
_quotes_matched_total: int = 0
_unmatched_syms: Deque[str] = deque(maxlen=25)

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
SIDE_MAP = {
    "buy_to_open": "Buy to Open",
    "sell_to_close": "Sell to Close",
    "sell_to_open": "Sell to Open",
    "buy_to_close": "Buy to Close",
}
_EXP_FROM_SYMBOL_RE = re.compile(r"\s(?P<yymmdd>\d{6})[CP]\d+$")


def _mask(s: str, n: int = 6) -> str:
    if not s:
        return "<empty>"
    return (s[:n] + "*" * 8 + s[-n:]) if len(s) > 2 * n else (s[0] + "*" * (len(s) - 2) + s[-1])


def _mask_url(u: Optional[str]) -> str:
    if not u:
        return "<empty>"
    return re.sub(r"://([^:@/]+):[^@/]+@", r"://\1:***@", u)


def _print_kv(title: str, kv: Dict[str, Any]) -> None:
    print(f"=== {title} ===")
    for k, v in kv.items():
        print(f"{k:>20}: {v}")
    print("====================")


def _safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"__text__": resp.text, "__status__": resp.status_code}


def _derive_exp_from_symbol(call_symbol: Optional[str], put_symbol: Optional[str]) -> Optional[str]:
    for s in (call_symbol, put_symbol):
        if not s or not isinstance(s, str):
            continue
        m = _EXP_FROM_SYMBOL_RE.search(s.strip())
        if m:
            yymmdd = m.group("yymmdd")
            return f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
    return None


def _today_utc() -> datetime.date:
    return datetime.now(timezone.utc).date()


def _proxy_env_snapshot() -> Dict[str, Optional[str]]:
    keys = ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"]
    snap = {k: _mask_url(os.getenv(k)) for k in keys if os.getenv(k)}
    return snap or {"_note": "no proxy env set"}


def _build_ssl_context() -> Tuple[ssl.SSLContext, Dict[str, Any]]:
    ctx = ssl.create_default_context()
    ssl_info: Dict[str, Any] = {
        "openssl": ssl.OPENSSL_VERSION,
        "verify": True,
        "check_hostname": True,
        "cafile": None,
        "no_verify_env": DXLINK_SSL_NO_VERIFY,
    }
    try:
        import certifi
        cafile = certifi.where()
        ctx.load_verify_locations(cafile=cafile)
        ssl_info["cafile"] = cafile
    except Exception as e:
        ssl_info["certifi_error"] = repr(e)
    if DXLINK_SSL_NO_VERIFY:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ssl_info["verify"] = False
        ssl_info["check_hostname"] = False
    return ctx, ssl_info

# Dedicated SSL context for the one-off underlying fetch
def _build_ssl_context_simple() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    if DXLINK_SSL_NO_VERIFY:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx

# ---------------- Symbol normalization ----------------
def _symbol_variants(sym: str) -> List[str]:
    s = str(sym)
    variants = {s}
    base = s.split(":", 1)[0]
    variants.add(base)
    def strip_dots(x: str) -> str:
        if x.startswith("./"):
            return x[2:]
        if x.startswith("."):
            return x[1:]
        return x
    variants.add(strip_dots(s))
    variants.add(strip_dots(base))
    venue = s.split(":", 1)[1] if ":" in s else ""
    nodot = strip_dots(base)
    if venue:
        variants.add(f"{nodot}:{venue}")
    return [v for v in variants if v]

def _register_live_symbol(streamer_symbol: Optional[str], order_symbol: Optional[str], kind: str) -> None:
    rec = {"bid": math.nan, "ask": math.nan, "kind": kind, "order_symbol": str(order_symbol or "")}
    seen: set[str] = set()
    for s in filter(None, [streamer_symbol, order_symbol]):
        for v in _symbol_variants(s):
            if v not in seen:
                live_quotes[v] = rec
                seen.add(v)

# --------------------------------------------------------------------------
# Auth + account
# --------------------------------------------------------------------------
def tasty_login() -> Tuple[Dict[str, str], str]:
    if not TASTY_USERNAME or not TASTY_PASSWORD:
        raise RuntimeError("TASTY_USERNAME/PASSWORD missing in env")
    login_url = f"{TASTY_BASE}/sessions"
    payload = {"login": TASTY_USERNAME, "password": TASTY_PASSWORD}
    base_headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": TASTY_USER_AGENT}
    print(f"🔐 Logging in: {login_url}")
    resp = requests.post(login_url, json=payload, headers=base_headers, timeout=REQ_TIMEOUT)
    resp.raise_for_status()
    root = _safe_json(resp)
    token = root.get("data", {}).get("session-token")
    if not token:
        raise RuntimeError("No session-token in login response")

    header_candidates = [
        {"Authorization": token, "Accept": "application/json", "Content-Type": "application/json", "User-Agent": TASTY_USER_AGENT},
        {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json", "User-Agent": TASTY_USER_AGENT},
    ]
    acct_url = f"{TASTY_BASE}/customers/me/accounts"
    last_err: Optional[Exception] = None
    for idx, h in enumerate(header_candidates):
        try:
            r = requests.get(acct_url, headers=h, timeout=REQ_TIMEOUT)
            r.raise_for_status()
            payload = _safe_json(r)
            items: List[Any] = []
            if isinstance(payload, dict):
                d = payload.get("data")
                if isinstance(d, dict) and isinstance(d.get("items"), list):
                    items = d["items"]
                elif isinstance(d, list):
                    items = d
            if not items:
                raise RuntimeError(f"Accounts API returned no items: {payload}")
            entry = items[0] if isinstance(items[0], dict) else {}
            acct = (entry.get("account") or {}).get("account-number") or entry.get("account-number")
            if not acct:
                raise RuntimeError(f"Could not parse account-number from: {payload}")
            _print_kv("Tastytrade Session", {"token": _mask(token), "account_number": acct, "header_variant": idx + 1})
            return h, acct
        except Exception as e:
            print("⚠️ Account fetch failed with this header style:", e)
            last_err = e
    raise RuntimeError(f"Unable to authenticate to accounts API: {last_err}")

def tasty_headers_account_cached() -> Tuple[Dict[str, str], str]:
    global account_headers, account_number
    if account_headers and account_number:
        return account_headers, account_number
    account_headers, account_number = tasty_login()
    return account_headers, account_number

# --------------------------------------------------------------------------
# Center strike selection (ported from Code 1)
# --------------------------------------------------------------------------
def get_quote_token_and_url(headers: Dict[str, str]) -> Tuple[str, str]:
    r = requests.get(f"{TASTY_BASE}/api-quote-tokens", headers=headers, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    d = _safe_json(r).get("data") or {}
    token = d.get("token")
    url = d.get("dxlink-url") or d.get("dxlink_url")
    if not token or not url:
        raise RuntimeError("Missing dxlink token/url")
    return str(token), str(url)

async def fetch_underlying_mid_once(symbol: str, url: str, token: str, timeout_sec: float) -> Tuple[float, float, float]:
    ssl_ctx = _build_ssl_context_simple()
    async with ws_connect(url, open_timeout=15, ping_interval=20, ping_timeout=10, ssl=ssl_ctx) as ws:
        await ws.send(json.dumps({"type": "SETUP", "channel": 0, "keepaliveTimeout": 40, "acceptKeepaliveTimeout": 40, "version": "center-strike/1.0"}))
        await ws.send(json.dumps({"type": "AUTH", "channel": 0, "token": token}))
        channel = 1
        await ws.send(json.dumps({"type": "CHANNEL_REQUEST", "channel": channel, "service": "FEED", "parameters": {"contract": "AUTO"}}))
        await ws.send(json.dumps({"channel": channel, "type": "FEED_SETUP", "acceptAggregationPeriod": 1, "acceptDataFormat": "COMPACT", "acceptEventFields": {"Quote": ["eventSymbol", "bidPrice", "askPrice"]}}))
        await ws.send(json.dumps({"channel": channel, "type": "FEED_SUBSCRIPTION", "reset": True, "add": [{"type": "Quote", "symbol": symbol}]}))
        deadline = asyncio.get_event_loop().time() + max(1.0, timeout_sec)
        while True:
            remain = deadline - asyncio.get_event_loop().time()
            if remain <= 0:
                raise TimeoutError(f"No quote within {timeout_sec}s for {symbol}")
            raw = await asyncio.wait_for(ws.recv(), timeout=remain)
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if isinstance(msg, dict) and msg.get("type") == "FEED_DATA":
                data = msg.get("data")
                if not (isinstance(data, list) and len(data) >= 2 and data[0] == "Quote"):
                    continue
                payload = data[1]
                if payload and isinstance(payload, list) and payload and isinstance(payload[0], list):
                    flat = []
                    for t in payload:
                        if isinstance(t, list):
                            flat.extend(t)
                    payload = flat
                for i in range(0, len(payload) - 2, 3):
                    sym, bid, ask = payload[i], payload[i + 1], payload[i + 2]
                    if not isinstance(sym, str):
                        continue
                    b = _safe_float(bid); a = _safe_float(ask)
                    if b is None or a is None:
                        continue
                    return (b, a, (a + b) / 2.0)
            if isinstance(msg, dict) and msg.get("type") == "EVENT":
                for ev in msg.get("events") or []:
                    if isinstance(ev, dict) and ev.get("eventType") == "Quote":
                        b = _safe_float(ev.get("bidPrice")); a = _safe_float(ev.get("askPrice"))
                        if b is None or a is None:
                            continue
                        return (b, a, (a + b) / 2.0)

async def ensure_center_strike_selected() -> None:
    """
    If FOP_CENTER_STRIKE is not set, select it by fetching underlying mid and snapping to FOP_STRIKE_STEP.
    """
    global FOP_CENTER_STRIKE
    if FOP_CENTER_STRIKE is not None:
        print(f"🎯 Center strike preset via env: {FOP_CENTER_STRIKE}")
        return
    if not UNDERLYING_STREAMER_SYMBOL:
        print("⚠️ No UNDERLYING_STREAMER_SYMBOL set; cannot auto-select center. Using midpoint fallback later.")
        return
    try:
        headers, _ = tasty_headers_account_cached()
        token, url = get_quote_token_and_url(headers)
        b, a, mid = await fetch_underlying_mid_once(UNDERLYING_STREAMER_SYMBOL, url, token, FIRST_QUOTE_TIMEOUT_SEC)
        strike = _snap_to_step(mid, FOP_STRIKE_STEP)
        FOP_CENTER_STRIKE = strike
        print(f"🎯 Auto-selected center strike: bid={b} ask={a} mid={mid} step={FOP_STRIKE_STEP} -> center={strike}  @ {_now_iso()}")
        if UPDATE_ENV_CENTER:
            try:
                _upsert_env_line(ENV_PATH, "FOP_CENTER_STRIKE", f"{strike:g}")
                print(f"📝 .env updated: FOP_CENTER_STRIKE={strike:g}")
            except Exception as e:
                print("⚠️ Failed to update .env with center strike:", e)
    except Exception as e:
        print("⚠️ Auto center selection failed:", e)

# --------------------------------------------------------------------------
# Chain fetch/parse/filter
# --------------------------------------------------------------------------
def _row_from_strike(expiration_hint: Optional[str], s: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    call_symbol = s.get("call")
    put_symbol = s.get("put")
    exp = expiration_hint or _derive_exp_from_symbol(call_symbol, put_symbol)
    if not exp:
        return None
    try:
        return {
            "expiration": exp,
            "strike": float(s.get("strike-price")) if s.get("strike-price") is not None else None,
            "call_symbol": call_symbol,
            "put_symbol": put_symbol,
            "call_streamer": s.get("call-streamer-symbol"),
            "put_streamer": s.get("put-streamer-symbol"),
        }
    except Exception:
        return None


def _rows_from_list_payload(data_list: List[Any], dbg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    dbg["expirations"] = 0
    dbg["strikes_total"] = 0
    for exp_block in data_list:
        if not isinstance(exp_block, dict):
            continue
        exp_hint = exp_block.get("expiration-date") or exp_block.get("expiration")
        items = exp_block.get("items")
        if isinstance(items, list):
            dbg["expirations"] += 1
            dbg["strikes_total"] += len(items)
            for s in items:
                if isinstance(s, dict):
                    row = _row_from_strike(exp_hint, s)
                    if row:
                        out.append(row)
    return out


def _rows_from_dict_payload(data_dict: Dict[str, Any], dbg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    chains = data_dict.get("option-chains")
    dbg["chains"] = len(chains) if isinstance(chains, list) else 0
    if not isinstance(chains, list):
        return out
    dbg["expirations"] = 0
    dbg["strikes_total"] = 0
    global _underlying_future_symbol
    for chain in chains:
        if not isinstance(chain, dict):
            continue
        futures = data_dict.get("futures")
        if SUBSCRIBE_UNDERLYING and isinstance(futures, list) and futures:
            fsym = futures[0].get("symbol")
            if isinstance(fsym, str):
                _underlying_future_symbol = fsym
        expirations = chain.get("expirations")
        if not isinstance(expirations, list):
            continue
        for exp in expirations:
            if not isinstance(exp, dict):
                continue
            exp_hint = exp.get("expiration-date") or exp.get("expiration")
            strikes = exp.get("strikes")
            if isinstance(strikes, list):
                dbg["expirations"] += 1
                dbg["strikes_total"] += len(strikes)
                for s in strikes:
                    if isinstance(s, dict):
                        row = _row_from_strike(exp_hint, s)
                        if row:
                            out.append(row)
            else:
                items = exp.get("items")
                if isinstance(items, list):
                    dbg["expirations"] += 1
                    dbg["strikes_total"] += len(items)
                    for s in items:
                        if isinstance(s, dict):
                            row = _row_from_strike(exp_hint, s)
                            if row:
                                out.append(row)
    return out


def fetch_fop_chain(root: str) -> pd.DataFrame:
    headers, _ = tasty_headers_account_cached()
    url = f"{TASTY_BASE}/futures-option-chains/{root}/nested"
    r = requests.get(url, headers=headers, timeout=REQ_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Chain fetch failed {r.status_code}: {r.text}")
    body = _safe_json(r)
    if not isinstance(body, dict) or "data" not in body:
        raise RuntimeError(f"Chain payload invalid: {body}")
    data = body["data"]

    dbg: Dict[str, Any] = {"shape": type(data).__name__}
    rows: List[Dict[str, Any]] = []
    if isinstance(data, list):
        rows = _rows_from_list_payload(data, dbg)
    elif isinstance(data, dict):
        rows = _rows_from_dict_payload(data, dbg)
    else:
        raise RuntimeError(f"Chain data unexpected type: {type(data).__name__}: {data}")

    df = pd.DataFrame(rows)
    global _last_chain_rows, _last_chain_sample, _last_chain_dbg
    _last_chain_rows = len(df)
    _last_chain_sample = df.head(5).to_dict(orient="records") if not df.empty else []
    _last_chain_dbg = dbg

    if df.empty:
        print(f"⚠️ Empty chain for {root}  (dbg={dbg})")
        return df

    df["expiration_dt"] = pd.to_datetime(df["expiration"]).dt.tz_localize(None)
    today = _today_utc()
    df["dte"] = (df["expiration_dt"].dt.date - today).apply(lambda d: d.days)
    return df


def select_expiration_window(df: pd.DataFrame, left: int, right: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df[(df["dte"] >= left) & (df["dte"] <= right)].copy()


def filter_chain_by_config(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if FOP_TARGET_EXP:
        m = df[df["expiration"] == FOP_TARGET_EXP].copy()
        if not m.empty:
            return m
        print(f"⚠️ No rows for FOP_TARGET_EXP={FOP_TARGET_EXP}. Falling back to DTE window.")
    return select_expiration_window(df, FOP_DTE_LEFT, FOP_DTE_RIGHT)


def center_strikes(df: pd.DataFrame, max_strikes: int) -> pd.DataFrame:
    if df.empty or max_strikes <= 0:
        return df
    out_parts: List[pd.DataFrame] = []
    half = max_strikes // 2
    for exp, g in df.groupby("expiration"):
        strikes = sorted({s for s in g["strike"].dropna().tolist()})
        if not strikes:
            continue
        if len(strikes) <= max_strikes:
            out_parts.append(g)
            print(f"🪙 Exp {exp}: kept all {len(strikes)} strikes (<= max {max_strikes})")
            continue
        if FOP_CENTER_STRIKE is not None:
            center_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - FOP_CENTER_STRIKE))
            center_val = strikes[center_idx]
            reason = f"manual/auto {FOP_CENTER_STRIKE}→{center_val}"
        else:
            center_idx = len(strikes) // 2
            center_val = strikes[center_idx]
            reason = "midpoint"
        lo = max(0, center_idx - half)
        hi = min(len(strikes), lo + max_strikes)
        lo = max(0, hi - max_strikes)
        keep_set = set(strikes[lo:hi])
        out_parts.append(g[g["strike"].isin(keep_set)])
        print(f"🪙 Exp {exp}: kept {len(keep_set)} strikes centered at {center_val} ({reason}) range {strikes[lo]}..{strikes[hi-1]}")
    if not out_parts:
        return df.iloc[0:0]
    return pd.concat(out_parts, ignore_index=True)

# --------------------------------------------------------------------------
# REST quotes fallback
# --------------------------------------------------------------------------
def tasty_rest_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    headers, _ = tasty_headers_account_cached()
    url = f"{TASTY_BASE}/market-data/quotes"
    params = {"symbols": ",".join(symbols)}
    r = requests.get(url, headers=headers, params=params, timeout=REQ_TIMEOUT)
    if not r.ok:
        print("⚠️ REST quotes error:", r.status_code, r.text)
        return {}
    body = _safe_json(r)
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        sym = item.get("symbol")
        bid = item.get("bid-price")
        ask = item.get("ask-price")
        if isinstance(sym, str) and bid is not None and ask is not None:
            try:
                out[sym] = {"bid": float(bid), "ask": float(ask)}
            except Exception:
                continue
    return out

async def quotes_watchdog(canonical_symbols: List[str]) -> None:
    global _last_quote_time, selected_call, selected_put
    if not canonical_symbols:
        return
    start = datetime.now().timestamp()
    while True:
        await asyncio.sleep(REST_POLL_SEC)
        idle = datetime.now().timestamp() - _last_quote_time
        if idle < DXLINK_IDLE_SEC:
            continue
        q = tasty_rest_quotes(canonical_symbols)
        if q:
            for sym, qa in q.items():
                found = None
                for v in _symbol_variants(sym):
                    if v in live_quotes:
                        found = live_quotes[v]
                        break
                if not found:
                    continue
                found["bid"] = qa["bid"]
                found["ask"] = qa["ask"]
                a = qa["ask"]; b = qa["bid"]
                if found["kind"] == "C" and ASK_MIN <= a <= ASK_MAX:
                    selected_call = {"order_symbol": found["order_symbol"], "ask": a, "bid": b, "streamer": sym}
                    print(f"🧂 (REST) Selected Call {found['order_symbol']} ask={a}")
                elif found["kind"] == "P" and ASK_MIN <= a <= ASK_MAX:
                    selected_put = {"order_symbol": found["order_symbol"], "ask": a, "bid": b, "streamer": sym}
                    print(f"🌶 (REST) Selected Put  {found['order_symbol']} ask={a}")
            _last_quote_time = datetime.now().timestamp()
        if datetime.now().timestamp() - start > REST_POLL_ROUNDS * REST_POLL_SEC:
            break

# --------------------------------------------------------------------------
# DXLink client
# --------------------------------------------------------------------------
class DXLinkClient:
    def __init__(self, headers: Dict[str, str]):
        self.headers = headers
        self.ws: Any = None
        self.connected = asyncio.Event()
        self.stop_flag = False
        self.channel_id = 1
        self._warned_non_dict = False

        self.state = "idle"
        self.ws_url: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_error_repr: Optional[str] = None
        self.connect_started_at: Optional[float] = None
        self.connect_ended_at: Optional[float] = None
        self.last_event_ts: Optional[float] = None
        self.last_msg_type: Optional[str] = None
        self.proxy_env = _proxy_env_snapshot()
        self.websockets_version = getattr(websockets, "__version__", "?")
        self.ssl_info: Dict[str, Any] = {}
        self.last_feed_config: Optional[Dict[str, Any]] = None
        self.accept_format: Optional[str] = None
        self.feed_setup_sent_at: Optional[float] = None
        self.feed_setup_ack_at: Optional[float] = None
        self.feed_handshake_notes: List[str] = []
        self._first_subscription_sent = False
        self._ka_task: Optional[asyncio.Task] = None
        self.keepalive_sec: int = max(5, DXLINK_KEEPALIVE_SEC)

        self.last_keepalive_sent_at: Optional[float] = None
        self.last_keepalive_recv_at: Optional[float] = None

        self._tx_log: Deque[Dict[str, Any]] = deque(maxlen=50)
        self._rx_log: Deque[Dict[str, Any]] = deque(maxlen=50)

    def get_status(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "ws_url": self.ws_url,
            "last_error": self.last_error,
            "last_error_repr": self.last_error_repr,
            "connect_started_at": self.connect_started_at,
            "connect_ended_at": self.connect_ended_at,
            "last_event_ts": self.last_event_ts,
            "last_msg_type": self.last_msg_type,
            "proxy_env": self.proxy_env,
            "websockets_version": self.websockets_version,
            "ssl": self.ssl_info,
            "last_feed_config": self.last_feed_config,
            "accept_format": self.accept_format,
            "feed_setup_sent_at": self.feed_setup_sent_at,
            "feed_setup_ack_at": self.feed_setup_ack_at,
            "feed_handshake_notes": self.feed_handshake_notes,
            "keepalive_sec": self.keepalive_sec,
            "last_keepalive_sent_at": self.last_keepalive_sent_at,
            "last_keepalive_recv_at": self.last_keepalive_recv_at,
            "rx_tail": list(self._rx_log),
            "tx_tail": list(self._tx_log),
            "quotes_rx_total": _quotes_rx_total,
            "quotes_matched_total": _quotes_matched_total,
            "unmatched_sample": list(_unmatched_syms),
        }

    def _get_quote_token(self) -> Tuple[str, str]:
        url = f"{TASTY_BASE}/api-quote-tokens"
        r = requests.get(url, headers=self.headers, timeout=REQ_TIMEOUT)
        if not r.ok:
            raise RuntimeError(f"/api-quote-tokens failed {r.status_code}: {r.text}")
        body = _safe_json(r)
        data = body.get("data", {})
        token = data.get("token")
        ws_url = data.get("dxlink-url") or data.get("dxlink_url")
        if not token or not ws_url:
            raise RuntimeError(f"/api-quote-tokens missing token/url: {body}")
        return str(token), str(ws_url)

    @staticmethod
    def _redact(o: Any) -> Any:
        if isinstance(o, dict):
            return {k: (_mask(v) if k.lower() in {"token", "authorization"} else DXLinkClient._redact(v)) for k, v in o.items()}
        if isinstance(o, list):
            return [DXLinkClient._redact(x) for x in o]
        return o

    async def connect_and_auth(self):
        token, ws_url = self._get_quote_token()
        self.ws_url = ws_url
        self.state = "connecting"
        self.connect_started_at = datetime.now().timestamp()
        print(f"🔌 Connecting DXLink: {ws_url}")

        ssl_ctx, self.ssl_info = _build_ssl_context()

        try:
            self.ws = await ws_connect(
                ws_url,
                open_timeout=15,
                ping_interval=WS_PING_INTERVAL_SEC,
                ping_timeout=WS_PING_TIMEOUT_SEC,
                proxy=None,
                max_size=None,
                ssl=ssl_ctx,
            )
            print("🔌 Connected")
            self.state = "connected"
        except Exception as e:
            self.state = "error"
            self.last_error_repr = repr(e)
            self.last_error = str(e)
            self.connect_ended_at = datetime.now().timestamp()
            print(f"❌ DXLink connect failed: {e!r}")
            raise

        async def _send(obj: Dict[str, Any]) -> None:
            self._tx_log.append({"ts": datetime.now().isoformat(), "out": self._redact(obj)})
            await self.ws.send(json.dumps(obj))

        async def _recv(timeout: float = 10.0) -> Any:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
            try:
                msg = json.loads(raw)
            except Exception:
                msg = raw
            if isinstance(msg, dict):
                self.last_msg_type = msg.get("type")
                self._rx_log.append({"ts": datetime.now().isoformat(), "in": self._redact(msg)})
            else:
                self._rx_log.append({"ts": datetime.now().isoformat(), "in": str(type(msg))})
            self.last_event_ts = datetime.now().timestamp()
            return msg

        # SETUP
        await _send({"type": "SETUP", "channel": 0, "keepaliveTimeout": max(30, DXLINK_KEEPALIVE_SEC + 10), "acceptKeepaliveTimeout": max(30, DXLINK_KEEPALIVE_SEC + 10), "version": "0.1-py/1.0.0"})
        for _ in range(5):
            try:
                m = await _recv(timeout=5)
            except asyncio.TimeoutError:
                break
            if isinstance(m, dict) and m.get("type") in {"SETUP_ACK", "STATE"}:
                continue
            else:
                break

        # AUTH
        await _send({"type": "AUTH", "channel": 0, "token": token})
        while True:
            auth_res = await _recv(timeout=15)
            if isinstance(auth_res, dict) and auth_res.get("state") == "AUTHORIZED":
                self.state = "authorized"
                break

        # FEED channel
        await _send({"type": "CHANNEL_REQUEST", "channel": self.channel_id, "service": "FEED", "parameters": {"contract": "AUTO"}})
        while True:
            m = await _recv(timeout=10)
            if isinstance(m, dict) and m.get("type") == "CHANNEL_OPENED" and m.get("channel") == self.channel_id:
                break

        # FEED_SETUP
        self.accept_format = DXLINK_DATA_FORMAT or "COMPACT"
        await _send({"channel": self.channel_id, "type": "FEED_SETUP", "acceptAggregationPeriod": 1, "acceptDataFormat": self.accept_format, "acceptEventFields": {"Quote": ["eventSymbol", "bidPrice", "askPrice"]}})
        self.feed_setup_sent_at = datetime.now().timestamp()
        got_ack = False
        deadline = self.feed_setup_sent_at + FEED_SETUP_ACK_TIMEOUT
        while True:
            try:
                frame = await _recv(timeout=5)
            except asyncio.TimeoutError:
                if datetime.now().timestamp() >= deadline:
                    break
                continue
            if not isinstance(frame, dict):
                continue
            t = frame.get("type")
            if t == "FEED_CONFIG":
                self.last_feed_config = frame
            elif t == "FEED_SETUP_ACK":
                got_ack = True
                self.feed_setup_ack_at = datetime.now().timestamp()
                break
            elif t in {"STATE", "KEEPALIVE", "CHANNEL_OPENED"}:
                pass
            elif t in {"WARNING", "ERROR"}:
                print("DX:", t, json.dumps(frame, separators=(",", ":"), ensure_ascii=False))
            if datetime.now().timestamp() >= deadline and not got_ack:
                break

        # Ready
        self.connected.set()
        self.state = "feed_ready"
        self.connect_ended_at = datetime.now().timestamp()
        print(f"✅ DXLink FEED ready (ack: {'yes' if got_ack else 'no'})")

        # keepalive
        self._ka_task = asyncio.create_task(self._keepalive_loop())

        # Reader loop
        try:
            while not self.stop_flag:
                envelope = await _recv(timeout=max(2 * DXLINK_KEEPALIVE_SEC, 60))
                msgs = envelope if isinstance(envelope, list) else [envelope]
                for msg in msgs:
                    if not isinstance(msg, dict):
                        if not self._warned_non_dict:
                            print("ℹ️ Ignoring non-dict DXLink frame:", type(msg))
                            self._warned_non_dict = True
                        continue

                    mtype = msg.get("type")
                    if mtype == "KEEPALIVE":
                        await self._reply_keepalive(msg)
                    elif mtype == "FEED_DATA":
                        self._handle_feed_data_compact(msg)
                    elif mtype == "EVENT":
                        events = msg.get("events", [])
                        if isinstance(events, list):
                            for ev in events:
                                if isinstance(ev, dict) and ev.get("eventType") == "Quote":
                                    self._on_quote(ev)
                    elif mtype == "FEED_SUBSCRIPTION_ACK":
                        print("DX: FEED_SUBSCRIPTION_ACK")
                    elif mtype == "SNAPSHOT_RESPONSE":
                        evs = msg.get("events") or []
                        print(f"DX: SNAPSHOT_RESPONSE x{len(evs)}")
                    elif mtype in {"WARNING", "ERROR"}:
                        print("DX:", mtype, json.dumps(msg, separators=(",", ":"), ensure_ascii=False))
        except asyncio.TimeoutError:
            print("⚠️ DXLink recv timeout; connection idle or stalled")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.state = "error"
            self.last_error = str(e)
            self.last_error_repr = repr(e)
            print("DXLink loop error:", e)
        finally:
            try:
                if self._ka_task:
                    self._ka_task.cancel()
            except Exception:
                pass
            try:
                if self.ws and not self.ws.closed:
                    await self.ws.close()
            except Exception:
                pass

    async def _reply_keepalive(self, msg: Dict[str, Any]) -> None:
        self.last_keepalive_recv_at = datetime.now().timestamp()
        ka = {"type": "KEEPALIVE", "channel": 0}
        if "id" in msg:
            ka["id"] = msg["id"]
        await self.ws.send(json.dumps(ka))
        self._tx_log.append({"ts": datetime.now().isoformat(), "out": {"type": "KEEPALIVE", "channel": 0, "id": ka.get("id")}})
        self.last_keepalive_sent_at = datetime.now().timestamp()

    async def _keepalive_loop(self) -> None:
        while not self.stop_flag and self.ws and not self.ws.closed:
            try:
                await self.ws.send(json.dumps({"type": "KEEPALIVE", "channel": 0}))
                self._tx_log.append({"ts": datetime.now().isoformat(), "out": {"type": "KEEPALIVE", "channel": 0}})
                self.last_keepalive_sent_at = datetime.now().timestamp()
                try:
                    pong = await self.ws.ping()
                    await asyncio.wait_for(pong, timeout=WS_PING_TIMEOUT_SEC)
                except Exception:
                    pass
            except Exception as e:
                print("⚠️ KEEPALIVE send failed:", e)
                return
            await asyncio.sleep(DXLINK_KEEPALIVE_SEC)

    def _handle_feed_data_compact(self, msg: Dict[str, Any]) -> None:
        data = msg.get("data")
        if not (isinstance(data, list) and len(data) >= 2):
            return
        dtype, payload = data[0], data[1]
        if dtype != "Quote":
            return
        triples: Iterable[Any]
        if payload and isinstance(payload, list) and payload and isinstance(payload[0], list):
            flat: List[Any] = []
            for t in payload:
                if isinstance(t, list):
                    flat.extend(t)
            triples = flat
        else:
            triples = payload
        for i in range(0, len(triples) - 2, 3):
            sym, bid, ask = triples[i], triples[i + 1], triples[i + 2]
            if not isinstance(sym, str):
                continue
            try:
                b = float(bid); a = float(ask)
            except Exception:
                continue
            self._on_quote({"eventSymbol": sym, "bidPrice": b, "askPrice": a})

    async def subscribe(self, symbols: List[str]) -> None:
        await self.connected.wait()
        if not symbols:
            return
        CHUNK = 200
        first = not self._first_subscription_sent
        for i in range(0, len(symbols), CHUNK):
            chunk = symbols[i : i + CHUNK]
            payload = {"channel": self.channel_id, "type": "FEED_SUBSCRIPTION", "add": [{"type": "Quote", "symbol": s} for s in chunk]}
            if first:
                payload["reset"] = True
                first = False
            await self.ws.send(json.dumps(payload))
            self._tx_log.append({"ts": datetime.now().isoformat(), "out": {"type": "FEED_SUBSCRIPTION", "batch": len(chunk)}})
        self._first_subscription_sent = True
        print(f"DX: FEED_SUBSCRIPTION sent for {len(symbols)} symbols")

    def _on_quote(self, ev: Dict[str, Any]) -> None:
        global _last_quote_time, selected_call, selected_put, _quotes_rx_total, _quotes_matched_total
        sym = ev.get("eventSymbol"); bid = ev.get("bidPrice"); ask = ev.get("askPrice")
        if sym is None or bid is None or ask is None:
            return
        _quotes_rx_total += 1

        q: Optional[Dict[str, Any]] = None
        for v in _symbol_variants(str(sym)):
            q = live_quotes.get(v)
            if q:
                break
        if not q:
            _unmatched_syms.append(str(sym))
            return

        try:
            b = float(bid); a = float(ask)
        except Exception:
            return

        q["bid"] = b; q["ask"] = a
        _quotes_matched_total += 1
        _last_quote_time = datetime.now().timestamp()

        if q["kind"] == "C" and ASK_MIN <= a <= ASK_MAX:
            selected_call = {"order_symbol": q["order_symbol"], "ask": a, "bid": b, "streamer": sym}
            print(f"🧂 Selected Call {q['order_symbol']} ask={a}")
        elif q["kind"] == "P" and ASK_MIN <= a <= ASK_MAX:
            selected_put = {"order_symbol": q["order_symbol"], "ask": a, "bid": b, "streamer": sym}
            print(f"🌶 Selected Put  {q['order_symbol']} ask={a}")

# --------------------------------------------------------------------------
# Pricing helpers
# --------------------------------------------------------------------------
def _effective_limit_price(raw_limit: float, price_effect: str) -> float:
    base = FORCE_LIMIT_PRICE if FORCE_LIMIT_PRICE is not None else raw_limit
    try:
        base = float(base)
    except Exception:
        base = raw_limit
    if not math.isfinite(base):
        base = raw_limit
    if TASTY_SIGN_LIMIT_PRICE:
        if price_effect == "Debit":
            return -abs(base)
        if price_effect == "Credit":
            return abs(base)
    return base

# --------------------------------------------------------------------------
# Orders
# --------------------------------------------------------------------------
def tasty_place_order(symbol: str, action: str, qty: int, limit_price: float) -> Dict[str, Any]:
    headers, acct = tasty_headers_account_cached()
    side = SIDE_MAP.get(action, "Buy to Open")
    price_effect = "Debit" if side.startswith("Buy") else "Credit"
    raw_limit = float(limit_price)
    signed_price = _effective_limit_price(raw_limit, price_effect)
    order = {
        "time-in-force": "Day",
        "order-type": "Limit",
        "price": str(signed_price),
        "price-effect": price_effect,
        "legs": [{"instrument-type": "Future Option", "symbol": symbol, "quantity": int(qty), "action": side}],
    }
    url = f"{TASTY_BASE}/accounts/{acct}/orders"
    _print_kv("Placing Order", {
        "url": url,
        "symbol": symbol,
        "qty": qty,
        "action": side,
        "limit_requested": raw_limit,
        "limit_effective": signed_price,
        "price_effect": price_effect,
        "forced_limit_env": FORCE_LIMIT_PRICE,
    })
    if DRY_RUN:
        print("🚫 DRY_RUN=true -> Skipping live order")
        return {"request": order, "response": {"dry_run": True}}
    resp = requests.post(url, json=order, headers=headers, timeout=REQ_TIMEOUT)
    status = resp.status_code
    body = _safe_json(resp)
    print(f"📬 Tastytrade response {status}", json.dumps(body, indent=2))
    return {"request": order, "response": body, "status": status}

# --------------------------------------------------------------------------
# Bootstrap
# --------------------------------------------------------------------------
async def async_bootstrap() -> None:
    headers, _ = tasty_headers_account_cached()

    # Ensure a center strike is selected/persisted before chain filtering.
    await ensure_center_strike_selected()

    chain = fetch_fop_chain(FOP_ROOT)
    global _last_chain_rows, _last_chain_sample
    _last_chain_rows = len(chain)
    _last_chain_sample = chain.head(5).to_dict(orient="records") if not chain.empty else []

    if chain.empty:
        print("No chain rows found; aborting stream.")
        return

    print(f"DTE base = {_today_utc()} UTC | target_exp={FOP_TARGET_EXP or '<none>'} | dte=[{FOP_DTE_LEFT},{FOP_DTE_RIGHT}]")
    chain = filter_chain_by_config(chain)
    if chain.empty:
        print("No contracts after expiration filter.")
        return

    if FOP_MAX_STRIKES > 0:
        before = len(chain)
        chain = center_strikes(chain, FOP_MAX_STRIKES)
        after = len(chain)
        print(f"🔻 Strike filter applied: rows {before} → {after} (FOP_MAX_STRIKES={FOP_MAX_STRIKES}, center={FOP_CENTER_STRIKE if FOP_CENTER_STRIKE is not None else 'midpoint'})")

    sel_exps = sorted(set(chain["expiration"].tolist()))
    print(f"✅ Selected expirations: {sel_exps}  (rows={len(chain)})")

    # Build symbol lists: streamer + canonical
    streamer_symbols: List[str] = []
    canonical_symbols: List[str] = []

    for _, row in chain.iterrows():
        c_stream = row.get("call_streamer"); p_stream = row.get("put_streamer")
        c_order = row.get("call_symbol");  p_order = row.get("put_symbol")

        if c_order:
            canonical_symbols.append(str(c_order))
            _register_live_symbol(None, str(c_order), "C")
        if p_order:
            canonical_symbols.append(str(p_order))
            _register_live_symbol(None, str(p_order), "P")

        if c_stream:
            streamer_symbols.append(str(c_stream))
            _register_live_symbol(str(c_stream), c_order or "", "C")
        if p_stream:
            streamer_symbols.append(str(p_stream))
            _register_live_symbol(str(p_stream), p_order or "", "P")

    if SUBSCRIBE_UNDERLYING and _underlying_future_symbol:
        fsym = _underlying_future_symbol
        for s in (fsym, f"{fsym}:XCME"):
            streamer_symbols.append(s)
            _register_live_symbol(s, fsym, "F")

    all_subs = sorted(set(streamer_symbols + canonical_symbols))
    print(f"📉 Subscribing to {len(all_subs)} symbols after filtering.")

    global dx_client
    dx_client = DXLinkClient(headers)

    async def run_client():
        try:
            await dx_client.connect_and_auth()
        except Exception:
            pass

    connect_task = asyncio.create_task(run_client())

    try:
        await asyncio.wait_for(dx_client.connected.wait(), timeout=DXLINK_READY_TIMEOUT)
    except asyncio.TimeoutError:
        print(f"❌ DXLink didn’t become ready within {DXLINK_READY_TIMEOUT}s; aborting streamer startup")
        connect_task.cancel()
        return

    sub_task = asyncio.create_task(dx_client.subscribe(all_subs))
    watchdog_task = asyncio.create_task(quotes_watchdog(canonical_symbols))
    await asyncio.gather(connect_task, sub_task, watchdog_task)

# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------
@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {
        "ok": True,
        "tasty_base": TASTY_BASE,
        "dry_run": DRY_RUN,
        "sign_limit_price": TASTY_SIGN_LIMIT_PRICE,
        "precheck_dry_run": TASTY_PRECHECK_DRY_RUN,
        "force_limit_price": FORCE_LIMIT_PRICE,
        "fop_root": FOP_ROOT,
        "dte_window": [FOP_DTE_LEFT, FOP_DTE_RIGHT],
        "target_exp": FOP_TARGET_EXP or None,
        "center_strike": FOP_CENTER_STRIKE,
        "strike_step": FOP_STRIKE_STEP,
        "underlying_streamer_symbol": UNDERLYING_STREAMER_SYMBOL or None,
        "have_selected_call": selected_call is not None,
        "have_selected_put": selected_put is not None,
        "tracked_symbols": len(live_quotes),
        "chain_rows": _last_chain_rows,
        "chain_dbg": _last_chain_dbg,
    }


@app.get("/debug/chain")
def debug_chain() -> Dict[str, Any]:
    return {"rows": _last_chain_rows, "sample": _last_chain_sample, "dbg": _last_chain_dbg}


@app.get("/dx/ping")
def dx_ping() -> Dict[str, Any]:
    idle = max(0, int(datetime.now().timestamp() - _last_quote_time))
    preview = list(live_quotes.keys())[:30]
    return {"ws_seen_any_quote": (_last_quote_time > 0), "tracked_count": len(live_quotes), "idle_sec": idle, "sample_symbols": preview}


@app.get("/dx/status")
def dx_status() -> Dict[str, Any]:
    base = {"websockets_version": getattr(websockets, "__version__", "?"), "ready_timeout_sec": DXLINK_READY_TIMEOUT}
    if dx_client is None:
        base.update({"state": "not_started"})
        base["proxy_env"] = _proxy_env_snapshot()
        return base
    base.update(dx_client.get_status())
    return base


@app.post("/order/dry-run")
async def order_dry_run_api(request: Request) -> Dict[str, Any]:
    data = await request.json()
    action = data.get("action", "buy")
    sentiment = data.get("sentiment", "long")
    quantity = int(data.get("quantity", 1))
    limit_price = float(data.get("limit_price", 0.05))

    opt = selected_call if action == "buy" else selected_put
    if not opt:
        return {"error": "No option selected yet from quotes."}

    trade_action = "buy_to_open" if sentiment in {"long", "short"} else ("sell_to_close" if sentiment == "flat" else "buy_to_open")
    side = SIDE_MAP.get(trade_action, "Buy to Open")
    price_effect = "Debit" if side.startswith("Buy") else "Credit"
    signed_price = _effective_limit_price(limit_price, price_effect)

    headers, acct = tasty_headers_account_cached()
    order = {
        "time-in-force": "Day",
        "order-type": "Limit",
        "price": str(signed_price),
        "price-effect": price_effect,
        "legs": [{"instrument-type": "Future Option", "symbol": opt["order_symbol"], "quantity": int(quantity), "action": side}],
    }
    url = f"{TASTY_BASE}/accounts/{acct}/orders/dry-run"
    resp = requests.post(url, headers=headers, json=order, timeout=REQ_TIMEOUT)
    body = _safe_json(resp)
    return {
        "symbol": opt["order_symbol"],
        "order": order,
        "status": resp.status_code,
        "body": body,
        "debug": {"limit_requested": limit_price, "limit_effective": signed_price, "forced_limit_env": FORCE_LIMIT_PRICE},
    }


@app.post("/tradingview-webhook")
async def tradingview_webhook(request: Request) -> Dict[str, Any]:
    data = await request.json()
    action = data.get("action")
    sentiment = data.get("sentiment")
    quantity = int(data.get("quantity") or 1)

    option = selected_call if action == "buy" else selected_put if action == "sell" else None
    if option is None:
        return {"error": "No option selected yet from quotes."}

    ask_price = float(option.get("ask") or 0.05)
    if sentiment in {"long", "short"}:
        trade_action = "buy_to_open"
    elif sentiment == "flat":
        trade_action = "sell_to_close"
    else:
        trade_action = "buy_to_open"

    limit_price = float(data.get("limit_price") or ask_price)

    trade_json = {
        "symbol": option["order_symbol"],
        "action": trade_action,
        "quantity": int(quantity),
        "limit_price": float(limit_price),
        "class": "option",
        "debug_symbols": {"final": option["order_symbol"], "source": option.get("streamer")},
    }
    print("Generated trade JSON:\n", json.dumps(trade_json, indent=2))

    if TASTY_PRECHECK_DRY_RUN:
        headers, acct = tasty_headers_account_cached()
        side = SIDE_MAP.get(trade_action, "Buy to Open")
        pe = "Debit" if side.startswith("Buy") else "Credit"
        signed_price = _effective_limit_price(limit_price, pe)
        pre_order = {
            "time-in-force": "Day",
            "order-type": "Limit",
            "price": str(signed_price),
            "price-effect": pe,
            "legs": [{"instrument-type": "Future Option", "symbol": option["order_symbol"], "quantity": int(quantity), "action": side}],
        }
        pre = requests.post(f"{TASTY_BASE}/accounts/{acct}/orders/dry-run", headers=headers, json=pre_order, timeout=REQ_TIMEOUT)
        pre_body = _safe_json(pre)
        if not pre.ok:
            return {
                "status": "precheck_failed",
                "trade": trade_json,
                "precheck": {"status": pre.status_code, "body": pre_body},
                "debug": {"limit_requested": limit_price, "limit_effective": signed_price, "forced_limit_env": FORCE_LIMIT_PRICE},
            }

    try:
        order_result = tasty_place_order(
            symbol=trade_json["symbol"],
            action=trade_json["action"],
            qty=trade_json["quantity"],
            limit_price=trade_json["limit_price"],
        )
    except Exception as e:
        print("❌ Failed to place order:", e)
        order_result = {"error": str(e)}
    return {"status": "ok", "trade": trade_json, "order_result": order_result}

# --------------------------------------------------------------------------
# Startup
# --------------------------------------------------------------------------
def start_asyncio_loop_in_thread() -> None:
    loop = asyncio.new_event_loop()

    def runner():
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(async_bootstrap())
        except Exception as e:
            print("Streamer terminated:", e)
        finally:
            loop.close()

    threading.Thread(target=runner, daemon=True).start()


@app.on_event("startup")
def startup_event() -> None:
    start_asyncio_loop_in_thread()

# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
