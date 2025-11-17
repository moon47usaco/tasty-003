# path: dxlink_quote_refresh_env.py
"""
DXLink Quote stream test (no SDK) with robust SSL handling.

.env required:
  TASTY_BASE=https://api.cert.tastyworks.com
  CLIENT_ID=your_oauth_client_id
  CLIENT_SECRET=your_oauth_client_secret
  TASTY_REFRESH_TOKEN=your_refresh_token
  # optional:
  QUOTE_SYMBOLS=SPY,AAPL
  QUOTE_COUNT=10
  SSL_NO_VERIFY=0          # set to 1 ONLY for temporary debugging

Install:
  pip install python-dotenv requests websockets certifi

Mac tip (if needed): run "Install Certificates.command" in your Python's Applications folder.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple

import certifi
import requests
import websockets
from dotenv import load_dotenv

ISO = "%Y-%m-%dT%H:%M:%S.%fZ"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO)


@dataclass
class QuoteTicket:
    dxlink_url: str
    token: str


def env_list(key: str, default: str) -> List[str]:
    raw = os.getenv(key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


def load_config() -> dict:
    load_dotenv()
    cfg = {
        "base": os.getenv("TASTY_BASE", "https://api.cert.tastyworks.com").rstrip("/"),
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "refresh_token": os.getenv("TASTY_REFRESH_TOKEN"),
        "symbols": env_list("QUOTE_SYMBOLS", "SPY,AAPL"),
        "count": int(os.getenv("QUOTE_COUNT", "10")),
        "no_verify": os.getenv("SSL_NO_VERIFY", "0") in ("1", "true", "True"),
    }
    missing = [k for k in ("client_id", "client_secret", "refresh_token") if not cfg[k]]
    if missing:
        raise RuntimeError(
            "Missing .env keys: " + ", ".join(k.upper() for k in missing)
        )
    return cfg


def make_requests_session(no_verify: bool) -> requests.Session:
    s = requests.Session()
    if no_verify:
        # why: only for emergency debugging; insecure
        s.verify = False
    else:
        s.verify = certifi.where()
    s.headers.update({"Accept": "application/json"})
    return s


def token_from_refresh(sess: requests.Session, base: str, client_id: str, client_secret: str, refresh_token: str) -> str:
    url = f"{base}/oauth/token"
    form = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = sess.post(url, data=form, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"oauth/token (refresh) failed: {resp.status_code} {resp.text}")
    j = resp.json()
    access = j.get("access_token")
    if not access:
        raise RuntimeError(f"oauth/token response missing access_token: {j}")
    return access


def get_dxlink_ticket(sess: requests.Session, base: str, access_token: str) -> QuoteTicket:
    url = f"{base}/api-quote-tokens"
    resp = sess.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"GET /api-quote-tokens failed: {resp.status_code} {resp.text}")
    j = resp.json()
    data = j.get("data") or {}
    token = data.get("token")
    ws_url = data.get("dxlink-url") or data.get("dxlink_url")
    if not token or not ws_url:
        raise RuntimeError(f"Malformed /api-quote-tokens response: {j}")
    return QuoteTicket(dxlink_url=ws_url, token=token)


def make_ssl_context(no_verify: bool) -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=certifi.where())
    if no_verify:
        # why: last-resort, insecure
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def ws_send(ws, msg: dict) -> None:
    await ws.send(json.dumps(msg))


async def keepalive(ws, interval_sec: int = 55) -> None:
    while True:
        await asyncio.sleep(interval_sec)
        await ws_send(ws, {"type": "KEEPALIVE", "channel": 0})


def decode_compact_quotes(payload: list) -> List[Tuple[str, float, float]]:
    out: List[Tuple[str, float, float]] = []
    d = list(payload)
    while d:
        sym, bid, ask = d[:3]
        d = d[3:]
        try:
            out.append((str(sym), float(bid), float(ask)))
        except Exception:
            continue
    return out


async def stream_quotes(ssl_context: ssl.SSLContext, ticket: QuoteTicket, symbols: List[str], max_events: int) -> None:
    async with websockets.connect(ticket.dxlink_url, max_size=None, ping_interval=None, ping_timeout=None, ssl=ssl_context) as ws:
        # SETUP
        await ws_send(ws, {"type": "SETUP", "channel": 0, "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60, "version": "dxlink-probe/0.2"})
        # swallow potential SETUP acks
        for _ in range(2):
            try:
                await ws.recv()
            except Exception:
                break

        # AUTH (with api-quote token)
        await ws_send(ws, {"type": "AUTH", "channel": 0, "token": ticket.token})
        auth_res = json.loads(await ws.recv())
        if auth_res.get("state") != "AUTHORIZED":
            raise RuntimeError(f"dxLink AUTH failed: {auth_res}")

        # FEED channel
        await ws_send(ws, {"type": "CHANNEL_REQUEST", "channel": 1, "service": "FEED", "parameters": {"contract": "AUTO"}})
        opened = json.loads(await ws.recv())
        if opened.get("type") != "CHANNEL_OPENED" or opened.get("channel") != 1:
            raise RuntimeError(f"FEED channel failed: {opened}")

        # FEED_SETUP (COMPACT)
        await ws_send(ws, {
            "type": "FEED_SETUP",
            "channel": 1,
            "acceptAggregationPeriod": 1,
            "acceptDataFormat": "COMPACT",
            "acceptEventFields": {"Quote": ["eventSymbol", "bidPrice", "askPrice"]},
        })
        # optional ack
        try:
            json.loads(await ws.recv())
        except Exception:
            pass

        # SUBSCRIBE
        await ws_send(ws, {"type": "FEED_SUBSCRIPTION", "channel": 1, "add": [{"type": "Quote", "symbol": s} for s in symbols]})

        hb = asyncio.create_task(keepalive(ws))
        printed = 0

        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") != "FEED_DATA":
                continue
            data = msg.get("data")
            if not isinstance(data, list) or not data or data[0] != "Quote":
                continue
            for sym, bid, ask in decode_compact_quotes(data[1]):
                print(f"{now_iso():<27} {sym:>10} bid={bid} ask={ask}")
                printed += 1
                if printed >= max_events:
                    hb.cancel()
                    return


async def main() -> None:
    cfg = load_config()
    try:
        sess = make_requests_session(cfg["no_verify"])
        access = token_from_refresh(sess, cfg["base"], cfg["client_id"], cfg["client_secret"], cfg["refresh_token"])
        ticket = get_dxlink_ticket(sess, cfg["base"], access)
        ssl_ctx = make_ssl_context(cfg["no_verify"])
        await stream_quotes(ssl_ctx, ticket, cfg["symbols"], cfg["count"])
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
