"""
MEXC Futures Fluctuation Monitor Bot
Version: 9.0.0 — WS depth-based fluctuation counter
  - Real-time WS feed (sub.depth.full) for sub-second latency
  - Counts ≥0.15% bid1/ask1 moves as fluctuations
  - Alerts when ≥4 fluctuations land within 60s
  - Filter: bid1/2/3 and ask1/2/3 each ≥ 10k volume,
            top-3 prices on each side within 0.20% range
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram import F
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mexc_flux_bot")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env file")

# Tunables (env-overridable, runtime-adjustable via commands)
flux_pct:        float = float(os.getenv("FLUX_PCT",        "0.15"))
flux_count:      int   = int(  os.getenv("FLUX_COUNT",      "4"))
flux_window:     int   = int(  os.getenv("FLUX_WINDOW",     "60"))
depth_range_pct: float = float(os.getenv("DEPTH_RANGE_PCT", "0.20"))
depth_min_vol:   float = float(os.getenv("DEPTH_MIN_VOL",   "10000"))

ADMIN_ID = 868931721

WS_URL          = "wss://contract.mexc.com/edge"
MEXC_TICKER_URL = "https://futures.mexc.com/api/v1/contract/ticker"
DEPTH_URL_FMT   = "https://contract.mexc.com/api/v1/contract/depth/{symbol}?limit=5"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://futures.mexc.com/",
    "Origin":  "https://futures.mexc.com",
}

# ---------------------------------------------------------------------------
# Symbol list — verified live on MEXC futures API
# ---------------------------------------------------------------------------

HARDCODED_SYMBOLS: dict[str, str] = {
    # Indices / ETFs
    "NAS100_USDT":      "NAS100",
    "US30_USDT":        "US30",
    "HK50_USDT":        "HK50",
    "JP225_USDT":       "JP225",
    "EWY_USDT":         "EWY",
    "SOXX_USDT":        "SOXX",
    "QQQSTOCK_USDT":    "QQQ",

    # Stocks
    "AAPLSTOCK_USDT":   "AAPL",
    "ABBVSTOCK_USDT":   "ABBV",
    "ABTSTOCK_USDT":    "ABT",
    "ACHRSTOCK_USDT":   "ACHR",
    "ACNSTOCK_USDT":    "ACN",
    "ALBSTOCK_USDT":    "ALB",
    "AMATSTOCK_USDT":   "AMAT",
    "AMDSTOCK_USDT":    "AMD",
    "AMZNSTOCK_USDT":   "AMZN",
    "ARMSTOCK_USDT":    "ARM",
    "ASMLSTOCK_USDT":   "ASML",
    "ASTSSTOCK_USDT":   "ASTS",
    "BABASTOCK_USDT":   "BABA",
    "BACSTOCK_USDT":    "BAC",
    "BBAISTOCK_USDT":   "BBAI",
    "COHRSTOCK_USDT":   "COHR",
    "COINBASE_USDT":    "COIN",
    "COPSTOCK_USDT":    "COP",
    "CRMSTOCK_USDT":    "CRM",
    "CRWDSTOCK_USDT":   "CRWD",
    "CRWVSTOCK_USDT":   "CRWV",
    "CSCOSTOCK_USDT":   "CSCO",
    "CSTOCK_USDT":      "C",
    "CVNASTOCK_USDT":   "CVNA",
    "CVXSTOCK_USDT":    "CVX",
    "FIGSTOCK_USDT":    "FIG",
    "FUTUSTOCK_USDT":   "FUTU",
    "GESTOCK_USDT":     "GE",
    "GEVSTOCK_USDT":    "GEV",
    "GOOGLSTOCK_USDT":  "GOOGL",
    "GSSTOCK_USDT":     "GS",
    "HIMSSTOCK_USDT":   "HIMS",
    "IBMSTOCK_USDT":    "IBM",
    "INTCSTOCK_USDT":   "INTC",
    "INTUSTOCK_USDT":   "INTU",
    "IONQSTOCK_USDT":   "IONQ",
    "IRENSTOCK_USDT":   "IREN",
    "JDSTOCK_USDT":     "JD",
    "JPMSTOCK_USDT":    "JPM",
    "KLACSTOCK_USDT":   "KLAC",
    "KOSTOCK_USDT":     "KO",
    "LINSTOCK_USDT":    "LIN",
    "LLYSTOCK_USDT":    "LLY",
    "LMTSTOCK_USDT":    "LMT",
    "LRCXSTOCK_USDT":   "LRCX",
    "MASTOCK_USDT":     "MA",
    "MELISTOCK_USDT":   "MELI",
    "METASTOCK_USDT":   "META",
    "MRVLSTOCK_USDT":   "MRVL",
    "MSFTSTOCK_USDT":   "MSFT",
    "MSTRSTOCK_USDT":   "MSTR",
    "MUSTOCK_USDT":     "MU",
    "NBISSTOCK_USDT":   "NBIS",
    "NFLXSTOCK_USDT":   "NFLX",
    "NKESTOCK_USDT":    "NKE",
    "NOWSTOCK_USDT":    "NOW",
    "ONDSSTOCK_USDT":   "ONDS",
    "ORCLSTOCK_USDT":   "ORCL",
    "OXYSTOCK_USDT":    "OXY",
    "PANWSTOCK_USDT":   "PANW",
    "PDDSTOCK_USDT":    "PDD",
    "PEPSTOCK_USDT":    "PEP",
    "PGSTOCK_USDT":     "PG",
    "PYPLSTOCK_USDT":   "PYPL",
    "QCOMSTOCK_USDT":   "QCOM",
    "RDDTSTOCK_USDT":   "RDDT",
    "RKLBSTOCK_USDT":   "RKLB",
    "ROBINHOOD_USDT":   "HOOD",
    "RTXSTOCK_USDT":    "RTX",
    "SHOPSTOCK_USDT":   "SHOP",
    "SMCISTOCK_USDT":   "SMCI",
    "SNDKSTOCK_USDT":   "SNDK",
    "SNOWSTOCK_USDT":   "SNOW",
    "SPOTSTOCK_USDT":   "SPOT",
    "STXSTOCK_USDT":    "STX",
    "TSMSTOCK_USDT":    "TSM",
    "TXNSTOCK_USDT":    "TXN",
    "UBERSTOCK_USDT":   "UBER",
    "UNHSTOCK_USDT":    "UNH",
    "VRTSTOCK_USDT":    "VRT",
    "VZSTOCK_USDT":     "VZ",
    "WFCSTOCK_USDT":    "WFC",
    "WMTSTOCK_USDT":    "WMT",
    "XOMSTOCK_USDT":    "XOM",
}

# ---------------------------------------------------------------------------
# Subscriber storage — Upstash Redis (REST) primary, JSON file fallback
# ---------------------------------------------------------------------------

import urllib.request
import urllib.error

SUBSCRIBERS_FILE = Path(os.getenv("SUBSCRIBERS_FILE") or (Path(__file__).parent / "subscribers.json"))

UPSTASH_URL   = (os.getenv("UPSTASH_REDIS_REST_URL")   or "").rstrip("/")
UPSTASH_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN") or "")
UPSTASH_KEY   = os.getenv("UPSTASH_SUBS_KEY", "stocksplash:subscribers")


def _upstash_enabled() -> bool:
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


def _upstash_command(command: list[str]) -> Optional[dict]:
    body = json.dumps(command).encode("utf-8")
    req = urllib.request.Request(
        UPSTASH_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {UPSTASH_TOKEN}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        logger.error("Upstash HTTP %s on %s: %s", exc.code, command[0], exc.read()[:200])
    except Exception as exc:
        logger.error("Upstash error on %s: %s", command[0], exc)
    return None


def _upstash_load() -> Optional[dict[int, dict]]:
    resp = _upstash_command(["GET", UPSTASH_KEY])
    if resp is None:
        return None
    val = resp.get("result")
    if not val:
        return {}
    try:
        data = json.loads(val)
        return {int(k): v for k, v in data.items()}
    except Exception as exc:
        logger.error("Upstash subscribers parse error: %s", exc)
        return None


def _upstash_save() -> bool:
    payload = json.dumps(subscribers)
    resp = _upstash_command(["SET", UPSTASH_KEY, payload])
    return bool(resp and resp.get("result") == "OK")


def load_subscribers() -> dict[int, dict]:
    if _upstash_enabled():
        data = _upstash_load()
        if data is not None:
            logger.info("Loaded %d subscribers from Upstash", len(data))
            return data
        logger.warning("Upstash load failed — falling back to local file")

    if SUBSCRIBERS_FILE.exists():
        try:
            data = json.loads(SUBSCRIBERS_FILE.read_text())
            return {int(k): v for k, v in data.items()}
        except Exception as exc:
            logger.error("Failed to load subscribers: %s", exc)
    return {}


def save_subscribers() -> None:
    if _upstash_enabled():
        if _upstash_save():
            return
        logger.warning("Upstash save failed — writing to local file as fallback")
    try:
        SUBSCRIBERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SUBSCRIBERS_FILE.write_text(json.dumps(subscribers, indent=2))
    except Exception as exc:
        logger.error("Failed to save subscribers: %s", exc)


subscribers: dict[int, dict] = load_subscribers()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


@dataclass
class FluxTracker:
    """Per-symbol fluctuation state."""
    bid_marker: float = 0.0
    ask_marker: float = 0.0
    events: deque = field(default_factory=deque)  # list[float] timestamps
    total_flux: int   = 0       # lifetime fluctuations seen
    total_alerts: int = 0       # lifetime alerts sent
    blocks: dict      = field(default_factory=dict)  # reason → count
    last_block:  str  = ""
    last_block_ts: float = 0.0


@dataclass
class DepthBook:
    """Latest top-5 depth snapshot from sub.depth.full."""
    bids: list = field(default_factory=list)  # [[price, vol, count], ...] descending
    asks: list = field(default_factory=list)  # ascending
    ts: float = 0.0


MONITORED_SYMBOLS: set[str] = set()
trackers:        dict[str, FluxTracker] = {}
depth_books:     dict[str, DepthBook]   = {}
contract_sizes:  dict[str, float]       = {}  # symbol → contractSize (e.g. 0.01 for AAPL)

symbols_discovered: bool = False
banned_symbols: set[str]        = set()
frozen_symbols: dict[str, float] = {}
muted_until:    float           = 0.0

# Test mode (one-shot)
test_mode:    bool          = False
test_chat_id: Optional[int] = None

# WebSocket health for /wsstatus
ws_state: dict = {
    "connected":     False,
    "connect_time":  0.0,
    "last_msg_ts":   0.0,
    "msg_count":     0,
    "subscribed":    0,
    "reconnects":    0,
    "msgs_last_10s": deque(),
    "errors":        deque(maxlen=10),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id == ADMIN_ID


async def reply(message: Message, text: str, **kwargs) -> None:
    thread_id = message.message_thread_id if message.is_topic_message else None
    await message.bot.send_message(
        chat_id=message.chat.id,
        text=text,
        message_thread_id=thread_id,
        parse_mode="HTML",
        **kwargs,
    )


def find_symbol(user_input: str) -> Optional[str]:
    s = user_input.upper().strip()
    if s in MONITORED_SYMBOLS:
        return s
    if s + "_USDT" in MONITORED_SYMBOLS:
        return s + "_USDT"
    for sym, base in HARDCODED_SYMBOLS.items():
        if base.upper() == s and sym in MONITORED_SYMBOLS:
            return sym
    for sym in MONITORED_SYMBOLS:
        if sym.split("_")[0].upper() == s:
            return sym
    return None


def is_muted() -> bool:
    return time.time() < muted_until


def mute_remaining() -> str:
    remaining = muted_until - time.time()
    if remaining <= 0:
        return ""
    h = int(remaining // 3600)
    m = int((remaining % 3600) // 60)
    s = int(remaining % 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# ---------------------------------------------------------------------------
# Symbol discovery
# ---------------------------------------------------------------------------


def discover_symbols(all_tickers: list[dict]) -> tuple[dict[str, str], list[str]]:
    api_symbol_set = {t.get("symbol", "") for t in all_tickers}
    found:     dict[str, str] = {}
    not_found: list[str]      = []
    for mexc_sym, base in HARDCODED_SYMBOLS.items():
        if mexc_sym in api_symbol_set:
            found[mexc_sym] = base
        else:
            not_found.append(base)
    return found, not_found


async def fetch_raw_tickers(session: aiohttp.ClientSession) -> Optional[list[dict]]:
    try:
        async with session.get(
            MEXC_TICKER_URL, headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30), ssl=True,
        ) as response:
            if response.status != 200:
                logger.warning("HTTP %d from API", response.status)
                return None
            payload: dict = await response.json(content_type=None)
            if not payload.get("success"):
                logger.warning("API returned success=false")
                return None
            return payload.get("data") or None
    except asyncio.TimeoutError:
        logger.warning("Ticker request timed out")
    except aiohttp.ClientConnectionError as exc:
        logger.warning("Connection error: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
    return None


# ---------------------------------------------------------------------------
# Fluctuation logic
# ---------------------------------------------------------------------------


def update_flux(symbol: str, now: float, bid1: float, ask1: float) -> int:
    """
    Each ≥flux_pct move on bid1 OR ask1 (any direction) since the last marker
    counts as one fluctuation; markers reset to current price after counting.
    Returns the rolling count within flux_window.
    """
    tr = trackers.setdefault(symbol, FluxTracker())

    if tr.bid_marker <= 0:
        tr.bid_marker = bid1
    elif abs(bid1 - tr.bid_marker) / tr.bid_marker * 100.0 >= flux_pct:
        tr.events.append(now)
        tr.bid_marker = bid1
        tr.total_flux += 1

    if tr.ask_marker <= 0:
        tr.ask_marker = ask1
    elif abs(ask1 - tr.ask_marker) / tr.ask_marker * 100.0 >= flux_pct:
        tr.events.append(now)
        tr.ask_marker = ask1
        tr.total_flux += 1

    cutoff = now - flux_window
    while tr.events and tr.events[0] < cutoff:
        tr.events.popleft()

    return len(tr.events)


def _level_usd(symbol: str, price: float, size: float) -> float:
    """USD notional at one orderbook level: contracts × contractSize × price."""
    cs = contract_sizes.get(symbol, 1.0)
    return size * cs * price


def passes_depth_filter(symbol: str, book: DepthBook) -> tuple[bool, str]:
    """Filter on USD notional + top-3 tightness. Each filter is bypassed
    when its threshold is set to 0."""
    if len(book.bids) < 3 or len(book.asks) < 3:
        return False, f"depth thin (b={len(book.bids)} a={len(book.asks)})"

    if depth_min_vol > 0:
        for i, lvl in enumerate(book.bids[:3]):
            usd = _level_usd(symbol, float(lvl[0]), float(lvl[1]))
            if usd < depth_min_vol:
                return False, f"bid{i+1} ${usd:.0f} < ${depth_min_vol:.0f}"
        for i, lvl in enumerate(book.asks[:3]):
            usd = _level_usd(symbol, float(lvl[0]), float(lvl[1]))
            if usd < depth_min_vol:
                return False, f"ask{i+1} ${usd:.0f} < ${depth_min_vol:.0f}"

    if depth_range_pct > 0:
        bid1p, bid3p = float(book.bids[0][0]), float(book.bids[2][0])
        ask1p, ask3p = float(book.asks[0][0]), float(book.asks[2][0])
        bid_spread = (bid1p - bid3p) / bid1p * 100.0 if bid1p > 0 else 999.0
        ask_spread = (ask3p - ask1p) / ask1p * 100.0 if ask1p > 0 else 999.0
        if bid_spread > depth_range_pct:
            return False, f"bid spread {bid_spread:.3f}% > {depth_range_pct:.2f}%"
        if ask_spread > depth_range_pct:
            return False, f"ask spread {ask_spread:.3f}% > {depth_range_pct:.2f}%"

    return True, "ok"


# ---------------------------------------------------------------------------
# Alert
# ---------------------------------------------------------------------------


def build_flux_message(
    symbol: str,
    count: int,
    span: float,
    bid1: float,
    ask1: float,
    is_test: bool = False,
) -> str:
    display = HARDCODED_SYMBOLS.get(symbol, symbol.replace("_", ""))
    header  = "🧪 <b>[TEST]</b> " if is_test else "🚨 "
    return (
        f"{header}<b>${display}</b>  {count} fluxes / {int(span)}s  (≥{flux_pct}%)\n"
        f"MEXC bid <b>${bid1:.4f}</b>  ask <b>${ask1:.4f}</b>"
    )


def build_trade_keyboard(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📊 MEXC Futures",
            url=f"https://futures.mexc.com/exchange/{symbol}",
        )
    ]])


async def broadcast_alert(bot: Bot, text: str, keyboard: InlineKeyboardMarkup) -> None:
    """Fan-out an alert to every subscriber's DM with the bot."""
    if not subscribers:
        logger.info("No subscribers — alert not delivered")
        return
    sent, dead = 0, []
    for chat_id in list(subscribers.keys()):
        try:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
            sent += 1
            await asyncio.sleep(0.03)
        except Exception as exc:
            msg = str(exc).lower()
            if any(x in msg for x in ("blocked", "not found", "deactivated", "user is deactivated")):
                dead.append(chat_id)
            else:
                logger.warning("Alert send failed to %s: %s", chat_id, exc)
    for cid in dead:
        subscribers.pop(cid, None)
    if dead:
        save_subscribers()
    logger.info("Alert fanned out: sent=%d dead=%d", sent, len(dead))


async def send_flux_alert(
    bot: Bot,
    symbol: str,
    count: int,
    span: float,
    bid1: float,
    ask1: float,
    is_test: bool = False,
    chat_id: Optional[int] = None,
) -> None:
    text     = build_flux_message(symbol, count, span, bid1, ask1, is_test)
    keyboard = build_trade_keyboard(symbol)
    if chat_id:
        try:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
        except Exception as exc:
            logger.error("Test alert failed: %s", exc)
    else:
        await broadcast_alert(bot, text, keyboard)


# ---------------------------------------------------------------------------
# Process incoming depth pushes (the hot path)
# ---------------------------------------------------------------------------


async def process_depth_update(bot: Bot, symbol: str, data: dict) -> None:
    global test_mode, test_chat_id

    bids = data.get("bids") or []
    asks = data.get("asks") or []
    if not bids or not asks:
        return

    now = time.time()
    book = depth_books.setdefault(symbol, DepthBook())
    book.bids = bids
    book.asks = asks
    book.ts   = now

    if symbol in banned_symbols:
        return
    if symbol in frozen_symbols and frozen_symbols[symbol] > now:
        return

    try:
        bid1 = float(bids[0][0])
        ask1 = float(asks[0][0])
    except (IndexError, ValueError, TypeError):
        return
    if bid1 <= 0 or ask1 <= 0:
        return

    count = update_flux(symbol, now, bid1, ask1)

    target = 1 if test_mode else flux_count
    if count < target:
        return

    if is_muted() and not test_mode:
        return

    tr = trackers[symbol]
    if not test_mode:
        passes, reason = passes_depth_filter(symbol, book)
        if not passes:
            short = reason.split(" ")[0]  # e.g. "bid1" / "bid" / "ask1" / "depth"
            tr.blocks[short]    = tr.blocks.get(short, 0) + 1
            tr.last_block       = reason
            tr.last_block_ts    = now
            logger.info("flux %s would-fire but blocked: %s", symbol, reason)
            return

    span = (now - tr.events[0]) if tr.events else 0.0
    tr.total_alerts += 1

    is_test = test_mode
    target_chat = None
    if is_test:
        test_mode = False
        target_chat = test_chat_id  # always DM the requester for /test

    tr.events.clear()

    asyncio.create_task(
        send_flux_alert(bot, symbol, count, span, bid1, ask1,
                        is_test=is_test, chat_id=target_chat)
    )


# ---------------------------------------------------------------------------
# WebSocket loop
# ---------------------------------------------------------------------------


async def ws_loop(bot: Bot) -> None:
    """Maintains a persistent WS connection with auto-reconnect."""
    backoff = 5
    sub_session = aiohttp.ClientSession()
    try:
        while True:
            try:
                logger.info("WS connecting to %s", WS_URL)
                ws_state["connected"] = False
                async with sub_session.ws_connect(
                    WS_URL,
                    heartbeat=20,
                    autoping=True,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as ws:
                    ws_state["connected"]    = True
                    ws_state["connect_time"] = time.time()
                    ws_state["msg_count"]    = 0
                    ws_state["subscribed"]   = 0
                    ws_state["msgs_last_10s"].clear()
                    backoff = 5
                    logger.info("WS connected — subscribing to %d symbols", len(MONITORED_SYMBOLS))

                    # Subscribe in small batches to be polite
                    sent = 0
                    for sym in sorted(MONITORED_SYMBOLS):
                        try:
                            await ws.send_json({
                                "method": "sub.depth.full",
                                "param": {"symbol": sym, "limit": 5},
                            })
                            sent += 1
                            if sent % 10 == 0:
                                await asyncio.sleep(0.25)
                        except Exception as exc:
                            logger.warning("WS sub %s failed: %s", sym, exc)
                    ws_state["subscribed"] = sent
                    logger.info("WS subscribed to %d symbols", sent)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            now = time.time()
                            ws_state["last_msg_ts"] = now
                            ws_state["msg_count"]  += 1
                            ws_state["msgs_last_10s"].append(now)
                            cutoff = now - 10.0
                            while ws_state["msgs_last_10s"] and ws_state["msgs_last_10s"][0] < cutoff:
                                ws_state["msgs_last_10s"].popleft()

                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                            ch = data.get("channel")
                            if ch == "push.depth.full":
                                sym = data.get("symbol")
                                d   = data.get("data") or {}
                                if sym in MONITORED_SYMBOLS:
                                    await process_depth_update(bot, sym, d)
                            elif ch and ch.startswith("rs."):
                                if data.get("data") != "success":
                                    logger.warning("WS sub error: %s", data)
                            # ignore pong, etc.
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("WS closed/error: %s", msg)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                ws_state["connected"]  = False
                ws_state["reconnects"] += 1
                ws_state["errors"].append(f"{time.strftime('%H:%M:%S')} {type(exc).__name__}: {exc}")
                logger.warning("WS error: %s — reconnect in %ds", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            # Clean exit from `async with` → reconnect
            ws_state["connected"]  = False
            ws_state["reconnects"] += 1
            await asyncio.sleep(2)
    finally:
        await sub_session.close()


# ---------------------------------------------------------------------------
# Symbol-discovery bootstrap (one-shot REST hit on startup)
# ---------------------------------------------------------------------------


async def fetch_contract_sizes(session: aiohttp.ClientSession) -> None:
    """Populate contract_sizes from /contract/detail. Each contract represents
    contractSize units of the underlying — needed to convert raw orderbook
    volumes into USD notional."""
    try:
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/detail",
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=True,
        ) as r:
            if r.status != 200:
                logger.warning("contract/detail HTTP %d", r.status)
                return
            payload = await r.json(content_type=None)
            for c in payload.get("data") or []:
                sym = c.get("symbol")
                cs  = c.get("contractSize")
                if sym and cs:
                    contract_sizes[sym] = float(cs)
        logger.info("Loaded %d contract sizes", len(contract_sizes))
    except Exception as exc:
        logger.error("contract/detail fetch failed: %s", exc)


async def bootstrap_symbols(session: aiohttp.ClientSession) -> None:
    global symbols_discovered, MONITORED_SYMBOLS

    raw = await fetch_raw_tickers(session)
    if not raw:
        logger.error("Symbol discovery failed — bot cannot start")
        return

    found, not_found = discover_symbols(raw)
    MONITORED_SYMBOLS = set(found.keys())
    for sym in MONITORED_SYMBOLS:
        trackers[sym]    = FluxTracker()
        depth_books[sym] = DepthBook()

    await fetch_contract_sizes(session)
    symbols_discovered = True

    missing_size = [s for s in MONITORED_SYMBOLS if s not in contract_sizes]
    if missing_size:
        logger.warning("No contractSize for: %s", ", ".join(sorted(missing_size)))

    logger.info("Bootstrap: %d/%d symbols matched (%d missing)",
                len(MONITORED_SYMBOLS), len(HARDCODED_SYMBOLS), len(not_found))
    if not_found:
        logger.warning("Not found on MEXC: %s", ", ".join(sorted(not_found)))


# ---------------------------------------------------------------------------
# Auto-mute scheduler (unchanged)
# ---------------------------------------------------------------------------


async def _notify_subscribers(bot: Bot, text: str) -> None:
    """Plain DM notification (no inline keyboard) to all subscribers."""
    for chat_id in list(subscribers.keys()):
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            await asyncio.sleep(0.03)
        except Exception:
            pass


async def auto_mute_scheduler(bot: Bot) -> None:
    global muted_until
    last_muted:   dict[str, object] = {}
    last_unmuted: dict[str, object] = {}
    logger.info("Auto-mute scheduler started")

    while True:
        await asyncio.sleep(20)
        try:
            import datetime
            now_utc = datetime.datetime.utcnow()
            weekday = now_utc.weekday()
            today   = now_utc.date()
            h, m    = now_utc.hour, now_utc.minute
            if weekday >= 5:
                continue

            windows = {
                "00:00": {
                    "active":    h == 0 and m < 5,
                    "duration":  5 * 60,
                    "mute_msg":  "🔇 <b>Auto-muted</b> — 00:00 UTC pause (5 min).",
                    "unmute_h":  0, "unmute_m": 5,
                    "unmute_msg": "🔊 <b>Auto-unmuted</b> — 00:05 UTC.",
                },
                "13:29": {
                    "active":    (h == 13 and m >= 29) or (h == 14 and m < 30),
                    "duration":  61 * 60,
                    "mute_msg":  "🔇 <b>Auto-muted</b> — market open window (13:29–14:30 UTC).",
                    "unmute_h":  14, "unmute_m": 30,
                    "unmute_msg": "🔊 <b>Auto-unmuted</b> — market open window passed.",
                },
            }
            for key, w in windows.items():
                if w["active"] and last_muted.get(key) != today:
                    muted_until         = time.time() + w["duration"]
                    last_muted[key]     = today
                    logger.info("Auto-mute: %s window", key)
                    await _notify_subscribers(bot, w["mute_msg"])
                elif (not w["active"]
                      and last_muted.get(key) == today
                      and last_unmuted.get(key) != today
                      and h >= w["unmute_h"] and m >= w["unmute_m"]):
                    muted_until          = 0.0
                    last_unmuted[key]    = today
                    logger.info("Auto-unmute: %s window passed", key)
                    await _notify_subscribers(bot, w["unmute_msg"])
        except Exception as exc:
            logger.error("Auto-mute scheduler error: %s", exc)


# ---------------------------------------------------------------------------
# Router / commands
# ---------------------------------------------------------------------------

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user = message.from_user
    if user is None or message.chat.type != "private":
        await reply(message, "ℹ️ Open a private chat with the bot and send /start there to subscribe to alerts.")
        return

    chat_id = message.chat.id
    name     = user.full_name or "—"
    username = f"@{user.username}" if user.username else ""
    if chat_id in subscribers:
        await reply(message,
            "✅ Already subscribed.\n"
            "You'll receive alerts directly here. Use /stop to unsubscribe."
        )
        return

    subscribers[chat_id] = {
        "name":      name,
        "username":  username,
        "joined_at": int(time.time()),
    }
    save_subscribers()
    await reply(message,
        f"✅ <b>Subscribed!</b>\n"
        f"You'll receive flux alerts here as DMs.\n\n"
        f"⚙️ Default rules: {flux_count}× ≥{flux_pct}% within {flux_window}s, "
        f"depth ≥${depth_min_vol:.0f} on top-3 levels, spread ≤{depth_range_pct}%.\n\n"
        f"Use /help to see all commands. /stop to unsubscribe."
    )


@router.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    chat_id = message.chat.id
    if chat_id not in subscribers:
        await reply(message, "ℹ️ You weren't subscribed.")
        return
    subscribers.pop(chat_id, None)
    save_subscribers()
    await reply(message, "👋 Unsubscribed. Send /start any time to resume.")


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    admin_section = ""
    if is_admin(message):
        admin_section = (
            "\n\n<b>🔐 Admin commands:</b>\n"
            "/threshold 0.15 — fluctuation %% (default 0.15)\n"
            "/count 4 — required fluctuations (default 4)\n"
            "/window 60 — rolling window seconds\n"
            "/depthrange 0.20 — max top-3 spread %%\n"
            "/depthvol 10000 — min USD per level (top-3 each side)\n"
            "/debugflux [TICKER] — show why alerts (don't) fire\n"
            "/wsstatus — verify the websocket feed\n"
            "/mute 30 — mute ALL alerts for N minutes\n"
            "/unmute — unmute immediately\n"
            "/ban TSLA — silence a ticker permanently\n"
            "/unban TSLA — restore banned ticker\n"
            "/freeze TSLA 30 — silence ticker for N minutes\n"
            "/unfreeze TSLA — restore frozen ticker early\n"
            "/blocked — show all banned + frozen tickers\n"
            "/symbols — all monitored symbols with status\n"
            "/subscribers — list all subscribers\n"
            "/kick ID — remove a subscriber\n"
            "/broadcast msg — send to all subscribers\n"
            "/debug — raw API diagnostic\n"
            "/test — fire a test alert at first 0.15%% wiggle\n"
        )

    await reply(message,
        "<b>📖 MEXC Flux Monitor — Help</b>\n\n"
        "<b>📊 Info:</b>\n"
        "/start — subscribe to DM alerts\n"
        "/stop — unsubscribe\n"
        "/status — live stats\n"
        "/symbols — all monitored symbols\n"
        "/book TICKER — show top-5 bid/ask\n"
        "/wsstatus — websocket health\n"
        "/help — this message"
        f"{admin_section}\n\n"
        "<b>ℹ️ How it works:</b>\n"
        f"Watches {len(MONITORED_SYMBOLS)} MEXC futures via WebSocket depth.\n"
        f"Each ≥{flux_pct}% move on bid1 OR ask1 = 1 fluctuation.\n"
        f"When {flux_count} fluxes hit within {flux_window}s and the\n"
        f"orderbook is liquid (top-3 each ≥${depth_min_vol:.0f} USD,\n"
        f"top-3 prices within {depth_range_pct}%) → 🚨 alert sent to your DM."
    )


@router.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    global flux_pct
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, f"Current fluctuation %: <b>{flux_pct}</b>\nUsage: <code>/threshold 0.15</code>")
        return
    try:
        new_val = float(args[0])
        if new_val <= 0 or new_val > 10:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Must be a number between 0.01 and 10.")
        return
    old = flux_pct
    flux_pct = new_val
    await reply(message, f"✅ Fluctuation %: <b>{old}</b> → <b>{new_val}</b>")


@router.message(Command("count"))
async def cmd_count(message: Message) -> None:
    global flux_count
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, f"Current count: <b>{flux_count}</b>\nUsage: <code>/count 4</code>")
        return
    try:
        new_val = int(args[0])
        if new_val < 2 or new_val > 50:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Must be an integer 2..50.")
        return
    old = flux_count
    flux_count = new_val
    await reply(message, f"✅ Count: <b>{old}</b> → <b>{new_val}</b>")


@router.message(Command("window"))
async def cmd_window(message: Message) -> None:
    global flux_window
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, f"Current window: <b>{flux_window}s</b>\nUsage: <code>/window 60</code>")
        return
    try:
        new_val = int(args[0])
        if new_val < 5 or new_val > 600:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Seconds must be 5..600.")
        return
    old = flux_window
    flux_window = new_val
    await reply(message, f"✅ Window: <b>{old}s</b> → <b>{new_val}s</b>")


@router.message(Command("depthrange"))
async def cmd_depthrange(message: Message) -> None:
    global depth_range_pct
    args = (message.text or "").split()[1:]
    if not args:
        cur = f"{depth_range_pct}% (off)" if depth_range_pct <= 0 else f"{depth_range_pct}%"
        await reply(message, f"Top-3 spread cap: <b>{cur}</b>\nUsage: <code>/depthrange 0.20</code>  (set <code>0</code> to disable)")
        return
    try:
        new_val = float(args[0])
        if new_val < 0 or new_val > 100:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Must be 0..100 (0 disables this filter).")
        return
    old = depth_range_pct
    depth_range_pct = new_val
    suffix = " (off)" if new_val == 0 else ""
    await reply(message, f"✅ Depth range: <b>{old}%</b> → <b>{new_val}%</b>{suffix}")


@router.message(Command("depthvol"))
async def cmd_depthvol(message: Message) -> None:
    global depth_min_vol
    args = (message.text or "").split()[1:]
    if not args:
        cur = f"${depth_min_vol:.0f} (off)" if depth_min_vol <= 0 else f"${depth_min_vol:.0f}"
        await reply(message, f"Min USD per level: <b>{cur}</b>\nUsage: <code>/depthvol 10000</code>  (set <code>0</code> to disable)")
        return
    try:
        new_val = float(args[0])
        if new_val < 0:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Must be 0 or positive (0 disables this filter).")
        return
    old = depth_min_vol
    depth_min_vol = new_val
    suffix = " (off)" if new_val == 0 else ""
    await reply(message, f"✅ Min USD/level: <b>${old:.0f}</b> → <b>${new_val:.0f}</b>{suffix}")


@router.message(Command("mute"))
async def cmd_mute(message: Message) -> None:
    global muted_until
    args = (message.text or "").split()[1:]
    if not args:
        if is_muted():
            await reply(message, f"🔇 Muted — <b>{mute_remaining()}</b> remaining.\nUse /unmute to restore.")
        else:
            await reply(message,
                "Usage:\n"
                "<code>/mute 30</code> — mute all alerts for 30 minutes\n"
                "<code>/mute TSLA 30</code> — mute TSLA specifically for 30 minutes")
        return

    ticker = find_symbol(args[0])
    if ticker:
        if len(args) < 2:
            await reply(message, f"Usage: <code>/mute {args[0].upper()} 30</code>")
            return
        try:
            minutes = float(args[1])
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await reply(message, "❌ Minutes must be a positive number.")
            return
        frozen_symbols[ticker] = time.time() + minutes * 60
        display = HARDCODED_SYMBOLS.get(ticker, ticker)
        h, m = int(minutes // 60), int(minutes % 60)
        await reply(message,
            f"🔇 <b>{display}</b> muted for <b>{'{}h {}m'.format(h,m) if h else '{}m'.format(m)}</b>\n"
            f"Use <code>/unfreeze {display}</code> to restore early.")
        return

    try:
        minutes = float(args[0])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Unknown symbol or invalid number.")
        return
    muted_until = time.time() + minutes * 60
    h, m = int(minutes // 60), int(minutes % 60)
    await reply(message, f"🔇 <b>Muted for {'{}h {}m'.format(h,m) if h else '{}m'.format(m)}</b>\nUse /unmute to restore.")


@router.message(Command("unmute"))
async def cmd_unmute(message: Message) -> None:
    global muted_until
    if not is_muted():
        await reply(message, "ℹ️ Bot is not muted.")
        return
    muted_until = 0.0
    await reply(message, "🔊 <b>Alerts restored!</b>")


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not symbols_discovered:
        await reply(message, "⏳ Still discovering symbols, try again shortly.")
        return

    now = time.time()
    with_data = sum(1 for tr in trackers.values() if tr.bid_marker > 0 or tr.ask_marker > 0)

    movers = []
    for symbol, tr in trackers.items():
        if tr.events:
            movers.append((symbol, len(tr.events), now - tr.events[0]))
    movers.sort(key=lambda x: (-x[1], x[2]))
    top_text = ""
    for sym, n, span in movers[:5]:
        tag = " 🔴" if sym in banned_symbols else (
            " 🟡" if sym in frozen_symbols and frozen_symbols[sym] > now else ""
        )
        top_text += f"  📌 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>{tag}: {n} fluxes / {int(span)}s\n"
    if not top_text:
        top_text = "  ⏳ No fluctuations in window yet.\n"

    mute_status = f"\n🔇 Muted: <b>{mute_remaining()}</b>" if is_muted() else ""
    ws_tag = "🟢" if ws_state["connected"] else "🔴"

    admin_extra = (
        f"\n👥 Subscribers: <b>{len(subscribers)}</b>"
        f"\n🔴 Banned: <b>{len(banned_symbols)}</b>"
        f"\n🟡 Frozen: <b>{len(frozen_symbols)}</b>"
        f"{mute_status}"
        if is_admin(message) else ""
    )

    await reply(message,
        f"📊 <b>Monitor Status</b>\n\n"
        f"{ws_tag} WS: <b>{'connected' if ws_state['connected'] else 'down'}</b> "
        f"(subs <b>{ws_state['subscribed']}</b>, msgs <b>{ws_state['msg_count']}</b>)\n"
        f"🔍 Symbols watched: <b>{len(MONITORED_SYMBOLS)}</b>\n"
        f"📈 With data: <b>{with_data}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"⚡ Threshold: <b>{flux_pct}%</b>  Count: <b>{flux_count}</b>  Window: <b>{flux_window}s</b>\n"
        f"📚 Depth filter: top-3 ≥<b>${depth_min_vol:.0f}</b> USD, spread ≤<b>{depth_range_pct}%</b>"
        f"{admin_extra}\n\n"
        f"🏆 <b>Most fluctuating now:</b>\n{top_text}"
    )


@router.message(Command("wsstatus"))
async def cmd_wsstatus(message: Message) -> None:
    """Verify the WebSocket feed is alive and pushing data."""
    now      = time.time()
    age      = now - ws_state["last_msg_ts"] if ws_state["last_msg_ts"] else -1
    rate_10s = len(ws_state["msgs_last_10s"])
    uptime   = (now - ws_state["connect_time"]) if ws_state["connect_time"] else 0
    h, m, s  = int(uptime // 3600), int((uptime % 3600) // 60), int(uptime % 60)
    uptime_s = f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"

    sample_lines = []
    for sym in ("AAPLSTOCK_USDT", "MSFTSTOCK_USDT", "NAS100_USDT"):
        b = depth_books.get(sym)
        if b and b.bids and b.asks:
            age_s = now - b.ts
            sample_lines.append(
                f"  • <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>: "
                f"bid <code>{b.bids[0][0]}</code> × <code>{b.bids[0][1]}</code>, "
                f"ask <code>{b.asks[0][0]}</code> × <code>{b.asks[0][1]}</code> "
                f"({age_s:.1f}s ago)"
            )
    sample_text = "\n".join(sample_lines) if sample_lines else "  (no books yet)"

    err_text = ""
    if ws_state["errors"] and is_admin(message):
        err_text = "\n\n<b>Recent errors:</b>\n" + "\n".join(
            f"  ⚠️ <code>{e}</code>" for e in list(ws_state["errors"])[-5:]
        )

    age_str = f"{age:.1f}s" if age >= 0 else "n/a"
    await reply(message,
        f"📡 <b>WebSocket health</b>\n\n"
        f"State: <b>{'🟢 connected' if ws_state['connected'] else '🔴 down'}</b>\n"
        f"Uptime: <b>{uptime_s}</b>  Reconnects: <b>{ws_state['reconnects']}</b>\n"
        f"Subscribed: <b>{ws_state['subscribed']}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"Messages: <b>{ws_state['msg_count']}</b> total  <b>{rate_10s}</b> in last 10s\n"
        f"Last msg: <b>{age_str}</b> ago\n\n"
        f"<b>Sample books:</b>\n{sample_text}"
        f"{err_text}"
    )


@router.message(Command("symbols"))
async def cmd_symbols(message: Message) -> None:
    if not symbols_discovered:
        await reply(message, "⏳ Still discovering, try again shortly.")
        return
    now = time.time()
    lines = []
    for sym in sorted(MONITORED_SYMBOLS):
        display = HARDCODED_SYMBOLS.get(sym, sym)
        if sym in banned_symbols:
            tag = "🔴"
        elif sym in frozen_symbols and frozen_symbols[sym] > now:
            remaining = int((frozen_symbols[sym] - now) / 60)
            tag = f"🟡{remaining}m"
        else:
            tag = "🟢"
        lines.append(f"  {tag} <b>{display}</b> — <code>{sym}</code>")
    chunks = ["\n".join(lines[i:i+50]) for i in range(0, len(lines), 50)]
    for i, chunk in enumerate(chunks):
        prefix = f"📋 <b>Monitored ({len(MONITORED_SYMBOLS)}):</b>\n" if i == 0 else ""
        suffix = "\n\n🟢 Active  🟡 Frozen  🔴 Banned" if i == len(chunks) - 1 else ""
        await reply(message, prefix + chunk + suffix)


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, "Usage: <code>/ban SYMBOL</code>")
        return
    sym = find_symbol(args[0])
    if not sym:
        await reply(message, f"❌ <code>{args[0].upper()}</code> not found.")
        return
    banned_symbols.add(sym)
    frozen_symbols.pop(sym, None)
    await reply(message, f"🔴 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> BANNED.")


@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, "Usage: <code>/unban SYMBOL</code>")
        return
    sym = find_symbol(args[0])
    if not sym or sym not in banned_symbols:
        await reply(message, "❌ Not found or not banned.")
        return
    banned_symbols.discard(sym)
    await reply(message, f"✅ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is ACTIVE again.")


@router.message(Command("freeze"))
async def cmd_freeze(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if len(args) < 2:
        await reply(message, "Usage: <code>/freeze SYMBOL MINUTES</code>")
        return
    sym = find_symbol(args[0])
    if not sym:
        await reply(message, f"❌ <code>{args[0].upper()}</code> not found.")
        return
    try:
        minutes = float(args[1])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Minutes must be a positive number.")
        return
    frozen_symbols[sym] = time.time() + minutes * 60
    h, m = int(minutes // 60), int(minutes % 60)
    await reply(message, f"🟡 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> frozen for <b>{'{}h {}m'.format(h,m) if h else '{}m'.format(m)}</b>.")


@router.message(Command("unfreeze"))
async def cmd_unfreeze(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, "Usage: <code>/unfreeze SYMBOL</code>")
        return
    sym = find_symbol(args[0])
    if not sym or sym not in frozen_symbols:
        await reply(message, "❌ Not found or not frozen.")
        return
    frozen_symbols.pop(sym)
    await reply(message, f"✅ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is ACTIVE again.")


@router.message(Command("blocked"))
async def cmd_blocked(message: Message) -> None:
    now = time.time()
    for s in [s for s, t in list(frozen_symbols.items()) if t <= now]:
        frozen_symbols.pop(s)
    banned_text = "".join(f"  🔴 <b>{HARDCODED_SYMBOLS.get(s, s)}</b>\n" for s in sorted(banned_symbols))
    frozen_text = "".join(
        f"  🟡 <b>{HARDCODED_SYMBOLS.get(s, s)}</b> — {int((t-now)//60)}m {int((t-now)%60)}s\n"
        for s, t in sorted(frozen_symbols.items())
    )
    mute_text = f"\n🔇 <b>Muted</b> — {mute_remaining()} remaining\n" if is_muted() else ""
    if not banned_text and not frozen_text and not mute_text:
        await reply(message, "✅ Nothing blocked, bot not muted.")
        return
    parts = ["🚫 <b>Blocked Status</b>\n"]
    if mute_text:
        parts.append(mute_text)
    if banned_text:
        parts.append(f"<b>Banned ({len(banned_symbols)}):</b>\n{banned_text}")
    if frozen_text:
        parts.append(f"<b>Frozen ({len(frozen_symbols)}):</b>\n{frozen_text}")
    await reply(message, "\n".join(parts))


@router.message(Command("subscribers"))
async def cmd_subscribers(message: Message) -> None:
    if not subscribers:
        await reply(message, "📭 No subscribers yet.")
        return
    lines = []
    for i, (cid, info) in enumerate(subscribers.items(), 1):
        joined = time.strftime("%d.%m.%Y", time.localtime(info.get("joined_at", 0)))
        lines.append(f"{i}. <b>{info['name']}</b> {info['username']}\n   <code>{cid}</code> | {joined}")
    chunks = [lines[i:i+30] for i in range(0, len(lines), 30)]
    for chunk in chunks:
        await reply(message, f"👥 <b>Subscribers ({len(subscribers)}):</b>\n\n" + "\n\n".join(chunk))


@router.message(Command("kick"))
async def cmd_kick(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, "Usage: <code>/kick CHAT_ID</code>")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await reply(message, "❌ Invalid ID.")
        return
    if target_id not in subscribers:
        await reply(message, "ℹ️ Not subscribed.")
        return
    name = subscribers[target_id]["name"]
    subscribers.pop(target_id)
    save_subscribers()
    await reply(message, f"✅ Removed <b>{name}</b>.")
    try:
        await message.bot.send_message(chat_id=target_id, text="ℹ️ You have been removed by the admin.")
    except Exception:
        pass


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await reply(message, "Usage: <code>/broadcast Your message here</code>")
        return
    msg = parts[1]
    await reply(message, f"📣 Sending to <b>{len(subscribers)}</b> subscribers...")
    sent, dead = 0, []
    for chat_id in list(subscribers.keys()):
        try:
            await message.bot.send_message(chat_id=chat_id, text=f"📣 <b>Admin:</b>\n\n{msg}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as exc:
            if any(x in str(exc).lower() for x in ("blocked", "not found", "deactivated")):
                dead.append(chat_id)
    for cid in dead:
        subscribers.pop(cid, None)
    if dead:
        save_subscribers()
    await reply(message, f"✅ Sent: <b>{sent}</b>, Removed dead: <b>{len(dead)}</b>")


@router.message(Command("debugconfig"))
async def cmd_debugconfig(message: Message) -> None:
    await reply(message,
        f"⚙️ <b>Current Config</b>\n\n"
        f"SUBSCRIBERS: <code>{len(subscribers)}</code>\n"
        f"FLUX_PCT:    <code>{flux_pct}</code>\n"
        f"FLUX_COUNT:  <code>{flux_count}</code>\n"
        f"FLUX_WINDOW: <code>{flux_window}s</code>\n"
        f"DEPTH_RANGE: <code>{depth_range_pct}%</code>\n"
        f"DEPTH_VOL:   <code>${depth_min_vol:.0f}</code>"
    )


@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    await reply(message, "🔍 Hitting REST API, please wait...")
    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(
                MEXC_TICKER_URL, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=30), ssl=True,
            ) as response:
                if response.status != 200:
                    await reply(message, f"❌ HTTP {response.status}")
                    return
                payload = await response.json(content_type=None)
                data: list[dict] = payload.get("data", [])
                if not data:
                    await reply(message, "⚠️ Connected but data empty!")
                    return
                found, not_found = discover_symbols(data)
                matched_lines = [
                    f"  ✅ <b>{base}</b> → <code>{sym}</code>"
                    for sym, base in sorted(found.items(), key=lambda x: x[1])
                ]
                # First chunk
                await reply(message,
                    f"✅ <b>API OK</b> — {len(data)} tickers\n"
                    f"Matched: <b>{len(found)}/{len(HARDCODED_SYMBOLS)}</b>\n\n"
                    + "\n".join(matched_lines[:50])
                )
                if len(matched_lines) > 50:
                    await reply(message, "\n".join(matched_lines[50:]))
                if not_found:
                    await reply(message, f"❌ Not in API: <code>{', '.join(sorted(not_found))}</code>")
        except asyncio.TimeoutError:
            await reply(message, "⏱ Timed out.")
        except Exception as exc:
            await reply(message, f"💥 <code>{exc}</code>")


@router.message(Command("test"))
async def cmd_test(message: Message, bot: Bot) -> None:
    global test_mode, test_chat_id
    if not symbols_discovered:
        await reply(message, "⏳ Still discovering symbols, try again shortly.")
        return
    test_mode    = True
    test_chat_id = message.chat.id
    await reply(message,
        "🧪 <b>Test mode armed.</b>\n"
        f"Will fire on the FIRST ≥{flux_pct}% fluctuation — depth/volume filter is "
        "<b>SKIPPED</b> so you can confirm the bot is alive.\n"
        "Returns to normal afterwards."
    )


@router.message(Command("book"))
async def cmd_book(message: Message, bot: Bot) -> None:
    """Show top-5 bid/ask for a symbol — sanity check the WS feed."""
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, "Usage: <code>/book AAPL</code> or <code>/book AAPLSTOCK_USDT</code>")
        return
    sym = find_symbol(args[0])
    if not sym:
        await reply(message, f"❌ <code>{args[0].upper()}</code> not in monitored list.")
        return

    book = depth_books.get(sym)
    if not book or not book.bids or not book.asks:
        await reply(message, f"⏳ No book yet for <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>. WS may still be subscribing.")
        return

    cs  = contract_sizes.get(sym, 1.0)
    age = time.time() - book.ts

    def fmt_row(label: str, idx: int, lvl: list) -> Optional[str]:
        try:
            price = float(lvl[0])
            size  = float(lvl[1])
        except (IndexError, ValueError, TypeError):
            return None
        usd = size * cs * price
        return f"  {label}{idx}  <code>{price}</code>  × <code>{int(size)}</code>  = <b>${usd:,.0f}</b>"

    # Asks: show top→down with ask5 first, ask1 last (closest to bids)
    ask_rows = [fmt_row("a", i, lvl) for i, lvl in enumerate(book.asks[:5], 1)]
    ask_rows = [r for r in ask_rows if r]
    ask_rows.reverse()

    bid_rows = [fmt_row("b", i, lvl) for i, lvl in enumerate(book.bids[:5], 1)]
    bid_rows = [r for r in bid_rows if r]

    await reply(message,
        f"📖 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>  <code>{sym}</code>\n"
        f"contractSize: <code>{cs}</code>   age: {age:.1f}s\n\n"
        f"<b>Asks</b>\n" + ("\n".join(ask_rows) if ask_rows else "  (none)") + "\n"
        f"   ─────────\n"
        f"<b>Bids</b>\n" + ("\n".join(bid_rows) if bid_rows else "  (none)") + "\n\n"
        f"<i>USD = size × contractSize × price</i>"
    )


@router.message(Command("debugflux"))
async def cmd_debugflux(message: Message, bot: Bot) -> None:
    """Show fluctuation + filter-block stats. /debugflux for top symbols, or /debugflux SYMBOL for one."""
    args = (message.text or "").split()[1:]

    if args:
        sym = find_symbol(args[0])
        if not sym:
            await reply(message, f"❌ <code>{args[0].upper()}</code> not in monitored list.")
            return
        tr   = trackers.get(sym)
        book = depth_books.get(sym)
        if not tr:
            await reply(message, "no tracker yet")
            return
        now      = time.time()
        in_win   = len(tr.events)
        block_lines = "\n".join(
            f"  • <code>{k}</code>: {v}" for k, v in sorted(tr.blocks.items(), key=lambda x: -x[1])
        ) or "  (none)"
        last_block_age = (now - tr.last_block_ts) if tr.last_block_ts else None
        last_block_str = (
            f"\n<b>Last block</b> ({last_block_age:.0f}s ago): <code>{tr.last_block}</code>"
            if tr.last_block else ""
        )
        book_line = ""
        if book and book.bids and book.asks:
            cs = contract_sizes.get(sym, 1.0)
            b_usds = [_level_usd(sym, float(l[0]), float(l[1])) for l in book.bids[:3]]
            a_usds = [_level_usd(sym, float(l[0]), float(l[1])) for l in book.asks[:3]]
            bid1, bid3 = float(book.bids[0][0]), float(book.bids[2][0])
            ask1, ask3 = float(book.asks[0][0]), float(book.asks[2][0])
            b_spread = (bid1 - bid3) / bid1 * 100 if bid1 else 0
            a_spread = (ask3 - ask1) / ask1 * 100 if ask1 else 0
            book_line = (
                f"\n<b>Live book (top-3 USD)</b>\n"
                f"  bid: " + " / ".join(f"${u:,.0f}" for u in b_usds) + f"   spread {b_spread:.3f}%\n"
                f"  ask: " + " / ".join(f"${u:,.0f}" for u in a_usds) + f"   spread {a_spread:.3f}%"
            )
        await reply(message,
            f"🔬 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>  <code>{sym}</code>\n"
            f"flux in window: <b>{in_win}/{flux_count}</b>\n"
            f"lifetime fluxes: <b>{tr.total_flux}</b>   alerts sent: <b>{tr.total_alerts}</b>\n"
            f"bid_marker <code>{tr.bid_marker}</code>   ask_marker <code>{tr.ask_marker}</code>\n"
            f"\n<b>Filter blocks</b>\n{block_lines}"
            f"{last_block_str}"
            f"{book_line}"
        )
        return

    # No arg: aggregate top blockers + most-fluctuating symbols
    now = time.time()
    by_alerts = sorted(trackers.items(), key=lambda kv: -kv[1].total_alerts)[:5]
    by_flux   = sorted(trackers.items(), key=lambda kv: -kv[1].total_flux)[:5]
    total_blocks: dict[str, int] = {}
    total_alerts = 0
    total_fluxes = 0
    for tr in trackers.values():
        total_alerts += tr.total_alerts
        total_fluxes += tr.total_flux
        for k, v in tr.blocks.items():
            total_blocks[k] = total_blocks.get(k, 0) + v

    blockers = "\n".join(
        f"  • <code>{k}</code>: {v}" for k, v in sorted(total_blocks.items(), key=lambda x: -x[1])
    ) or "  (none)"

    flux_top = "\n".join(
        f"  • <b>{HARDCODED_SYMBOLS.get(s, s)}</b>: {tr.total_flux} fluxes, {tr.total_alerts} alerts"
        for s, tr in by_flux if tr.total_flux > 0
    ) or "  (none yet)"

    alert_top = "\n".join(
        f"  • <b>{HARDCODED_SYMBOLS.get(s, s)}</b>: {tr.total_alerts}"
        for s, tr in by_alerts if tr.total_alerts > 0
    ) or "  (none yet)"

    await reply(message,
        f"🔬 <b>Flux debug — global</b>\n"
        f"Lifetime: <b>{total_fluxes}</b> fluxes, <b>{total_alerts}</b> alerts\n\n"
        f"<b>Filter blocks (all symbols)</b>\n{blockers}\n\n"
        f"<b>Top by fluxes</b>\n{flux_top}\n\n"
        f"<b>Top by alerts</b>\n{alert_top}\n\n"
        f"<i>Use</i> <code>/debugflux AAPL</code> <i>for one symbol.</i>"
    )


@router.message()
async def cmd_catch_all(message: Message) -> None:
    pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    logger.info(
        "Bot @%s started — flux=%.2f%% count=%d window=%ds depth_vol=%.0f depth_range=%.2f%% subscribers=%d",
        me.username, flux_pct, flux_count, flux_window, depth_min_vol, depth_range_pct, len(subscribers),
    )

    # Discover symbols via REST once, then start WS
    boot_session = aiohttp.ClientSession()
    try:
        await bootstrap_symbols(boot_session)
    finally:
        await boot_session.close()

    if subscribers:
        await _notify_subscribers(
            bot,
            f"🟢 <b>Flux bot started</b> — {len(MONITORED_SYMBOLS)} symbols, "
            f"{flux_count}× ≥{flux_pct}% / {flux_window}s, "
            f"depth ≥${depth_min_vol:.0f}/{depth_range_pct}%."
        )

    ws_task       = asyncio.create_task(ws_loop(bot))
    automute_task = asyncio.create_task(auto_mute_scheduler(bot))

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        for t in (ws_task, automute_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await bot.session.close()
        logger.info("Bot shut down")


if __name__ == "__main__":
    asyncio.run(main())
