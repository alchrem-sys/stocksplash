"""
MEXC Futures Price Surge Monitor Bot
Version: 8.1.0 — bid1/ask1 based movement detection
"""

import asyncio
import json
import logging
import os
import time
import threading
import requests as _requests
from collections import deque
from dataclasses import dataclass
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
logger = logging.getLogger("mexc_surge_bot")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env file")

FETCH_INTERVAL: float = float(os.getenv("FETCH_INTERVAL", "2"))
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "60"))
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "60"))
CHANNEL_ID: str = os.getenv("CHANNEL_ID", "") or os.getenv("CHAT_ID", "")
THREAD_ID: Optional[int] = int(os.getenv("THREAD_ID", "0")) or None

# Runtime-adjustable threshold (changed via /threshold command)
surge_threshold: float = float(os.getenv("SURGE_THRESHOLD", "1.0"))

ADMIN_ID = 868931721

MEXC_TICKER_URL = "https://futures.mexc.com/api/v1/contract/ticker"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://futures.mexc.com/",
    "Origin": "https://futures.mexc.com",
}

# ---------------------------------------------------------------------------
# Yahoo Finance — cookie/crumb fetch (same as spread bot)
# ---------------------------------------------------------------------------

_yahoo_session = _requests.Session()
_yahoo_crumb:  Optional[str] = None
_yahoo_cookie: Optional[str] = None
_crumb_lock    = threading.Lock()

_YAHOO_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# MEXC symbol → Yahoo ticker (stocks only)
YAHOO_MAP: dict[str, str] = {
    "TSLASTOCK_USDT": "TSLA", "NVDASTOCK_USDT": "NVDA", "AAPLSTOCK_USDT": "AAPL",
    "MSFTSTOCK_USDT": "MSFT", "AMZNSTOCK_USDT": "AMZN", "METASTOCK_USDT": "META",
    "AMDSTOCK_USDT":  "AMD",  "GOOGLSTOCK_USDT": "GOOGL", "COINBASE_USDT": "COIN",
    "CVNASTOCK_USDT": "CVNA", "AMATSTOCK_USDT": "AMAT",  "QCOMSTOCK_USDT": "QCOM",
    "CRMSTOCK_USDT":  "CRM",  "SHOPSTOCK_USDT": "SHOP",  "VZSTOCK_USDT":   "VZ",
    "INTCSTOCK_USDT": "INTC", "QQQSTOCK_USDT":  "QQQ",   "CSCOSTOCK_USDT": "CSCO",
    "JNJSTOCK_USDT":  "JNJ",  "FUTUSTOCK_USDT": "FUTU",  "XOMSTOCK_USDT":  "XOM",
    "RDDTSTOCK_USDT": "RDDT", "SPOTSTOCK_USDT": "SPOT",  "NFLXSTOCK_USDT": "NFLX",
    "SMCISTOCK_USDT": "SMCI", "ORCLSTOCK_USDT": "ORCL",  "ASMLSTOCK_USDT": "ASML",
    "ACNSTOCK_USDT":  "ACN",  "UNHSTOCK_USDT":  "UNH",   "NOWSTOCK_USDT":  "NOW",
    "LLYSTOCK_USDT":  "LLY",  "LRCXSTOCK_USDT": "LRCX",  "IBMSTOCK_USDT":  "IBM",
    "COSTSTOCK_USDT": "COST", "JDSTOCK_USDT":   "JD",    "JPMSTOCK_USDT":  "JPM",
    "GSSTOCK_USDT":   "GS",   "MASTOCK_USDT":   "MA",    "KOSTOCK_USDT":   "KO",
    "WMTSTOCK_USDT":  "WMT",  "GESTOCK_USDT":   "GE",    "MUSTOCK_USDT":   "MU",
    "VSTOCK_USDT":    "V",    "NKESTOCK_USDT":  "NKE",   "PEPSTOCK_USDT":  "PEP",
    "BASTOCK_USDT":   "BA",   "ROBINHOOD_USDT": "HOOD",  "FIGSTOCK_USDT":  "FIG",
}


def _refresh_yahoo_crumb() -> bool:
    global _yahoo_crumb, _yahoo_cookie
    try:
        r = _yahoo_session.get("https://finance.yahoo.com", headers=_YAHOO_HEADERS, timeout=10)
        if "consent" in r.url:
            _yahoo_session.post(
                "https://consent.yahoo.com/v2/collectConsent",
                data={"agree": ["agree"], "consentUUID": "default", "sessionId": "default"},
                headers=_YAHOO_HEADERS, timeout=10,
            )
        r2 = _yahoo_session.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers={**_YAHOO_HEADERS, "Accept": "text/plain"}, timeout=10,
        )
        if r2.status_code == 200 and r2.text and len(r2.text) < 50:
            _yahoo_crumb  = r2.text.strip()
            _yahoo_cookie = "; ".join(f"{k}={v}" for k, v in _yahoo_session.cookies.items())
            logger.info("Yahoo crumb OK")
            return True
        return False
    except Exception as exc:
        logger.error("Yahoo crumb error: %s", exc)
        return False


def _fetch_yahoo_price(ticker: str) -> Optional[float]:
    """Fetch current price for a single ticker. Returns None on failure."""
    global _yahoo_crumb
    with _crumb_lock:
        if not _yahoo_crumb:
            _refresh_yahoo_crumb()
    try:
        resp = _yahoo_session.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={
                "symbols": ticker,
                "fields":  "regularMarketPrice,preMarketPrice,postMarketPrice,marketState,regularMarketTime,postMarketTime,preMarketTime",
                "crumb":   _yahoo_crumb or "",
            },
            headers={**_YAHOO_HEADERS, "Cookie": _yahoo_cookie or ""},
            timeout=8,
        )
        if resp.status_code in (401, 403):
            with _crumb_lock:
                _refresh_yahoo_crumb()
            return None
        results = resp.json().get("quoteResponse", {}).get("result", [])
        if not results:
            return None
        q = results[0]
        # Use same timestamp logic as spread bot
        reg_price  = float(q.get("regularMarketPrice") or 0) or None
        pre_price  = float(q.get("preMarketPrice")     or 0) or None
        post_price = float(q.get("postMarketPrice")    or 0) or None
        reg_time   = int(q.get("regularMarketTime")    or 0)
        pre_time   = int(q.get("preMarketTime")        or 0)
        post_time  = int(q.get("postMarketTime")       or 0)
        if post_price and post_time > reg_time:
            return post_price
        if pre_price and pre_time > reg_time:
            return pre_price
        return reg_price
    except Exception as exc:
        logger.warning("Yahoo price fetch error for %s: %s", ticker, exc)
        return None

HARDCODED_SYMBOLS: dict[str, str] = {
    "NAS100_USDT":     "NAS100",
    "HK50_USDT":       "HK50",
    "US30_USDT":       "US30",
    "SP500_USDT":      "SP500",
    "COINBASE_USDT":   "COIN",
    "FIGSTOCK_USDT":   "FIG",
    "ROBINHOOD_USDT":  "HOOD",
    "TSLASTOCK_USDT":  "TSLA",
    "NVDASTOCK_USDT":  "NVDA",
    "CVNASTOCK_USDT":  "CVNA",
    "AMATSTOCK_USDT":  "AMAT",
    "GOOGLSTOCK_USDT": "GOOGL",
    "QCOMSTOCK_USDT":  "QCOM",
    "CRMSTOCK_USDT":   "CRM",
    "SHOPSTOCK_USDT":  "SHOP",
    "MSFTSTOCK_USDT":  "MSFT",
    "VZSTOCK_USDT":    "VZ",
    "INTCSTOCK_USDT":  "INTC",
    "QQQSTOCK_USDT":   "QQQ",
    "CSCOSTOCK_USDT":  "CSCO",
    "JNJSTOCK_USDT":   "JNJ",
    "AMZNSTOCK_USDT":  "AMZN",
    "FUTUSTOCK_USDT":  "FUTU",
    "AAPLSTOCK_USDT":  "AAPL",
    "AMDSTOCK_USDT":   "AMD",
    "XOMSTOCK_USDT":   "XOM",
    "METASTOCK_USDT":  "META",
    "RDDTSTOCK_USDT":  "RDDT",
    "SPOTSTOCK_USDT":  "SPOT",
    "NFLXSTOCK_USDT":  "NFLX",
    "SMCISTOCK_USDT":  "SMCI",
    "ORCLSTOCK_USDT":  "ORCL",
    "ASMLSTOCK_USDT":  "ASML",
    "ACNSTOCK_USDT":   "ACN",
    "UNHSTOCK_USDT":   "UNH",
    "NOWSTOCK_USDT":   "NOW",
    "LLYSTOCK_USDT":   "LLY",
    "LRCXSTOCK_USDT":  "LRCX",
    "IBMSTOCK_USDT":   "IBM",
    "COSTSTOCK_USDT":  "COST",
    "JDSTOCK_USDT":    "JD",
    "JPMSTOCK_USDT":   "JPM",
    "GSSTOCK_USDT":    "GS",
    "MASTOCK_USDT":    "MA",
    "KOSTOCK_USDT":    "KO",
    "WMTSTOCK_USDT":   "WMT",
    "GESTOCK_USDT":    "GE",
    "MUSTOCK_USDT":    "MU",
    "VSTOCK_USDT":     "V",
    "NKESTOCK_USDT":   "NKE",
    "PEPSTOCK_USDT":   "PEP",
    "BASTOCK_USDT":    "BA",
}

# ---------------------------------------------------------------------------
# Subscriber storage
# ---------------------------------------------------------------------------

SUBSCRIBERS_FILE = Path(__file__).parent / "subscribers.json"


def load_subscribers() -> dict[int, dict]:
    if SUBSCRIBERS_FILE.exists():
        try:
            data = json.loads(SUBSCRIBERS_FILE.read_text())
            return {int(k): v for k, v in data.items()}
        except Exception as exc:
            logger.error("Failed to load subscribers: %s", exc)
    return {}


def save_subscribers() -> None:
    try:
        SUBSCRIBERS_FILE.write_text(json.dumps(subscribers, indent=2))
    except Exception as exc:
        logger.error("Failed to save subscribers: %s", exc)


subscribers: dict[int, dict] = load_subscribers()

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class BookSnapshot:
    ts:   float
    bid1: float
    ask1: float


@dataclass
class SplashTracker:
    """
    Tracks the lowest ask1 (for UP splash) and highest bid1 (for DOWN crash)
    within a rolling 60-second window. Resets when the window expires.
    """
    # UP: track lowest ask1 → alert when ask1 rises >= threshold from floor
    floor_price: float = 0.0   # lowest ask1 seen
    floor_time:  float = 0.0   # when floor was set

    # DOWN: track highest bid1 → alert when bid1 falls >= threshold from ceiling
    ceil_price:  float = 0.0   # highest bid1 seen
    ceil_time:   float = 0.0   # when ceiling was set


MONITORED_SYMBOLS: set[str] = set()

# New: one tracker per symbol
trackers: dict[str, SplashTracker] = {}

last_surge_alert: dict[str, float] = {}
last_crash_alert: dict[str, float] = {}

symbols_discovered: bool = False
test_mode: bool = False
test_chat_id: Optional[int] = None

banned_symbols: set[str] = set()
frozen_symbols: dict[str, float] = {}
muted_until: float = 0.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id == ADMIN_ID


async def reply(message: Message, text: str, **kwargs) -> None:
    """
    Smart reply — always responds in the same thread the command came from.
    Works for DMs, groups, and forum topic threads alike.
    """
    thread_id = message.message_thread_id if message.is_topic_message else None
    await message.bot.send_message(
        chat_id=message.chat.id,
        text=text,
        message_thread_id=thread_id,
        parse_mode="HTML",
        **kwargs,
    )


async def deny(message: Message) -> None:
    await reply(message, "⛔ You are not authorized to use this command.")


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
    found: dict[str, str] = {}
    not_found: list[str] = []
    for mexc_sym, base in HARDCODED_SYMBOLS.items():
        if mexc_sym in api_symbol_set:
            found[mexc_sym] = base
            logger.info("Confirmed: %-10s -> %s", base, mexc_sym)
        else:
            not_found.append(base)
            logger.warning("NOT IN API: %-10s (%s)", base, mexc_sym)
    return found, not_found


# ---------------------------------------------------------------------------
# Channel broadcast
# ---------------------------------------------------------------------------


async def broadcast(bot: Bot, text: str, keyboard: InlineKeyboardMarkup) -> None:
    if not CHANNEL_ID:
        logger.warning("CHANNEL_ID not set — alert not sent")
        return
    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            reply_markup=keyboard,
            message_thread_id=THREAD_ID,
        )
        logger.info("Alert posted to %s thread=%s", CHANNEL_ID, THREAD_ID)
    except Exception as exc:
        logger.error("Failed to post to channel: %s", exc)


# ---------------------------------------------------------------------------
# Alert builders — CHANGED: show bid1/ask1 label and direction
# ---------------------------------------------------------------------------


def build_surge_message(
    symbol: str,
    pct_change: float,
    current_price: float,
    price_type: str,
    elapsed_seconds: int = 0,
    yahoo_price: Optional[float] = None,
    is_test: bool = False,
) -> str:
    display  = HARDCODED_SYMBOLS.get(symbol, symbol.replace("_", ""))
    sign     = "" if pct_change < 0 else "+"
    header   = "🧪 <b>[TEST]</b> " if is_test else ""
    yahoo_line = f"\nYahoo:  <b>${yahoo_price:.4f}</b>" if yahoo_price else ""
    return (
        f"{header}<b>${display}</b>  {sign}{pct_change:.2f}%  +{elapsed_seconds}s\n"
        f"MEXC:   <b>${current_price:.4f}</b>"
        f"{yahoo_line}"
    )


def build_trade_keyboard(symbol: str) -> InlineKeyboardMarkup:
    yahoo_ticker = YAHOO_MAP.get(symbol)
    buttons = [
        InlineKeyboardButton(
            text="📊 MEXC Futures",
            url=f"https://futures.mexc.com/exchange/{symbol}"
        )
    ]
    if yahoo_ticker:
        buttons.append(InlineKeyboardButton(
            text="📈 Yahoo Finance",
            url=f"https://finance.yahoo.com/quote/{yahoo_ticker}"
        ))
    return InlineKeyboardMarkup(inline_keyboard=[buttons])


async def send_surge_alert(
    bot: Bot,
    symbol: str,
    pct_change: float,
    current_price: float,
    price_type: str = "bid1",
    elapsed_seconds: int = 0,
    chat_id: Optional[int] = None,
    is_test: bool = False,
) -> None:
    # Fetch Yahoo price in thread so we don't block the event loop
    yahoo_price: Optional[float] = None
    yahoo_ticker = YAHOO_MAP.get(symbol)
    if yahoo_ticker:
        try:
            loop = asyncio.get_event_loop()
            yahoo_price = await loop.run_in_executor(None, _fetch_yahoo_price, yahoo_ticker)
        except Exception:
            pass

    text     = build_surge_message(symbol, pct_change, current_price, price_type,
                                   elapsed_seconds, yahoo_price, is_test)
    keyboard = build_trade_keyboard(symbol)
    if chat_id:
        try:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
        except Exception as exc:
            logger.error("Test alert failed: %s", exc)
    else:
        await broadcast(bot, text, keyboard)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = Router()


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    admin_section = ""
    if is_admin(message):
        admin_section = (
            "\n\n<b>🔐 Admin commands:</b>\n"
            "/threshold 2.0 — set alert threshold (default 1%)\n"
            "/mute 30 — mute ALL alerts for N minutes\n"
            "/unmute — unmute immediately\n"
            "/ban TSLA — permanently silence a ticker\n"
            "/unban TSLA — restore banned ticker\n"
            "/freeze TSLA 30 — silence ticker for N minutes\n"
            "/unfreeze TSLA — restore frozen ticker early\n"
            "/blocked — show all banned + frozen tickers\n"
            "/symbols — all monitored symbols with status\n"
            "/subscribers — list all subscribers\n"
            "/kick ID — remove a subscriber\n"
            "/broadcast msg — send message to all subscribers\n"
            "/debug — raw API diagnostic\n"
            "/test — test alert at 0.1% threshold\n"
        )

    await reply(message, 
        "<b>📖 MEXC Surge Monitor — Help</b>\n\n"
        "<b>📊 Info commands:</b>\n"
        "/status — live stats + top 5 movers\n"
        "/symbols — all monitored symbols\n"
        "/help — show this message"
        f"{admin_section}\n\n"
        "<b>ℹ️ How it works:</b>\n"
        f"Bot monitors {len(MONITORED_SYMBOLS)} futures symbols every 2s.\n"
        f"📈 UP move → measured via <b>bid1</b>\n"
        f"📉 DOWN move → measured via <b>ask1</b>\n"
        f"Alerts fire when either moves ±{surge_threshold}% within {WINDOW_SECONDS}s.\n"
        "🚨 = surge  🔴 = crash"
    )


@router.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    global surge_threshold
    args = (message.text or "").split()[1:]
    if not args:
        await reply(message, 
            f"Current threshold: <b>±{surge_threshold}%</b>\n\n"
            "Usage: <code>/threshold 2.0</code>"
        )
        return
    try:
        new_val = float(args[0])
        if new_val <= 0 or new_val > 100:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Must be a number between 0.1 and 100.")
        return
    old = surge_threshold
    surge_threshold = new_val
    await reply(message, f"✅ Threshold: <b>±{old}%</b> → <b>±{new_val}%</b>")


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

    # Check if first arg is a ticker or a number
    ticker = find_symbol(args[0])
    if ticker:
        # /mute TICKER MINUTES
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
        h = int(minutes // 60)
        m = int(minutes % 60)
        await reply(message,
            f"🔇 <b>{display}</b> muted for <b>{'{}h {}m'.format(h,m) if h else '{}m'.format(m)}</b>\n"
            f"Use <code>/unfreeze {display}</code> to restore early.")
        return

    # /mute MINUTES
    try:
        minutes = float(args[0])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await reply(message, "❌ Unknown symbol or invalid number.")
        return
    muted_until = time.time() + minutes * 60
    h = int(minutes // 60)
    m = int(minutes % 60)
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
    windows_with_data = sum(1 for tr in trackers.values() if tr.floor_price > 0)
    on_cooldown = sum(
        1 for sym in MONITORED_SYMBOLS
        if now - max(last_surge_alert.get(sym, 0), last_crash_alert.get(sym, 0)) < COOLDOWN_SECONDS
    )

    # Show top movers based on how far current price is from tracked floor/ceiling
    movers = []
    for symbol, tr in trackers.items():
        if tr.floor_price > 0 and tr.floor_time > 0:
            elapsed = now - tr.floor_time
            if elapsed < SPLASH_WINDOW:
                movers.append((symbol, elapsed, tr.floor_price))

    movers.sort(key=lambda x: x[1])
    top_text = ""
    for sym, elapsed, price in movers[:5]:
        tag = " 🔴" if sym in banned_symbols else (
            " 🟡" if sym in frozen_symbols and frozen_symbols[sym] > now else ""
        )
        top_text += f"  📌 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>{tag}: floor ${price:.4f} ({int(elapsed)}s ago)\n"

    if not top_text:
        top_text = "  ⏳ Filling window, wait 10s...\n"

    mute_status = f"\n🔇 Muted: <b>{mute_remaining()}</b>" if is_muted() else ""
    admin_extra = (
        f"\n👥 Subscribers: <b>{len(subscribers)}</b>\n"
        f"🔴 Banned: <b>{len(banned_symbols)}</b>\n"
        f"🟡 Frozen: <b>{len(frozen_symbols)}</b>"
        f"{mute_status}"
        if is_admin(message) else ""
    )

    await reply(message, 
        f"📊 <b>Monitor Status</b>\n\n"
        f"🔍 Symbols watched: <b>{len(MONITORED_SYMBOLS)}</b>\n"
        f"📈 Windows with data: <b>{windows_with_data}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"🔕 On cooldown: <b>{on_cooldown}</b>\n"
        f"⚡ Threshold: <b>±{surge_threshold}%</b>\n"
        f"⏱ Window: <b>{WINDOW_SECONDS}s</b>\n"
        f"🔄 Fetch interval: <b>{FETCH_INTERVAL}s</b>"
        f"{admin_extra}\n\n"
        f"🏆 <b>Top movers right now:</b>\n{top_text}"
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
    await reply(message, 
        f"📋 <b>Monitored symbols ({len(MONITORED_SYMBOLS)}):</b>\n"
        + "\n".join(lines)
        + "\n\n🟢 Active  🟡 Frozen  🔴 Banned"
    )


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
        await reply(message, f"❌ Not found or not banned.")
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
        await reply(message, f"ℹ️ Not subscribed.")
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
        f"CHANNEL_ID: <code>{CHANNEL_ID or 'NOT SET'}</code>\n"
        f"THREAD_ID:  <code>{THREAD_ID or 'NOT SET'}</code>\n"
        f"THRESHOLD:  <code>±{surge_threshold}%</code>\n"
        f"WINDOW:     <code>{WINDOW_SECONDS}s</code>\n"
        f"COOLDOWN:   <code>{COOLDOWN_SECONDS}s</code>"
    )


@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    await reply(message, "🔍 Hitting API, please wait...")
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
                # Also check bid1/ask1 availability on a sample ticker
                sample = next((t for t in data if t.get("symbol") in found), None)
                book_info = ""
                if sample:
                    bid1 = sample.get("bid1") or sample.get("bidPrice")
                    ask1 = sample.get("ask1") or sample.get("askPrice")
                    book_info = f"\n\n📖 Sample bid1/ask1: <b>{bid1} / {ask1}</b>"
                matched_text = "\n".join(
                    f"  ✅ <b>{base}</b> → <code>{sym}</code>"
                    for sym, base in sorted(found.items(), key=lambda x: x[1])
                )
                await reply(message, 
                    f"✅ <b>API OK</b> — {len(data)} tickers\n"
                    f"Matched: <b>{len(found)}/{len(HARDCODED_SYMBOLS)}</b>{book_info}\n\n"
                    f"{matched_text}"
                )
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
    await reply(message,
        "🧪 <b>Test mode activated!</b>\n"
        "Scanning for first symbol with ≥ <b>0.1%</b> move via bid1/ask1.\n"
        f"One alert fires then returns to <b>±{surge_threshold}%</b>."
    )
    best_symbol  = None
    best_pct     = 0.0
    best_price   = 0.0
    best_ptype   = "bid1"
    best_elapsed = 0
    now          = time.time()

    for symbol, tr in trackers.items():
        if symbol in banned_symbols:
            continue
        if symbol in frozen_symbols and frozen_symbols[symbol] > now:
            continue

        # Check UP: rise from floor
        if tr.floor_price > 0 and tr.floor_time > 0:
            # we don't have a current ask1 here — skip; test mode fires in the loop
            pass

    # No snapshot data available at command time — fall back to background test mode
    test_mode    = True
    test_chat_id = message.chat.id
    await reply(message, "⏳ Watching for next 0.1%+ move — alert will fire to the group thread.")




# Catch-all: silently ignore any non-command messages (prevents "not handled" log spam)
@router.message()
async def cmd_catch_all(message: Message) -> None:
    pass


# ---------------------------------------------------------------------------
# MEXC API
# ---------------------------------------------------------------------------


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


# ── CHANGED: parse bid1 + ask1 instead of lastPrice ──
def parse_book(ticker: dict) -> Optional[tuple[float, float]]:
    """Extract (bid1, ask1). Returns None if either is missing."""
    try:
        bid1 = float(ticker.get("bid1") or ticker.get("bidPrice") or 0)
        ask1 = float(ticker.get("ask1") or ticker.get("askPrice") or 0)
        if bid1 > 0 and ask1 > 0:
            return bid1, ask1
    except (TypeError, ValueError):
        pass
    return None


# ---------------------------------------------------------------------------
# Sliding window — CHANGED to store BookSnapshot
# ---------------------------------------------------------------------------


SPLASH_WINDOW_DAY:   int = 15   # seconds — market hours (08:00–21:00 UTC)
SPLASH_WINDOW_NIGHT: int = 60   # seconds — night hours  (21:00–08:00 UTC)


def get_splash_window() -> int:
    """Returns 15s during market hours, 60s at night (23:00–08:00 UTC)."""
    import datetime
    h = datetime.datetime.utcnow().hour
    return SPLASH_WINDOW_DAY if 8 <= h < 23 else SPLASH_WINDOW_NIGHT


# Keep for reference
SPLASH_WINDOW: int = SPLASH_WINDOW_DAY


def update_and_check(
    symbol: str,
    now: float,
    bid1: float,
    ask1: float,
    threshold: float,
) -> Optional[tuple[float, float, str, int]]:
    """
    Core splash detection logic.

    Tracks the LOWEST ask1 (floor) for UP splashes and
    the HIGHEST bid1 (ceiling) for DOWN crashes.

    Returns (pct_change, price, price_type, elapsed_seconds) or None.

    Rules:
      - If ask1 sets a new low → update floor + reset timer
      - If ask1 >= floor * (1 + threshold/100) → UP alert, reset
      - If 60s pass since floor was set and threshold not reached → reset
      (same logic mirrored for DOWN via bid1)
    """
    tr = trackers[symbol]

    result = None

    # ── UP: track lowest ask1 ─────────────────────────────────────────────
    if ask1 > 0:
        if tr.floor_price == 0.0:
            # First reading — initialise floor
            tr.floor_price = ask1
            tr.floor_time  = now
        elif ask1 < tr.floor_price:
            # New low → update floor, restart timer
            tr.floor_price = ask1
            tr.floor_time  = now
        else:
            elapsed = now - tr.floor_time
            rise_pct = (ask1 - tr.floor_price) / tr.floor_price * 100

            if rise_pct >= threshold:
                # 🚀 SPLASH triggered
                result = (rise_pct, ask1, "ask1", int(elapsed))
                # Reset for next cycle
                tr.floor_price = ask1
                tr.floor_time  = now
            elif elapsed > get_splash_window():
                # Timeout — no splash within window, reset
                tr.floor_price = ask1
                tr.floor_time  = now

    # ── DOWN: track highest bid1 ──────────────────────────────────────────
    if bid1 > 0 and result is None:
        if tr.ceil_price == 0.0:
            tr.ceil_price = bid1
            tr.ceil_time  = now
        elif bid1 > tr.ceil_price:
            tr.ceil_price = bid1
            tr.ceil_time  = now
        else:
            elapsed  = now - tr.ceil_time
            fall_pct = (tr.ceil_price - bid1) / tr.ceil_price * 100

            if fall_pct >= threshold:
                result = (-fall_pct, bid1, "bid1", int(elapsed))
                tr.ceil_price = bid1
                tr.ceil_time  = now
            elif elapsed > get_splash_window():
                    tr.ceil_price = bid1
                    tr.ceil_time  = now

    return result


# Keep for test mode compatibility
def check_movement(symbol: str, threshold: float) -> Optional[tuple[float, float, str]]:
    tr = trackers.get(symbol)
    if not tr or tr.floor_price == 0.0:
        return None
    ask1 = tr.floor_price  # use last known floor as proxy
    return None  # test mode will use update_and_check directly


# ---------------------------------------------------------------------------
# Monitoring loop
# ---------------------------------------------------------------------------


async def auto_mute_scheduler(bot: Bot) -> None:
    global muted_until
    # Track each window independently so one firing doesn't block another
    last_muted:   dict[str, object] = {}   # window_key → date
    last_unmuted: dict[str, object] = {}   # window_key → date

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
                    try:
                        await bot.send_message(chat_id=CHANNEL_ID, text=w["mute_msg"],
                                               message_thread_id=THREAD_ID)
                    except Exception:
                        pass

                elif (not w["active"]
                      and last_muted.get(key) == today
                      and last_unmuted.get(key) != today
                      and h >= w["unmute_h"] and m >= w["unmute_m"]):
                    muted_until          = 0.0
                    last_unmuted[key]    = today
                    logger.info("Auto-unmute: %s window passed", key)
                    try:
                        await bot.send_message(chat_id=CHANNEL_ID, text=w["unmute_msg"],
                                               message_thread_id=THREAD_ID)
                    except Exception:
                        pass

        except Exception as exc:
            logger.error("Auto-mute scheduler error: %s", exc)


async def monitoring_loop(bot: Bot) -> None:
    global test_mode, test_chat_id, MONITORED_SYMBOLS, symbols_discovered

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info(
            "Monitoring started — interval=%.1fs window=%ds cooldown=%ds",
            FETCH_INTERVAL, WINDOW_SECONDS, COOLDOWN_SECONDS,
        )

        while True:
            cycle_start = time.monotonic()
            raw_data = await fetch_raw_tickers(session)

            if raw_data:
                now = time.time()

                if not symbols_discovered:
                    found, not_found = discover_symbols(raw_data)
                    MONITORED_SYMBOLS = set(found.keys())
                    for sym in MONITORED_SYMBOLS:
                        trackers[sym] = SplashTracker()
                    symbols_discovered = True
                    logger.info("Ready: %d symbols (%d missing)", len(MONITORED_SYMBOLS), len(not_found))

                # Clean expired freezes
                for s in [s for s, t in list(frozen_symbols.items()) if t <= now]:
                    frozen_symbols.pop(s)

                ticker_map = {t.get("symbol", ""): t for t in raw_data}

                for sym in MONITORED_SYMBOLS:
                    if sym in banned_symbols or sym in frozen_symbols:
                        continue

                    ticker = ticker_map.get(sym)
                    if not ticker:
                        continue

                    book = parse_book(ticker)
                    if book is None:
                        continue
                    bid1, ask1 = book

                    # Initialise tracker on first sight
                    if sym not in trackers:
                        trackers[sym] = SplashTracker()

                    # Test mode — use low 0.1% threshold
                    if test_mode:
                        result = update_and_check(sym, now, bid1, ask1, 0.1)
                        if result:
                            pct, price, ptype, elapsed = result
                            test_mode = False
                            target = None if CHANNEL_ID else test_chat_id
                            asyncio.create_task(
                                send_surge_alert(bot, sym, pct, price, ptype,
                                                 elapsed_seconds=elapsed,
                                                 chat_id=target, is_test=True)
                            )
                        continue

                    if is_muted():
                        # Still update tracker so floor/ceiling stay fresh
                        update_and_check(sym, now, bid1, ask1, 9999)
                        continue

                    result = update_and_check(sym, now, bid1, ask1, surge_threshold)
                    if result:
                        pct, price, ptype, elapsed = result
                        if pct > 0:
                            last_sent = last_surge_alert.get(sym, 0.0)
                            if now - last_sent >= COOLDOWN_SECONDS:
                                last_surge_alert[sym] = now
                                asyncio.create_task(
                                    send_surge_alert(bot, sym, pct, price, ptype,
                                                     elapsed_seconds=elapsed)
                                )
                        else:
                            last_sent = last_crash_alert.get(sym, 0.0)
                            if now - last_sent >= COOLDOWN_SECONDS:
                                last_crash_alert[sym] = now
                                asyncio.create_task(
                                    send_surge_alert(bot, sym, pct, price, ptype,
                                                     elapsed_seconds=elapsed)
                                )

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(0.0, FETCH_INTERVAL - elapsed))


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
        "Bot @%s started — threshold=±%.1f%% channel=%s thread=%s subscribers=%d",
        me.username, surge_threshold,
        CHANNEL_ID or "NOT SET",
        THREAD_ID or "NOT SET",
        len(subscribers),
    )

    # Init Yahoo crumb in background (non-blocking)
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _refresh_yahoo_crumb)

    # Verify we can actually reach the thread at startup
    if CHANNEL_ID:
        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text="🟢 <b>Splash bot started</b> — monitoring active.",
                message_thread_id=THREAD_ID,
            )
            logger.info("Startup ping to thread OK")
        except Exception as exc:
            logger.error("STARTUP PING FAILED — check CHAT_ID/THREAD_ID: %s", exc)

    monitor_task   = asyncio.create_task(monitoring_loop(bot))
    automute_task  = asyncio.create_task(auto_mute_scheduler(bot))

    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message"],
        )
    finally:
        monitor_task.cancel()
        automute_task.cancel()
        for task in (monitor_task, automute_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await bot.session.close()
        logger.info("Bot shut down")


if __name__ == "__main__":
    asyncio.run(main())
