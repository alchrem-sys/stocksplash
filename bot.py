"""
MEXC Futures Splash Monitor Bot
Version: 5.0.0 — bid1/ask1 based movement detection

Logic:
  UP   movement → measured via bid1  (you sell into bid1 when price rises)
  DOWN movement → measured via ask1  (you buy  at  ask1 when price drops)

Alert fires when either direction exceeds SURGE_THRESHOLD% within WINDOW_SECONDS.
"""

import asyncio
import logging
import os
import time
from collections import deque
from pathlib import Path
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
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
logger = logging.getLogger("splash_bot")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
CHAT_ID:   str = os.getenv("CHAT_ID",   "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env")
if not CHAT_ID:
    raise RuntimeError("CHAT_ID is missing from .env")

FETCH_INTERVAL:   float = float(os.getenv("FETCH_INTERVAL",   "2"))
SURGE_THRESHOLD:  float = float(os.getenv("SURGE_THRESHOLD",  "1.0"))
WINDOW_SECONDS:   int   = int(os.getenv("WINDOW_SECONDS",     "60"))
COOLDOWN_SECONDS: int   = int(os.getenv("COOLDOWN_SECONDS",   "60"))

MEXC_TICKER_URLS = [
    "https://contract.mexc.com/api/v1/contract/ticker",
    "https://futures.mexc.com/api/v1/contract/ticker",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://futures.mexc.com/",
    "Origin":          "https://futures.mexc.com",
}

TARGET_BASE_NAMES = [
    "COIN", "FIG",  "TSLA", "CVNA", "NVDA",
    "AMAT", "GOOGL","QCOM", "CRM",  "AMZN", "FUTU",
    "AAPL", "MU",   "SHOP", "WMT",  "MSFT",
    "QQQ",  "CSCO", "HOOD", "KO",
    "VZ",   "INTC", "GE",   "JNJ",  "MA",
    "AMD",  "META", "RDDT", "SPOT", "NFLX",
    "ORCL", "ASML", "PEP",  "ACN",  "XOM",
    "V",    "NKE",  "SMCI", "UNH",  "NOW",
    "GS",   "LLY",  "LRCX", "IBM",  "COST",
    "BA",   "JD",   "JPM",
]

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

from dataclasses import dataclass

@dataclass
class BookSnapshot:
    """bid1 + ask1 at a point in time."""
    ts:   float
    bid1: float
    ask1: float


# symbol → deque of BookSnapshot
book_windows: dict[str, deque[BookSnapshot]] = {}

# symbol → timestamp of last alert
last_alert_time: dict[str, float] = {}

# Dynamically discovered symbols
MONITORED_SYMBOLS: set[str]  = set()
symbols_discovered: bool      = False
active_url_index:   int       = 0

# Test mode
test_mode:    bool            = False
test_chat_id: Optional[int]   = None

# ---------------------------------------------------------------------------
# Symbol discovery
# ---------------------------------------------------------------------------

def discover_symbols(api_symbols: set[str]) -> set[str]:
    found, missing = [], []
    for base in TARGET_BASE_NAMES:
        sym = f"{base}_USDT"
        if sym in api_symbols:
            found.append(sym)
        else:
            missing.append(base)
    if missing:
        logger.warning("Not on MEXC futures: %s", ", ".join(missing))
    logger.info("Discovered %d/%d symbols", len(found), len(TARGET_BASE_NAMES))
    return set(found)

# ---------------------------------------------------------------------------
# MEXC API — fetch raw tickers
# ---------------------------------------------------------------------------

async def fetch_raw_tickers(
    session: aiohttp.ClientSession,
) -> Optional[list[dict]]:
    global active_url_index

    order = list(range(len(MEXC_TICKER_URLS)))
    order = order[active_url_index:] + order[:active_url_index]

    for idx in order:
        url = MEXC_TICKER_URLS[idx]
        try:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15),
                ssl=True,
            ) as resp:
                if resp.status != 200:
                    logger.warning("[%s] HTTP %d", url, resp.status)
                    continue
                payload: dict = await resp.json(content_type=None)
                if not payload.get("success"):
                    logger.warning("[%s] success=false", url)
                    continue
                data: list[dict] = payload.get("data", [])
                if data:
                    if idx != active_url_index:
                        logger.info("Switched to: %s", url)
                        active_url_index = idx
                    return data
        except asyncio.TimeoutError:
            logger.warning("[%s] Timed out", url)
        except Exception as exc:
            logger.warning("[%s] Error: %s", url, exc)

    logger.error("All API endpoints failed")
    return None


def parse_book(ticker: dict) -> Optional[tuple[float, float]]:
    """Extract (bid1, ask1) from a ticker dict. Returns None if missing."""
    try:
        bid1 = float(ticker.get("bid1") or ticker.get("bidPrice") or 0)
        ask1 = float(ticker.get("ask1") or ticker.get("askPrice") or 0)
        if bid1 > 0 and ask1 > 0:
            return bid1, ask1
    except (TypeError, ValueError):
        pass
    return None

# ---------------------------------------------------------------------------
# Sliding window + movement detection
# ---------------------------------------------------------------------------

def update_window(symbol: str, now: float, bid1: float, ask1: float) -> None:
    window = book_windows[symbol]
    window.append(BookSnapshot(ts=now, bid1=bid1, ask1=ask1))
    cutoff = now - WINDOW_SECONDS
    while window and window[0].ts < cutoff:
        window.popleft()


def check_movement(
    symbol: str,
    threshold: float,
) -> Optional[tuple[str, float, float]]:
    """
    Returns (direction, pct_change, current_price) if threshold exceeded.

    UP   → bid1 moved up   → (bid_now - bid_old) / bid_old * 100
    DOWN → ask1 moved down → (ask_old - ask_now) / ask_old * 100

    We return the actionable price:
      UP   → current bid1  (you can sell here)
      DOWN → current ask1  (you can buy  here)
    """
    window = book_windows[symbol]
    if len(window) < 2:
        return None

    oldest = window[0]
    latest = window[-1]

    # UP: bid1 rose
    if oldest.bid1 > 0:
        up_pct = (latest.bid1 - oldest.bid1) / oldest.bid1 * 100
        if up_pct >= threshold:
            return "UP", up_pct, latest.bid1

    # DOWN: ask1 fell
    if oldest.ask1 > 0:
        down_pct = (oldest.ask1 - latest.ask1) / oldest.ask1 * 100
        if down_pct >= threshold:
            return "DOWN", down_pct, latest.ask1

    return None

# ---------------------------------------------------------------------------
# Alert builders
# ---------------------------------------------------------------------------

def build_alert_message(
    symbol:        str,
    direction:     str,
    pct_change:    float,
    current_price: float,
    is_test:       bool = False,
) -> str:
    display = symbol.replace("_", "")
    arrow   = "📈" if direction == "UP" else "📉"
    action  = "LONG" if direction == "DOWN" else "SHORT"
    price_label = "bid1" if direction == "UP" else "ask1"
    header  = "🧪 <b>[TEST ALERT]</b>\n\n" if is_test else ""
    footer  = f"\n\n<i>Test — real threshold is {SURGE_THRESHOLD}%</i>" if is_test else ""

    return (
        f"{header}"
        f"{arrow} <b>{action} {display} | {pct_change:.2f}% splash</b>\n\n"
        f"📌 MEXC Futures ({price_label}): <b>${current_price:.4f}</b>\n"
        f"⏱ Within last <b>{WINDOW_SECONDS}s</b>"
        f"{footer}"
    )


def build_keyboard(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📊 MEXC Futures",
            url=f"https://futures.mexc.com/exchange/{symbol}",
        )
    ]])


async def send_alert(
    bot:           Bot,
    symbol:        str,
    direction:     str,
    pct_change:    float,
    current_price: float,
    chat_id:       Optional[str | int] = None,
    is_test:       bool = False,
) -> None:
    target = chat_id or CHAT_ID
    try:
        await bot.send_message(
            chat_id=target,
            text=build_alert_message(symbol, direction, pct_change, current_price, is_test),
            reply_markup=build_keyboard(symbol),
        )
        logger.info(
            "%s alert: %s %s %.2f%% @ $%.4f",
            "TEST" if is_test else "ALERT",
            direction, symbol, pct_change, current_price,
        )
    except Exception as exc:
        logger.error("Failed to send alert for %s: %s", symbol, exc)

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    status = (
        f"📡 Watching <b>{len(MONITORED_SYMBOLS)}</b> symbols"
        if symbols_discovered
        else "⏳ Discovering symbols..."
    )
    await message.answer(
        "👋 <b>MEXC Splash Monitor running!</b>\n\n"
        f"{status}\n"
        f"🚨 Alert at <b>±{SURGE_THRESHOLD}%</b> within <b>{WINDOW_SECONDS}s</b>\n"
        f"🔕 Cooldown: <b>{COOLDOWN_SECONDS}s</b> per symbol\n\n"
        "<b>How it works:</b>\n"
        "📈 UP move → measured via <b>bid1</b>\n"
        "📉 DOWN move → measured via <b>ask1</b>\n\n"
        "<b>Commands:</b>\n"
        "/status — live stats + top movers\n"
        "/test — scan at 0.1% threshold\n"
        "/threshold X — change alert threshold\n"
        "/window X — change window seconds\n"
    )


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return

    now = time.time()
    windows_with_data = sum(1 for w in book_windows.values() if len(w) >= 2)
    on_cooldown       = sum(1 for t in last_alert_time.values() if now - t < COOLDOWN_SECONDS)

    # Top movers (both directions)
    movers = []
    for symbol, window in book_windows.items():
        if len(window) < 2:
            continue
        oldest, latest = window[0], window[-1]
        if oldest.bid1 > 0:
            up_pct = (latest.bid1 - oldest.bid1) / oldest.bid1 * 100
            movers.append((symbol, "UP",   up_pct,   latest.bid1))
        if oldest.ask1 > 0:
            down_pct = (oldest.ask1 - latest.ask1) / oldest.ask1 * 100
            movers.append((symbol, "DOWN", down_pct, latest.ask1))

    movers.sort(key=lambda x: abs(x[2]), reverse=True)
    top5 = movers[:5]

    top_text = ""
    for sym, direction, pct, price in top5:
        arrow = "📈" if direction == "UP" else "📉"
        top_text += f"  {arrow} <b>{sym.replace('_','')}</b>: {pct:+.3f}% @ ${price:.4f}\n"
    if not top_text:
        top_text = "  ⏳ Not enough data yet\n"

    await message.answer(
        f"📊 <b>Splash Bot Status</b>\n\n"
        f"🔍 Symbols watched: <b>{len(MONITORED_SYMBOLS)}</b>\n"
        f"📈 Windows with data: <b>{windows_with_data}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"🔕 On cooldown: <b>{on_cooldown}</b>\n"
        f"⚡ Alert threshold: <b>{SURGE_THRESHOLD}%</b>\n"
        f"⏱ Window: <b>{WINDOW_SECONDS}s</b>\n"
        f"🔄 Fetch interval: <b>{FETCH_INTERVAL}s</b>\n\n"
        f"🏆 <b>Top movers right now:</b>\n{top_text}"
    )


@router.message(Command("test"))
async def cmd_test(message: Message, bot: Bot) -> None:
    global test_mode, test_chat_id

    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return

    await message.answer(
        "🧪 <b>Test mode activated!</b>\n\n"
        "Scanning for first symbol that moved <b>≥ 0.1%</b> via bid1/ask1.\n"
        f"Will send one alert then return to <b>{SURGE_THRESHOLD}% threshold</b>."
    )

    # Scan immediately
    best = None
    for symbol in MONITORED_SYMBOLS:
        result = check_movement(symbol, 0.1)
        if result:
            direction, pct, price = result
            if best is None or pct > best[1]:
                best = (symbol, pct, direction, price)

    if best:
        symbol, pct, direction, price = best
        await bot.send_message(
            chat_id=message.chat.id,
            text=build_alert_message(symbol, direction, pct, price, is_test=True),
            reply_markup=build_keyboard(symbol),
        )
        logger.info("Test alert: %s %s %.3f%%", direction, symbol, pct)
    else:
        test_mode    = True
        test_chat_id = message.chat.id
        await message.answer(
            "⏳ No symbol at 0.1%+ yet.\n\n"
            "Waiting in background — you'll get an alert the moment "
            "any bid1/ask1 moves 0.1%, then it returns to "
            f"<b>{SURGE_THRESHOLD}% threshold</b>."
        )


@router.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    global SURGE_THRESHOLD
    args = (message.text or "").split()[1:]
    if not args:
        await message.answer(f"Current threshold: <b>{SURGE_THRESHOLD}%</b>\nUsage: <code>/threshold 0.5</code>")
        return
    try:
        val = float(args[0])
        if not 0.01 <= val <= 100:
            raise ValueError
        SURGE_THRESHOLD = val
        await message.answer(f"✅ Threshold set to <b>{val}%</b>")
    except ValueError:
        await message.answer("❌ Invalid value. Example: <code>/threshold 0.5</code>")


@router.message(Command("window"))
async def cmd_window(message: Message) -> None:
    global WINDOW_SECONDS
    args = (message.text or "").split()[1:]
    if not args:
        await message.answer(f"Current window: <b>{WINDOW_SECONDS}s</b>\nUsage: <code>/window 60</code>")
        return
    try:
        val = int(args[0])
        if not 5 <= val <= 3600:
            raise ValueError
        WINDOW_SECONDS = val
        # Clear all windows since they're now wrong size
        for w in book_windows.values():
            w.clear()
        await message.answer(f"✅ Window set to <b>{val}s</b>")
    except ValueError:
        await message.answer("❌ Invalid value (5–3600). Example: <code>/window 30</code>")

# ---------------------------------------------------------------------------
# Monitoring loop
# ---------------------------------------------------------------------------

async def monitoring_loop(bot: Bot) -> None:
    global symbols_discovered, MONITORED_SYMBOLS, test_mode, test_chat_id

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info(
            "Splash monitor started — interval=%.1fs threshold=%.2f%% window=%ds",
            FETCH_INTERVAL, SURGE_THRESHOLD, WINDOW_SECONDS,
        )

        while True:
            cycle_start = time.monotonic()
            raw_data    = await fetch_raw_tickers(session)

            if raw_data:
                now = time.time()

                # First run: discover symbols
                if not symbols_discovered:
                    api_symbols    = {t.get("symbol", "") for t in raw_data}
                    MONITORED_SYMBOLS = discover_symbols(api_symbols)
                    for sym in MONITORED_SYMBOLS:
                        book_windows[sym] = deque()
                    symbols_discovered = True
                    logger.info(
                        "Tracking %d symbols: %s",
                        len(MONITORED_SYMBOLS),
                        sorted(MONITORED_SYMBOLS),
                    )

                # Process each ticker
                for ticker in raw_data:
                    sym = ticker.get("symbol", "")
                    if sym not in MONITORED_SYMBOLS:
                        continue

                    book = parse_book(ticker)
                    if book is None:
                        continue

                    bid1, ask1 = book
                    update_window(sym, now, bid1, ask1)

                    # Test mode — 0.1% threshold
                    if test_mode:
                        result = check_movement(sym, 0.1)
                        if result:
                            direction, pct, price = result
                            test_mode = False
                            asyncio.create_task(
                                send_alert(bot, sym, direction, pct, price,
                                           chat_id=test_chat_id, is_test=True)
                            )
                            logger.info("Test satisfied by %s %s %.3f%%", direction, sym, pct)
                            continue

                    # Normal mode
                    result = check_movement(sym, SURGE_THRESHOLD)
                    if result:
                        direction, pct, price = result
                        last_sent = last_alert_time.get(sym, 0.0)
                        if now - last_sent >= COOLDOWN_SECONDS:
                            last_alert_time[sym] = now
                            asyncio.create_task(
                                send_alert(bot, sym, direction, pct, price)
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
    logger.info("Authenticated as @%s (id=%d)", me.username, me.id)

    monitor_task = asyncio.create_task(monitoring_loop(bot))

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()
        logger.info("Bot shut down")


if __name__ == "__main__":
    asyncio.run(main())
