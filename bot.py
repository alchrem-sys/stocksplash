"""
MEXC Futures Price Surge Monitor Bot
Version: 6.0.0 — Freeze / Ban / Unban controls
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
logger = logging.getLogger("mexc_surge_bot")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env file")
if not CHAT_ID:
    raise RuntimeError("CHAT_ID is missing from .env file")

FETCH_INTERVAL: float = float(os.getenv("FETCH_INTERVAL", "2"))
SURGE_THRESHOLD: float = float(os.getenv("SURGE_THRESHOLD", "1.0"))
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "60"))
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "60"))

# Only this Telegram user ID can use control commands
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

TARGET_BASE_NAMES = [
    "COIN", "FIG", "TSLA", "CVNA", "NVDA",
    "NAS100", "AMAT", "SP500", "MSTR", "GOOGL",
    "QCOM", "HK50", "CRM", "AMZN", "FUTU",
    "AAPL", "MU", "SHOP", "WMT", "MSFT",
    "US30", "QQQ", "CSCO", "HOOD", "KO",
    "VZ", "INTC", "GE", "JNJ", "MA",
    "AMD", "META", "RDDT", "SPOT", "NFLX",
    "ORCL", "ASML", "PEP", "ACN", "XOM",
    "V", "NKE", "SMCI", "UNH", "NOW",
    "GS", "LLY", "LRCX", "IBM", "COST",
    "BA", "JD", "JPM",
]

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

MONITORED_SYMBOLS: set[str] = set()
price_windows: dict[str, deque[tuple[float, float]]] = {}
last_alert_time: dict[str, float] = {}
symbols_discovered: bool = False
test_mode: bool = False
test_chat_id: Optional[int] = None

# symbol -> permanently banned (no alerts ever)
banned_symbols: set[str] = set()

# symbol -> unfreeze timestamp (alerts paused until that time)
frozen_symbols: dict[str, float] = {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id == ADMIN_ID


async def deny(message: Message) -> None:
    await message.answer("⛔ You are not authorized to use this command.")


def normalize_input(raw: str) -> str:
    """
    Accept any user input format and return the MEXC symbol style.
    Examples: tsla -> TSLA, TSLA_USDT -> TSLA_USDT, tslausdt -> TSLA_USDT
    """
    s = raw.upper().strip()
    if "_" not in s:
        if s.endswith("USDT"):
            s = s[:-4] + "_USDT"
        elif s.endswith("USDC"):
            s = s[:-4] + "_USDC"
        else:
            s = s + "_USDT"
    return s


def find_symbol(user_input: str) -> Optional[str]:
    """Return the matching monitored symbol or None."""
    normalized = normalize_input(user_input)
    if normalized in MONITORED_SYMBOLS:
        return normalized
    # Fallback: match by base name substring
    base = normalized.split("_")[0]
    for sym in MONITORED_SYMBOLS:
        if sym.startswith(base + "_"):
            return sym
    return None


def symbol_status(sym: str) -> str:
    if sym in banned_symbols:
        return "🔴 BANNED"
    if sym in frozen_symbols:
        remaining = frozen_symbols[sym] - time.time()
        if remaining > 0:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            return f"🟡 FROZEN ({mins}m {secs}s left)"
        else:
            frozen_symbols.pop(sym, None)
    return "🟢 ACTIVE"


# ---------------------------------------------------------------------------
# Symbol discovery
# ---------------------------------------------------------------------------


def discover_symbols(all_tickers: list[dict]) -> tuple[dict[str, str], list[str]]:
    api_symbols: list[str] = [t.get("symbol", "") for t in all_tickers]
    api_symbol_set = set(api_symbols)

    found: dict[str, str] = {}
    not_found: list[str] = []

    for base in TARGET_BASE_NAMES:
        match = None

        for suffix in ("_USDT", "_USDC"):
            candidate = f"{base}{suffix}"
            if candidate in api_symbol_set:
                match = candidate
                break

        if not match:
            for sym in api_symbols:
                if sym.split("_")[0].upper() == base.upper():
                    match = sym
                    break

        if not match:
            candidates = [
                sym for sym in api_symbols
                if base.upper() in sym.upper()
                and ("USDT" in sym or "USDC" in sym)
            ]
            candidates.sort(key=len)
            if candidates:
                match = candidates[0]

        if match:
            found[match] = base
            logger.info("Matched: %-10s -> %s", base, match)
        else:
            not_found.append(base)
            logger.warning("NOT FOUND on MEXC: %s", base)

    return found, not_found


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = Router()


# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------

@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    status = (
        f"📡 Watching <b>{len(MONITORED_SYMBOLS)}</b> symbols"
        if symbols_discovered
        else "⏳ Discovering symbols..."
    )
    admin_hint = "\n\n<b>Admin commands:</b>\n/ban TSLA — permanently silence a ticker\n/freeze TSLA 30 — silence for N minutes\n/unban TSLA — restore a banned ticker\n/unfreeze TSLA — restore a frozen ticker\n/blocked — show banned + frozen list" if message.from_user and message.from_user.id == ADMIN_ID else ""
    await message.answer(
        f"👋 <b>MEXC Surge Monitor is running!</b>\n\n"
        f"{status}\n"
        f"🚨 Alerts at <b>+{SURGE_THRESHOLD}%</b> within <b>{WINDOW_SECONDS}s</b>\n"
        f"🔕 Cooldown: <b>{COOLDOWN_SECONDS}s</b>\n\n"
        "<b>Commands:</b>\n"
        "/status — live stats + top 5 movers\n"
        "/symbols — all matched symbols\n"
        "/test — one-time scan at 0.1%\n"
        "/debug — API diagnostic"
        f"{admin_hint}"
    )


# ---------------------------------------------------------------------------
# /ban  — permanently silence a symbol
# Usage: /ban TSLA   or   /ban TSLA_USDT
# ---------------------------------------------------------------------------

@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if not args:
        await message.answer(
            "Usage: <code>/ban SYMBOL</code>\n"
            "Example: <code>/ban TSLA</code>\n\n"
            "Permanently silences all alerts for that ticker.\n"
            "Use /unban to restore."
        )
        return

    sym = find_symbol(args[0])
    if sym is None:
        await message.answer(
            f"❌ Symbol <code>{args[0].upper()}</code> not found in monitored list.\n"
            "Send /symbols to see available tickers."
        )
        return

    banned_symbols.add(sym)
    frozen_symbols.pop(sym, None)  # remove freeze if any
    logger.info("ADMIN banned symbol: %s", sym)
    await message.answer(
        f"🔴 <b>{sym}</b> is now <b>BANNED</b>.\n"
        "No alerts will fire for this ticker until you use:\n"
        f"<code>/unban {sym.split('_')[0]}</code>"
    )


# ---------------------------------------------------------------------------
# /unban  — restore a banned symbol
# ---------------------------------------------------------------------------

@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if not args:
        await message.answer("Usage: <code>/unban SYMBOL</code>")
        return

    sym = find_symbol(args[0])
    if sym is None:
        await message.answer(f"❌ Symbol <code>{args[0].upper()}</code> not found.")
        return

    if sym not in banned_symbols:
        await message.answer(f"ℹ️ <b>{sym}</b> is not banned.")
        return

    banned_symbols.discard(sym)
    logger.info("ADMIN unbanned symbol: %s", sym)
    await message.answer(
        f"✅ <b>{sym}</b> is now <b>ACTIVE</b> again.\n"
        "Alerts will resume normally."
    )


# ---------------------------------------------------------------------------
# /freeze  — silence a symbol for N minutes
# Usage: /freeze TSLA 30
# ---------------------------------------------------------------------------

@router.message(Command("freeze"))
async def cmd_freeze(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if len(args) < 2:
        await message.answer(
            "Usage: <code>/freeze SYMBOL MINUTES</code>\n"
            "Example: <code>/freeze TSLA 30</code>\n\n"
            "Pauses alerts for that ticker for N minutes.\n"
            "Use /unfreeze to restore early."
        )
        return

    sym = find_symbol(args[0])
    if sym is None:
        await message.answer(
            f"❌ Symbol <code>{args[0].upper()}</code> not found.\n"
            "Send /symbols to see available tickers."
        )
        return

    try:
        minutes = float(args[1])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Minutes must be a positive number. Example: <code>/freeze TSLA 30</code>")
        return

    unfreeze_at = time.time() + minutes * 60
    frozen_symbols[sym] = unfreeze_at
    logger.info("ADMIN froze symbol: %s for %.1f minutes", sym, minutes)

    hours = int(minutes // 60)
    mins = int(minutes % 60)
    duration_str = f"{hours}h {mins}m" if hours else f"{mins}m"

    await message.answer(
        f"🟡 <b>{sym}</b> is now <b>FROZEN</b> for <b>{duration_str}</b>.\n"
        "No alerts will fire during this period.\n\n"
        f"Resumes automatically, or use:\n"
        f"<code>/unfreeze {sym.split('_')[0]}</code>"
    )


# ---------------------------------------------------------------------------
# /unfreeze  — restore a frozen symbol early
# ---------------------------------------------------------------------------

@router.message(Command("unfreeze"))
async def cmd_unfreeze(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if not args:
        await message.answer("Usage: <code>/unfreeze SYMBOL</code>")
        return

    sym = find_symbol(args[0])
    if sym is None:
        await message.answer(f"❌ Symbol <code>{args[0].upper()}</code> not found.")
        return

    if sym not in frozen_symbols:
        await message.answer(f"ℹ️ <b>{sym}</b> is not frozen.")
        return

    frozen_symbols.pop(sym)
    logger.info("ADMIN unfroze symbol: %s", sym)
    await message.answer(
        f"✅ <b>{sym}</b> is now <b>ACTIVE</b> again.\n"
        "Alerts will resume immediately."
    )


# ---------------------------------------------------------------------------
# /blocked  — show all banned and frozen symbols
# ---------------------------------------------------------------------------

@router.message(Command("blocked"))
async def cmd_blocked(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return

    now = time.time()

    # Clean expired freezes
    expired = [s for s, t in frozen_symbols.items() if t <= now]
    for s in expired:
        frozen_symbols.pop(s)

    banned_text = ""
    for sym in sorted(banned_symbols):
        banned_text += f"  🔴 <b>{sym}</b> — permanently banned\n"

    frozen_text = ""
    for sym, until in sorted(frozen_symbols.items()):
        remaining = until - now
        mins = int(remaining // 60)
        secs = int(remaining % 60)
        frozen_text += f"  🟡 <b>{sym}</b> — {mins}m {secs}s remaining\n"

    if not banned_text and not frozen_text:
        await message.answer("✅ No symbols are currently banned or frozen.")
        return

    parts = ["🚫 <b>Blocked Symbols</b>\n"]
    if banned_text:
        parts.append(f"<b>Banned ({len(banned_symbols)}):</b>\n{banned_text}")
    if frozen_text:
        parts.append(f"<b>Frozen ({len(frozen_symbols)}):</b>\n{frozen_text}")

    await message.answer("\n".join(parts))


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return

    now = time.time()
    windows_with_data = sum(1 for w in price_windows.values() if len(w) >= 2)
    on_cooldown = sum(1 for t in last_alert_time.values() if now - t < COOLDOWN_SECONDS)

    movers = []
    for symbol, window in price_windows.items():
        if len(window) < 2:
            continue
        _, oldest_price = window[0]
        _, latest_price = window[-1]
        if oldest_price <= 0:
            continue
        pct = (latest_price / oldest_price - 1) * 100.0
        movers.append((symbol, pct, latest_price))

    movers.sort(key=lambda x: x[1], reverse=True)

    top_text = ""
    for sym, pct, price in movers[:5]:
        arrow = "📈" if pct >= 0 else "📉"
        tag = ""
        if sym in banned_symbols:
            tag = " 🔴"
        elif sym in frozen_symbols and frozen_symbols[sym] > now:
            tag = " 🟡"
        top_text += f"  {arrow} <b>{sym}</b>{tag}: {pct:+.3f}% @ ${price:.4f}\n"

    if not top_text:
        top_text = "  ⏳ Filling window, wait 10s...\n"

    await message.answer(
        f"📊 <b>Monitor Status</b>\n\n"
        f"🔍 Symbols matched: <b>{len(MONITORED_SYMBOLS)}</b>\n"
        f"📈 Windows with data: <b>{windows_with_data}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"🔕 On cooldown: <b>{on_cooldown}</b>\n"
        f"🔴 Banned: <b>{len(banned_symbols)}</b>\n"
        f"🟡 Frozen: <b>{len(frozen_symbols)}</b>\n"
        f"⚡ Alert threshold: <b>{SURGE_THRESHOLD}%</b>\n"
        f"⏱ Window: <b>{WINDOW_SECONDS}s</b>\n"
        f"🔄 Fetch interval: <b>{FETCH_INTERVAL}s</b>\n\n"
        f"🏆 <b>Top 5 movers right now:</b>\n{top_text}"
    )


# ---------------------------------------------------------------------------
# /symbols
# ---------------------------------------------------------------------------

@router.message(Command("symbols"))
async def cmd_symbols(message: Message) -> None:
    if not symbols_discovered:
        await message.answer("⏳ Still discovering, try again in a few seconds.")
        return

    now = time.time()
    lines = []
    for sym in sorted(MONITORED_SYMBOLS):
        if sym in banned_symbols:
            tag = "🔴"
        elif sym in frozen_symbols and frozen_symbols[sym] > now:
            remaining = int((frozen_symbols[sym] - now) / 60)
            tag = f"🟡{remaining}m"
        else:
            tag = "🟢"
        lines.append(f"  {tag} <code>{sym}</code>")

    await message.answer(
        f"📋 <b>Monitored symbols ({len(MONITORED_SYMBOLS)}):</b>\n"
        + "\n".join(lines)
        + "\n\n🟢 Active  🟡 Frozen  🔴 Banned"
    )


# ---------------------------------------------------------------------------
# /debug
# ---------------------------------------------------------------------------

@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    await message.answer(
        f"🔍 Hitting <code>{MEXC_TICKER_URL}</code>\nPlease wait..."
    )

    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(
                MEXC_TICKER_URL,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=True,
            ) as response:
                if response.status != 200:
                    await message.answer(f"❌ HTTP {response.status}")
                    return

                payload = await response.json(content_type=None)
                data: list[dict] = payload.get("data", [])

                if not data:
                    await message.answer(
                        f"⚠️ Connected but data empty!\nKeys: {list(payload.keys())}"
                    )
                    return

                await message.answer(
                    f"✅ <b>API reachable</b>\n"
                    f"Total tickers: <b>{len(data)}</b>\n\n"
                    f"📄 Sample ticker:\n<code>{data[0]}</code>"
                )

                found, not_found = discover_symbols(data)
                matched_text = "\n".join(
                    f"  ✅ {base} → <code>{sym}</code>"
                    for sym, base in sorted(found.items(), key=lambda x: x[1])
                ) or "  none"

                await message.answer(
                    f"🔎 <b>Matched: {len(found)}/{len(TARGET_BASE_NAMES)}</b>\n\n"
                    f"{matched_text}"
                )

                if not_found:
                    await message.answer(
                        f"❌ <b>Not on MEXC ({len(not_found)}):</b>\n"
                        f"<code>{', '.join(sorted(not_found))}</code>"
                    )

        except asyncio.TimeoutError:
            await message.answer("⏱ Timed out. MEXC may be geo-blocking your IP.")
        except Exception as exc:
            await message.answer(f"💥 Error: <code>{exc}</code>")


# ---------------------------------------------------------------------------
# /test
# ---------------------------------------------------------------------------

@router.message(Command("test"))
async def cmd_test(message: Message, bot: Bot) -> None:
    global test_mode, test_chat_id

    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return

    await message.answer(
        "🧪 <b>Test mode activated!</b>\n\n"
        "Scanning for first symbol ≥ <b>0.1%</b> move in 60s.\n"
        f"One alert fires, then returns to <b>{SURGE_THRESHOLD}% threshold</b>.\n\n"
        "⚠️ Banned/frozen symbols are <b>skipped</b> even in test mode."
    )

    best_symbol = None
    best_pct = 0.0
    best_price = 0.0
    now = time.time()

    for symbol, window in price_windows.items():
        if symbol in banned_symbols:
            continue
        if symbol in frozen_symbols and frozen_symbols[symbol] > now:
            continue
        if len(window) < 2:
            continue
        _, oldest_price = window[0]
        _, latest_price = window[-1]
        if oldest_price <= 0:
            continue
        pct = (latest_price / oldest_price - 1) * 100.0
        if pct >= 0.1 and pct > best_pct:
            best_pct = pct
            best_symbol = symbol
            best_price = latest_price

    if best_symbol:
        await bot.send_message(
            chat_id=message.chat.id,
            text=(
                f"🧪 <b>[TEST ALERT — 0.1% threshold]</b>\n\n"
                f"🚨 <b>FUTURES SURGE: #{best_symbol.replace('_', '')}</b>\n"
                f"📈 Change: <b>+{best_pct:.2f}%</b> in 60s\n"
                f"💵 Current Price: <b>${best_price:.4f}</b>\n\n"
                f"✅ Bot is working! Back to <b>{SURGE_THRESHOLD}%</b>."
            ),
            reply_markup=build_trade_keyboard(best_symbol),
        )
    else:
        test_mode = True
        test_chat_id = message.chat.id
        await message.answer(
            "⏳ No symbol at 0.1%+ yet.\n"
            "Watching in background — alert fires the moment any "
            "active symbol crosses 0.1%.\n\n"
            "Send /status to see current movers."
        )


# ---------------------------------------------------------------------------
# Alert builders
# ---------------------------------------------------------------------------


def build_alert_message(
    symbol: str,
    pct_change: float,
    current_price: float,
    is_test: bool = False,
) -> str:
    display = symbol.replace("_", "")
    header = "🧪 <b>[TEST ALERT]</b>\n\n" if is_test else ""
    footer = f"\n\n<i>Test — real threshold is {SURGE_THRESHOLD}%</i>" if is_test else ""
    return (
        f"{header}"
        f"🚨 <b>FUTURES SURGE: #{display}</b>\n"
        f"📈 Change: <b>+{pct_change:.2f}%</b> in 60s\n"
        f"💵 Current Price: <b>${current_price:.4f}</b>"
        f"{footer}"
    )


def build_trade_keyboard(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔗 Open Chart on MEXC",
            url=f"https://futures.mexc.com/exchange/{symbol}"
        )],
        [InlineKeyboardButton(
            text="⚡ Trade Now",
            url=f"https://futures.mexc.com/exchange/{symbol}?type=futures"
        )],
    ])


async def send_alert(
    bot: Bot,
    symbol: str,
    pct_change: float,
    current_price: float,
    chat_id: Optional[str | int] = None,
    is_test: bool = False,
) -> None:
    target = chat_id or CHAT_ID
    try:
        await bot.send_message(
            chat_id=target,
            text=build_alert_message(symbol, pct_change, current_price, is_test),
            reply_markup=build_trade_keyboard(symbol),
        )
        logger.info(
            "%s alert: %s +%.2f%%",
            "TEST" if is_test else "SURGE",
            symbol,
            pct_change,
        )
    except Exception as exc:
        logger.error("Failed to send alert for %s: %s", symbol, exc)


# ---------------------------------------------------------------------------
# MEXC API
# ---------------------------------------------------------------------------


async def fetch_raw_tickers(session: aiohttp.ClientSession) -> Optional[list[dict]]:
    try:
        async with session.get(
            MEXC_TICKER_URL,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
            ssl=True,
        ) as response:
            if response.status != 200:
                logger.warning("HTTP %d from API", response.status)
                return None

            payload: dict = await response.json(content_type=None)

            if not payload.get("success"):
                logger.warning("API returned success=false")
                return None

            data: list[dict] = payload.get("data", [])
            return data or None

    except asyncio.TimeoutError:
        logger.warning("Ticker request timed out")
    except aiohttp.ClientConnectionError as exc:
        logger.warning("Connection error: %s", exc)
    except aiohttp.ClientError as exc:
        logger.warning("Client error: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)

    return None


def parse_price(ticker: dict) -> Optional[float]:
    raw = (
        ticker.get("lastPrice")
        or ticker.get("last_price")
        or ticker.get("last")
        or ticker.get("price")
        or ticker.get("close")
    )
    try:
        return float(raw) if raw is not None else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Sliding window
# ---------------------------------------------------------------------------


def update_window(symbol: str, now: float, price: float) -> None:
    window = price_windows[symbol]
    window.append((now, price))
    cutoff = now - WINDOW_SECONDS
    while window and window[0][0] < cutoff:
        window.popleft()


def get_pct_change(symbol: str) -> Optional[tuple[float, float]]:
    window = price_windows[symbol]
    if len(window) < 2:
        return None
    _, oldest_price = window[0]
    _, latest_price = window[-1]
    if oldest_price <= 0:
        return None
    pct = (latest_price / oldest_price - 1) * 100.0
    return pct, latest_price


# ---------------------------------------------------------------------------
# Monitoring loop
# ---------------------------------------------------------------------------


async def monitoring_loop(bot: Bot) -> None:
    global test_mode, test_chat_id, MONITORED_SYMBOLS, symbols_discovered

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info(
            "Monitoring started — interval=%.1fs threshold=%.2f%% window=%ds cooldown=%ds",
            FETCH_INTERVAL, SURGE_THRESHOLD, WINDOW_SECONDS, COOLDOWN_SECONDS,
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
                        price_windows[sym] = deque()
                    symbols_discovered = True
                    logger.info(
                        "Discovery complete: %d/%d matched: %s",
                        len(MONITORED_SYMBOLS),
                        len(TARGET_BASE_NAMES),
                        sorted(MONITORED_SYMBOLS),
                    )

                # Clean expired freezes
                expired = [s for s, t in frozen_symbols.items() if t <= now]
                for s in expired:
                    frozen_symbols.pop(s)
                    logger.info("Freeze expired for %s — alerts resumed", s)

                ticker_map: dict[str, dict] = {
                    t.get("symbol", ""): t for t in raw_data
                }

                for sym in MONITORED_SYMBOLS:

                    # Skip banned symbols entirely
                    if sym in banned_symbols:
                        continue

                    # Skip frozen symbols
                    if sym in frozen_symbols:
                        continue

                    ticker = ticker_map.get(sym)
                    if ticker is None:
                        continue

                    price = parse_price(ticker)
                    if price is None:
                        continue

                    update_window(sym, now, price)
                    result = get_pct_change(sym)
                    if result is None:
                        continue

                    pct_change, current_price = result

                    # Test mode
                    if test_mode and pct_change >= 0.1:
                        test_mode = False
                        asyncio.create_task(
                            send_alert(
                                bot, sym, pct_change, current_price,
                                chat_id=test_chat_id,
                                is_test=True,
                            )
                        )
                        logger.info("Test satisfied by %s (+%.3f%%), reverting", sym, pct_change)
                        continue

                    # Normal surge alert
                    if pct_change >= SURGE_THRESHOLD:
                        last_sent = last_alert_time.get(sym, 0.0)
                        if now - last_sent >= COOLDOWN_SECONDS:
                            last_alert_time[sym] = now
                            asyncio.create_task(
                                send_alert(bot, sym, pct_change, current_price)
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
    logger.info("Bot authenticated as @%s (id=%d)", me.username, me.id)

    monitor_task = asyncio.create_task(monitoring_loop(bot))

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            logger.info("Monitoring loop cancelled cleanly")
        await bot.session.close()
        logger.info("Bot shut down")


if __name__ == "__main__":
    asyncio.run(main())