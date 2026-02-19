"""
MEXC Futures Price Surge Monitor Bot
Version: 8.0.0 — Surge + Crash alerts, /threshold, /mute, /help, channel mode
"""

import asyncio
import json
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
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from .env file")

FETCH_INTERVAL: float = float(os.getenv("FETCH_INTERVAL", "2"))
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "60"))
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "60"))
CHANNEL_ID: str = os.getenv("CHANNEL_ID", "")

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
# Hardcoded symbol map
# ---------------------------------------------------------------------------

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
# Subscriber storage (for admin DM commands)
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

MONITORED_SYMBOLS: set[str] = set()
price_windows: dict[str, deque[tuple[float, float]]] = {}

# Separate cooldowns for surges and crashes per symbol
last_surge_alert: dict[str, float] = {}
last_crash_alert: dict[str, float] = {}

symbols_discovered: bool = False
test_mode: bool = False
test_chat_id: Optional[int] = None

# Admin controls
banned_symbols: set[str] = set()
frozen_symbols: dict[str, float] = {}  # symbol -> unfreeze timestamp

# Global mute — all alerts paused until this timestamp
muted_until: float = 0.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id == ADMIN_ID


async def deny(message: Message) -> None:
    await message.answer("⛔ You are not authorized to use this command.")


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
# Channel broadcast — single target
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
        )
        logger.info("Alert posted to channel %s", CHANNEL_ID)
    except Exception as exc:
        logger.error("Failed to post to channel: %s", exc)


# ---------------------------------------------------------------------------
# Alert builders
# ---------------------------------------------------------------------------


def build_surge_message(
    symbol: str,
    pct_change: float,
    current_price: float,
    is_test: bool = False,
) -> str:
    display = HARDCODED_SYMBOLS.get(symbol, symbol.replace("_", ""))
    is_crash = pct_change < 0
    emoji = "🔴" if is_crash else "🚨"
    direction = "CRASH" if is_crash else "SURGE"
    arrow = "📉" if is_crash else "📈"
    sign = "" if is_crash else "+"
    header = "🧪 <b>[TEST ALERT]</b>\n\n" if is_test else ""
    footer = f"\n\n<i>Test — real threshold is ±{surge_threshold}%</i>" if is_test else ""
    return (
        f"{header}"
        f"{emoji} <b>FUTURES {direction}: #{display}</b>\n"
        f"{arrow} Change: <b>{sign}{pct_change:.2f}%</b> in 60s\n"
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


async def send_surge_alert(
    bot: Bot,
    symbol: str,
    pct_change: float,
    current_price: float,
    chat_id: Optional[int] = None,
    is_test: bool = False,
) -> None:
    text = build_surge_message(symbol, pct_change, current_price, is_test)
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


# ---------------------------------------------------------------------------
# /help
# ---------------------------------------------------------------------------

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

    await message.answer(
        "<b>📖 MEXC Surge Monitor — Help</b>\n\n"
        "<b>📊 Info commands:</b>\n"
        "/status — live stats + top 5 movers\n"
        "/symbols — all monitored symbols\n"
        "/help — show this message"
        f"{admin_section}\n\n"
        "<b>ℹ️ How it works:</b>\n"
        f"Bot monitors {len(MONITORED_SYMBOLS)} futures symbols every 2s.\n"
        f"Alerts fire when price moves ±{surge_threshold}% within 60s.\n"
        "🚨 = surge  🔴 = crash"
    )


# ---------------------------------------------------------------------------
# /threshold — change alert % at runtime (admin only)
# Usage: /threshold 2.0
# ---------------------------------------------------------------------------

@router.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    global surge_threshold
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if not args:
        await message.answer(
            f"Current threshold: <b>±{surge_threshold}%</b>\n\n"
            "Usage: <code>/threshold 2.0</code>\n"
            "Sets the alert trigger to ±2% moves."
        )
        return

    try:
        new_val = float(args[0])
        if new_val <= 0 or new_val > 100:
            raise ValueError
    except ValueError:
        await message.answer("❌ Must be a number between 0.1 and 100.\nExample: <code>/threshold 2.0</code>")
        return

    old = surge_threshold
    surge_threshold = new_val
    logger.info("ADMIN changed threshold: %.2f%% -> %.2f%%", old, new_val)
    await message.answer(
        f"✅ Threshold updated!\n"
        f"Old: <b>±{old}%</b>\n"
        f"New: <b>±{new_val}%</b>\n\n"
        f"Alerts now fire when any symbol moves ±{new_val}% within 60s."
    )


# ---------------------------------------------------------------------------
# /mute — silence all alerts for N minutes (admin only)
# Usage: /mute 30
# ---------------------------------------------------------------------------

@router.message(Command("mute"))
async def cmd_mute(message: Message) -> None:
    global muted_until
    if not is_admin(message):
        await deny(message)
        return

    args = (message.text or "").split()[1:]
    if not args:
        if is_muted():
            await message.answer(
                f"🔇 Currently muted — <b>{mute_remaining()}</b> remaining.\n"
                "Use /unmute to restore immediately."
            )
        else:
            await message.answer(
                "Usage: <code>/mute MINUTES</code>\n"
                "Example: <code>/mute 30</code>\n\n"
                "Silences ALL channel alerts for N minutes."
            )
        return

    try:
        minutes = float(args[0])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Minutes must be a positive number.")
        return

    muted_until = time.time() + minutes * 60
    h = int(minutes // 60)
    m = int(minutes % 60)
    duration_str = f"{h}h {m}m" if h else f"{m}m"
    logger.info("ADMIN muted all alerts for %.1f minutes", minutes)
    await message.answer(
        f"🔇 <b>All alerts muted for {duration_str}</b>\n"
        f"Resumes automatically, or use /unmute."
    )


# ---------------------------------------------------------------------------
# /unmute — restore alerts immediately (admin only)
# ---------------------------------------------------------------------------

@router.message(Command("unmute"))
async def cmd_unmute(message: Message) -> None:
    global muted_until
    if not is_admin(message):
        await deny(message)
        return

    if not is_muted():
        await message.answer("ℹ️ Bot is not muted.")
        return

    muted_until = 0.0
    logger.info("ADMIN unmuted alerts")
    await message.answer("🔊 <b>Alerts restored!</b> Channel will receive alerts again.")


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
    on_cooldown = sum(
        1 for sym in MONITORED_SYMBOLS
        if now - max(last_surge_alert.get(sym, 0), last_crash_alert.get(sym, 0)) < COOLDOWN_SECONDS
    )

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
    # Show top 3 surges and top 3 crashes
    for sym, pct, price in movers[:3]:
        tag = " 🔴" if sym in banned_symbols else (
            " 🟡" if sym in frozen_symbols and frozen_symbols[sym] > now else ""
        )
        top_text += f"  📈 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>{tag}: {pct:+.3f}% @ ${price:.4f}\n"
    for sym, pct, price in movers[-3:]:
        if pct >= 0:
            continue
        tag = " 🔴" if sym in banned_symbols else (
            " 🟡" if sym in frozen_symbols and frozen_symbols[sym] > now else ""
        )
        top_text += f"  📉 <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b>{tag}: {pct:+.3f}% @ ${price:.4f}\n"

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

    await message.answer(
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


# ---------------------------------------------------------------------------
# /symbols
# ---------------------------------------------------------------------------

@router.message(Command("symbols"))
async def cmd_symbols(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    if not symbols_discovered:
        await message.answer("⏳ Still discovering, try again shortly.")
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
    await message.answer(
        f"📋 <b>Monitored symbols ({len(MONITORED_SYMBOLS)}):</b>\n"
        + "\n".join(lines)
        + "\n\n🟢 Active  🟡 Frozen  🔴 Banned"
    )


# ---------------------------------------------------------------------------
# /ban /unban /freeze /unfreeze /blocked
# ---------------------------------------------------------------------------

@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    args = (message.text or "").split()[1:]
    if not args:
        await message.answer("Usage: <code>/ban SYMBOL</code>  e.g. <code>/ban TSLA</code>")
        return
    sym = find_symbol(args[0])
    if not sym:
        await message.answer(f"❌ <code>{args[0].upper()}</code> not found. See /symbols")
        return
    banned_symbols.add(sym)
    frozen_symbols.pop(sym, None)
    display = HARDCODED_SYMBOLS.get(sym, sym)
    await message.answer(
        f"🔴 <b>{display}</b> is <b>BANNED</b>.\n"
        f"Use <code>/unban {args[0].upper()}</code> to restore."
    )


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
    if not sym:
        await message.answer(f"❌ <code>{args[0].upper()}</code> not found.")
        return
    if sym not in banned_symbols:
        await message.answer(f"ℹ️ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is not banned.")
        return
    banned_symbols.discard(sym)
    await message.answer(f"✅ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is now <b>ACTIVE</b> again.")


@router.message(Command("freeze"))
async def cmd_freeze(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    args = (message.text or "").split()[1:]
    if len(args) < 2:
        await message.answer(
            "Usage: <code>/freeze SYMBOL MINUTES</code>\n"
            "Example: <code>/freeze TSLA 30</code>"
        )
        return
    sym = find_symbol(args[0])
    if not sym:
        await message.answer(f"❌ <code>{args[0].upper()}</code> not found.")
        return
    try:
        minutes = float(args[1])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Minutes must be a positive number.")
        return
    frozen_symbols[sym] = time.time() + minutes * 60
    h = int(minutes // 60)
    m = int(minutes % 60)
    duration_str = f"{h}h {m}m" if h else f"{m}m"
    display = HARDCODED_SYMBOLS.get(sym, sym)
    await message.answer(
        f"🟡 <b>{display}</b> frozen for <b>{duration_str}</b>.\n"
        f"Use <code>/unfreeze {args[0].upper()}</code> to restore early."
    )


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
    if not sym:
        await message.answer(f"❌ <code>{args[0].upper()}</code> not found.")
        return
    if sym not in frozen_symbols:
        await message.answer(f"ℹ️ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is not frozen.")
        return
    frozen_symbols.pop(sym)
    await message.answer(f"✅ <b>{HARDCODED_SYMBOLS.get(sym, sym)}</b> is now <b>ACTIVE</b> again.")


@router.message(Command("blocked"))
async def cmd_blocked(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    now = time.time()
    for s in [s for s, t in list(frozen_symbols.items()) if t <= now]:
        frozen_symbols.pop(s)

    banned_text = "".join(
        f"  🔴 <b>{HARDCODED_SYMBOLS.get(s, s)}</b>\n"
        for s in sorted(banned_symbols)
    )
    frozen_text = "".join(
        f"  🟡 <b>{HARDCODED_SYMBOLS.get(s, s)}</b> — {int((t-now)//60)}m {int((t-now)%60)}s\n"
        for s, t in sorted(frozen_symbols.items())
    )
    mute_text = f"\n🔇 <b>All alerts muted</b> — {mute_remaining()} remaining\n" if is_muted() else ""

    if not banned_text and not frozen_text and not mute_text:
        await message.answer("✅ No symbols are currently banned or frozen, bot is not muted.")
        return

    parts = ["🚫 <b>Blocked Status</b>\n"]
    if mute_text:
        parts.append(mute_text)
    if banned_text:
        parts.append(f"<b>Banned ({len(banned_symbols)}):</b>\n{banned_text}")
    if frozen_text:
        parts.append(f"<b>Frozen ({len(frozen_symbols)}):</b>\n{frozen_text}")
    await message.answer("\n".join(parts))


# ---------------------------------------------------------------------------
# /subscribers /kick /broadcast
# ---------------------------------------------------------------------------

@router.message(Command("subscribers"))
async def cmd_subscribers(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    if not subscribers:
        await message.answer("📭 No subscribers yet.")
        return
    lines = []
    for i, (cid, info) in enumerate(subscribers.items(), 1):
        joined = time.strftime("%d.%m.%Y", time.localtime(info.get("joined_at", 0)))
        lines.append(
            f"{i}. <b>{info['name']}</b> {info['username']}\n"
            f"   <code>{cid}</code> | {joined}"
        )
    chunks = [lines[i:i+30] for i in range(0, len(lines), 30)]
    for chunk in chunks:
        await message.answer(
            f"👥 <b>Subscribers ({len(subscribers)}):</b>\n\n" + "\n\n".join(chunk)
        )


@router.message(Command("kick"))
async def cmd_kick(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    args = (message.text or "").split()[1:]
    if not args:
        await message.answer("Usage: <code>/kick CHAT_ID</code>")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await message.answer("❌ Invalid ID.")
        return
    if target_id not in subscribers:
        await message.answer(f"ℹ️ <code>{target_id}</code> is not subscribed.")
        return
    name = subscribers[target_id]["name"]
    subscribers.pop(target_id)
    save_subscribers()
    await message.answer(f"✅ Removed <b>{name}</b> (<code>{target_id}</code>).")
    try:
        await message.bot.send_message(
            chat_id=target_id,
            text="ℹ️ You have been removed from MEXC Surge Monitor by the admin."
        )
    except Exception:
        pass


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: <code>/broadcast Your message here</code>")
        return
    msg = parts[1]
    await message.answer(f"📣 Sending to <b>{len(subscribers)}</b> subscribers...")
    sent = 0
    dead = []
    for chat_id in list(subscribers.keys()):
        try:
            await message.bot.send_message(
                chat_id=chat_id,
                text=f"📣 <b>Message from admin:</b>\n\n{msg}",
            )
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as exc:
            err = str(exc).lower()
            if "blocked" in err or "not found" in err or "deactivated" in err:
                dead.append(chat_id)
    for cid in dead:
        subscribers.pop(cid, None)
    if dead:
        save_subscribers()
    await message.answer(
        f"✅ Done — Sent: <b>{sent}</b>, Removed dead: <b>{len(dead)}</b>"
    )


# ---------------------------------------------------------------------------
# /debug
# ---------------------------------------------------------------------------

@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    await message.answer("🔍 Hitting API, please wait...")
    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(
                MEXC_TICKER_URL, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=30), ssl=True,
            ) as response:
                if response.status != 200:
                    await message.answer(f"❌ HTTP {response.status}")
                    return
                payload = await response.json(content_type=None)
                data: list[dict] = payload.get("data", [])
                if not data:
                    await message.answer("⚠️ Connected but data empty!")
                    return
                found, not_found = discover_symbols(data)
                matched_text = "\n".join(
                    f"  ✅ <b>{base}</b> → <code>{sym}</code>"
                    for sym, base in sorted(found.items(), key=lambda x: x[1])
                )
                await message.answer(
                    f"✅ <b>API OK</b> — {len(data)} tickers\n"
                    f"Matched: <b>{len(found)}/{len(HARDCODED_SYMBOLS)}</b>\n\n"
                    f"{matched_text}"
                )
                if not_found:
                    await message.answer(
                        f"❌ Not in API: <code>{', '.join(sorted(not_found))}</code>"
                    )
        except asyncio.TimeoutError:
            await message.answer("⏱ Timed out.")
        except Exception as exc:
            await message.answer(f"💥 <code>{exc}</code>")


# ---------------------------------------------------------------------------
# /test
# ---------------------------------------------------------------------------

@router.message(Command("test"))
async def cmd_test(message: Message, bot: Bot) -> None:
    global test_mode, test_chat_id
    if not is_admin(message):
        await deny(message)
        return
    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return
    await message.answer(
        "🧪 <b>Test mode activated!</b>\n"
        "Scanning for first symbol with ≥ <b>0.1%</b> move.\n"
        f"One alert fires then returns to <b>±{surge_threshold}%</b>."
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
        if abs(pct) >= 0.1 and abs(pct) > abs(best_pct):
            best_pct = pct
            best_symbol = symbol
            best_price = latest_price
    if best_symbol:
        await send_surge_alert(
            bot, best_symbol, best_pct, best_price,
            chat_id=message.chat.id, is_test=True,
        )
    else:
        test_mode = True
        test_chat_id = message.chat.id
        await message.answer(
            "⏳ No symbol at 0.1%+ yet — watching in background.\n"
            "Alert fires the moment any symbol crosses 0.1%."
        )


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


def parse_price(ticker: dict) -> Optional[float]:
    raw = (
        ticker.get("lastPrice") or ticker.get("last_price")
        or ticker.get("last") or ticker.get("price") or ticker.get("close")
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
    return (latest_price / oldest_price - 1) * 100.0, latest_price


# ---------------------------------------------------------------------------
# Monitoring loop
# ---------------------------------------------------------------------------


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
                        price_windows[sym] = deque()
                    symbols_discovered = True
                    logger.info(
                        "Ready: monitoring %d symbols (%d not on MEXC)",
                        len(MONITORED_SYMBOLS), len(not_found),
                    )

                # Clean expired freezes
                for s in [s for s, t in list(frozen_symbols.items()) if t <= now]:
                    frozen_symbols.pop(s)
                    logger.info("Freeze expired: %s", s)

                ticker_map = {t.get("symbol", ""): t for t in raw_data}

                for sym in MONITORED_SYMBOLS:
                    if sym in banned_symbols:
                        continue
                    if sym in frozen_symbols:
                        continue

                    ticker = ticker_map.get(sym)
                    if not ticker:
                        continue
                    price = parse_price(ticker)
                    if price is None:
                        continue

                    update_window(sym, now, price)
                    result = get_pct_change(sym)
                    if result is None:
                        continue

                    pct_change, current_price = result

                    # Test mode — fire on any 0.1% move then disable
                    if test_mode and abs(pct_change) >= 0.1:
                        test_mode = False
                        asyncio.create_task(
                            send_surge_alert(
                                bot, sym, pct_change, current_price,
                                chat_id=test_chat_id, is_test=True,
                            )
                        )
                        continue

                    # Skip if globally muted
                    if is_muted():
                        continue

                    # Surge alert — price up
                    if pct_change >= surge_threshold:
                        last_sent = last_surge_alert.get(sym, 0.0)
                        if now - last_sent >= COOLDOWN_SECONDS:
                            last_surge_alert[sym] = now
                            asyncio.create_task(
                                send_surge_alert(bot, sym, pct_change, current_price)
                            )

                    # Crash alert — price down
                    elif pct_change <= -surge_threshold:
                        last_sent = last_crash_alert.get(sym, 0.0)
                        if now - last_sent >= COOLDOWN_SECONDS:
                            last_crash_alert[sym] = now
                            asyncio.create_task(
                                send_surge_alert(bot, sym, pct_change, current_price)
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
        "Bot @%s started — threshold=±%.1f%% channel=%s subscribers=%d",
        me.username, surge_threshold, CHANNEL_ID or "NOT SET", len(subscribers),
    )

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
