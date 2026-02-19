"""
MEXC Futures Price Surge Monitor Bot
Version: 7.2.0 — Fully hardcoded symbol map
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
SURGE_THRESHOLD: float = float(os.getenv("SURGE_THRESHOLD", "1.0"))
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "60"))
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "60"))

ADMIN_ID = 868931721
CHANNEL_ID: str = os.getenv("CHANNEL_ID", "")

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
# Fully hardcoded symbol map — verified from API logs
# Format: "EXACT_MEXC_SYMBOL": "DISPLAY_NAME"
# ---------------------------------------------------------------------------

HARDCODED_SYMBOLS: dict[str, str] = {
    # Indices — standard format
    "NAS100_USDT":      "NAS100",
    "HK50_USDT":        "HK50",
    "US30_USDT":        "US30",

    # Non-standard names — manually confirmed
    "COINBASE_USDT":    "COIN",
    "FIGSTOCK_USDT":    "FIG",
    "ROBINHOOD_USDT":   "HOOD",

    # STOCK suffix — confirmed from logs
    "CVNASTOCK_USDT":   "CVNA",
    "AMATSTOCK_USDT":   "AMAT",
    "GOOGLSTOCK_USDT":  "GOOGL",
    "QCOMSTOCK_USDT":   "QCOM",
    "CRMSTOCK_USDT":    "CRM",
    "SHOPSTOCK_USDT":   "SHOP",
    "MSFTSTOCK_USDT":   "MSFT",
    "VZSTOCK_USDT":     "VZ",
    "INTCSTOCK_USDT":   "INTC",
    "QQQSTOCK_USDT":    "QQQ",
    "CSCOSTOCK_USDT":   "CSCO",
    "JNJSTOCK_USDT":    "JNJ",
    "AMZNSTOCK_USDT":   "AMZN",
    "FUTUSTOCK_USDT":   "FUTU",
    "AAPLSTOCK_USDT":   "AAPL",
    "AMDSTOCK_USDT":    "AMD",
    "XOMSTOCK_USDT":    "XOM",
    "METASTOCK_USDT":   "META",
    "RDDTSTOCK_USDT":   "RDDT",
    "SPOTSTOCK_USDT":   "SPOT",
    "NFLXSTOCK_USDT":   "NFLX",
    "SMCISTOCK_USDT":   "SMCI",
    "ORCLSTOCK_USDT":   "ORCL",
    "ASMLSTOCK_USDT":   "ASML",
    "ACNSTOCK_USDT":    "ACN",
    "UNHSTOCK_USDT":    "UNH",
    "NOWSTOCK_USDT":    "NOW",
    "LLYSTOCK_USDT":    "LLY",
    "LRCXSTOCK_USDT":   "LRCX",
    "IBMSTOCK_USDT":    "IBM",
    "COSTSTOCK_USDT":   "COST",
    "JDSTOCK_USDT":     "JD",
    "JPMSTOCK_USDT":    "JPM",
    "GSSTOCK_USDT":     "GS",
    "MASTOCK_USDT":     "MA",
    "KOSTOCK_USDT":     "KO",
    "WMTSTOCK_USDT":    "WMT",
    "GESTOCK_USDT":     "GE",
    "MUSTOCK_USDT":     "MU",
    "VSTOCK_USDT":      "V",
    "NKESTOCK_USDT":    "NKE",
    "PEPSTOCK_USDT":    "PEP",
    "BASTOCK_USDT":     "BA",

    # Unconfirmed — will show warning if wrong, just won't monitor
    "TSLASTOCK_USDT":   "TSLA",
    "NVDASTOCK_USDT":   "NVDA",
    "SP500_USDT":       "SP500",
}

# ---------------------------------------------------------------------------
# Persistent subscriber storage
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
last_alert_time: dict[str, float] = {}
symbols_discovered: bool = False
test_mode: bool = False
test_chat_id: Optional[int] = None
banned_symbols: set[str] = set()
frozen_symbols: dict[str, float] = {}

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


# ---------------------------------------------------------------------------
# Symbol discovery — verify hardcoded symbols exist in API
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
            logger.warning("NOT IN API:  %-10s (%s)", base, mexc_sym)

    return found, not_found


# ---------------------------------------------------------------------------
# Broadcast
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
# Router
# ---------------------------------------------------------------------------

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user = message.from_user
    chat_id = message.chat.id
    already = chat_id in subscribers

    subscribers[chat_id] = {
        "name": user.full_name if user else "Unknown",
        "username": f"@{user.username}" if user and user.username else "no username",
        "joined_at": subscribers.get(chat_id, {}).get("joined_at", time.time()),
    }
    save_subscribers()

    if not already:
        logger.info("New subscriber: %s id=%d", subscribers[chat_id]["name"], chat_id)
        try:
            await message.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🆕 <b>New subscriber!</b>\n\n"
                    f"👤 {subscribers[chat_id]['name']}\n"
                    f"🔗 {subscribers[chat_id]['username']}\n"
                    f"🆔 <code>{chat_id}</code>\n"
                    f"👥 Total: <b>{len(subscribers)}</b>"
                ),
            )
        except Exception:
            pass

    status = (
        f"📡 Watching <b>{len(MONITORED_SYMBOLS)}</b> symbols"
        if symbols_discovered else "📡 Starting up..."
    )
    verb = "You're already subscribed" if already else "You're now subscribed"

    await message.answer(
        f"👋 <b>Welcome to MEXC Surge Monitor!</b>\n\n"
        f"✅ {verb} to price surge alerts.\n\n"
        f"{status}\n"
        f"🚨 Alerts fire at <b>+{SURGE_THRESHOLD}%</b> within <b>{WINDOW_SECONDS}s</b>\n\n"
        "<b>Commands:</b>\n"
        "/status — live stats + top movers\n"
        "/stop — unsubscribe from alerts\n"
        "/test — test alert at 0.1% threshold\n"
    )


@router.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    chat_id = message.chat.id
    if chat_id not in subscribers:
        await message.answer("ℹ️ You are not subscribed. Send /start to subscribe.")
        return
    name = subscribers[chat_id]["name"]
    subscribers.pop(chat_id)
    save_subscribers()
    await message.answer(
        "✅ <b>You have been unsubscribed.</b>\n"
        "Send /start anytime to subscribe again."
    )
    try:
        await message.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"👋 <b>Subscriber left</b>\n"
                f"👤 {name} (<code>{chat_id}</code>)\n"
                f"👥 Remaining: <b>{len(subscribers)}</b>"
            ),
        )
    except Exception:
        pass


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
        tag = " 🔴" if sym in banned_symbols else (
            " 🟡" if sym in frozen_symbols and frozen_symbols[sym] > now else ""
        )
        top_text += f"  {arrow} <b>{sym}</b>{tag}: {pct:+.3f}% @ ${price:.4f}\n"
    if not top_text:
        top_text = "  ⏳ Filling window, wait 10s...\n"

    admin_extra = (
        f"\n👥 Subscribers: <b>{len(subscribers)}</b>\n"
        f"🔴 Banned: <b>{len(banned_symbols)}</b>\n"
        f"🟡 Frozen: <b>{len(frozen_symbols)}</b>"
        if is_admin(message) else ""
    )

    await message.answer(
        f"📊 <b>Monitor Status</b>\n\n"
        f"🔍 Symbols watched: <b>{len(MONITORED_SYMBOLS)}</b>\n"
        f"📈 Windows with data: <b>{windows_with_data}/{len(MONITORED_SYMBOLS)}</b>\n"
        f"🔕 On cooldown: <b>{on_cooldown}</b>\n"
        f"⚡ Threshold: <b>{SURGE_THRESHOLD}%</b>\n"
        f"⏱ Window: <b>{WINDOW_SECONDS}s</b>\n"
        f"🔄 Fetch interval: <b>{FETCH_INTERVAL}s</b>"
        f"{admin_extra}\n\n"
        f"🏆 <b>Top 5 movers right now:</b>\n{top_text}"
    )


@router.message(Command("symbols"))
async def cmd_symbols(message: Message) -> None:
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
    await message.answer(
        f"🔴 <b>{sym}</b> is <b>BANNED</b>.\n"
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
        await message.answer(f"ℹ️ <b>{sym}</b> is not banned.")
        return
    banned_symbols.discard(sym)
    await message.answer(f"✅ <b>{sym}</b> is now <b>ACTIVE</b> again.")


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
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    duration_str = f"{hours}h {mins}m" if hours else f"{mins}m"
    await message.answer(
        f"🟡 <b>{sym}</b> frozen for <b>{duration_str}</b>.\n"
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
        await message.answer(f"ℹ️ <b>{sym}</b> is not frozen.")
        return
    frozen_symbols.pop(sym)
    await message.answer(f"✅ <b>{sym}</b> is now <b>ACTIVE</b> again.")


@router.message(Command("blocked"))
async def cmd_blocked(message: Message) -> None:
    if not is_admin(message):
        await deny(message)
        return
    now = time.time()
    for s in [s for s, t in frozen_symbols.items() if t <= now]:
        frozen_symbols.pop(s)
    banned_text = "".join(f"  🔴 <b>{s}</b>\n" for s in sorted(banned_symbols))
    frozen_text = "".join(
        f"  🟡 <b>{s}</b> — {int((t-now)//60)}m {int((t-now)%60)}s\n"
        for s, t in sorted(frozen_symbols.items())
    )
    if not banned_text and not frozen_text:
        await message.answer("✅ No symbols are currently banned or frozen.")
        return
    parts = ["🚫 <b>Blocked Symbols</b>\n"]
    if banned_text:
        parts.append(f"<b>Banned ({len(banned_symbols)}):</b>\n{banned_text}")
    if frozen_text:
        parts.append(f"<b>Frozen ({len(frozen_symbols)}):</b>\n{frozen_text}")
    await message.answer("\n".join(parts))


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
                        f"❌ <b>Not in API ({len(not_found)}):</b>\n"
                        f"<code>{', '.join(sorted(not_found))}</code>\n\n"
                        f"These symbols don't exist on MEXC futures."
                    )
        except asyncio.TimeoutError:
            await message.answer("⏱ Timed out. MEXC may be geo-blocking your IP.")
        except Exception as exc:
            await message.answer(f"💥 <code>{exc}</code>")


@router.message(Command("test"))
async def cmd_test(message: Message, bot: Bot) -> None:
    global test_mode, test_chat_id
    if not symbols_discovered:
        await message.answer("⏳ Still discovering symbols, try again shortly.")
        return
    await message.answer(
        "🧪 <b>Test mode activated!</b>\n"
        f"Scanning for first symbol ≥ <b>0.1%</b>.\n"
        f"One alert fires then returns to <b>{SURGE_THRESHOLD}%</b>."
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
                f"🧪 <b>[TEST — 0.1% threshold]</b>\n\n"
                f"🚨 <b>FUTURES SURGE: #{best_symbol.replace('_', '')}</b>\n"
                f"📈 Change: <b>+{best_pct:.2f}%</b> in 60s\n"
                f"💵 Current Price: <b>${best_price:.4f}</b>\n\n"
                f"✅ Bot working! Back to <b>{SURGE_THRESHOLD}%</b>."
            ),
            reply_markup=build_trade_keyboard(best_symbol),
        )
    else:
        test_mode = True
        test_chat_id = message.chat.id
        await message.answer(
            "⏳ No symbol at 0.1%+ yet — watching in background.\n"
            "Alert fires the moment any symbol crosses 0.1%."
        )


# ---------------------------------------------------------------------------
# Alert builders
# ---------------------------------------------------------------------------


def build_alert_message(
    symbol: str, pct_change: float, current_price: float, is_test: bool = False
) -> str:
    display = HARDCODED_SYMBOLS.get(symbol, symbol.replace("_", ""))
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


async def send_surge_alert(
    bot: Bot, symbol: str, pct_change: float, current_price: float,
    chat_id: Optional[int] = None, is_test: bool = False,
) -> None:
    text = build_alert_message(symbol, pct_change, current_price, is_test)
    keyboard = build_trade_keyboard(symbol)
    if chat_id:
        try:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
        except Exception as exc:
            logger.error("Failed to send test alert: %s", exc)
    else:
        await broadcast(bot, text, keyboard)


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
                        "Ready: monitoring %d symbols (%d not on MEXC)",
                        len(MONITORED_SYMBOLS), len(not_found),
                    )

                for s in [s for s, t in frozen_symbols.items() if t <= now]:
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

                    if test_mode and pct_change >= 0.1:
                        test_mode = False
                        asyncio.create_task(
                            send_surge_alert(
                                bot, sym, pct_change, current_price,
                                chat_id=test_chat_id, is_test=True,
                            )
                        )
                        continue

                    if pct_change >= SURGE_THRESHOLD:
                        last_sent = last_alert_time.get(sym, 0.0)
                        if now - last_sent >= COOLDOWN_SECONDS:
                            last_alert_time[sym] = now
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
    logger.info("Bot @%s started — %d subscribers loaded", me.username, len(subscribers))

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
