import os
import json
import logging
import asyncio
import time
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import feedparser

# ================================================================
# КОНФИГУРАЦИЯ
# ================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN    = os.getenv("BOT_TOKEN")
ADMIN_ID     = int(os.getenv("ADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

# ================================================================
# ХРАНИЛИЩЕ (только подписчики, без Premium)
# ================================================================

if DATABASE_URL:
    import psycopg2

    def _conn():
        return psycopg2.connect(DATABASE_URL, sslmode="require")

    def init_storage():
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS subscribers (chat_id BIGINT PRIMARY KEY);"
                )
            c.commit()
        logger.info("PostgreSQL инициализирован")

    def get_subscribers() -> set:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT chat_id FROM subscribers")
                return {r[0] for r in cur.fetchall()}

    def add_subscriber(chat_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "INSERT INTO subscribers (chat_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (chat_id,)
                )
            c.commit()

    def remove_subscriber(chat_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM subscribers WHERE chat_id = %s", (chat_id,))
            c.commit()

    def count_subscribers() -> int:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM subscribers")
                return cur.fetchone()[0]

else:
    SUBS_FILE = "subscribers.json"

    def init_storage(): pass

    def _load_s(): return json.load(open(SUBS_FILE)) if os.path.exists(SUBS_FILE) else []
    def _save_s(d):
        with open(SUBS_FILE, "w") as f: json.dump(d, f)

    def get_subscribers() -> set: return set(_load_s())
    def add_subscriber(cid: int): s = get_subscribers(); s.add(cid); _save_s(list(s))
    def remove_subscriber(cid: int): s = get_subscribers(); s.discard(cid); _save_s(list(s))
    def count_subscribers() -> int: return len(get_subscribers())

# ================================================================
# АНАЛИЗ НОВОСТЕЙ (RSS + ключевые слова)
# ================================================================

_news_cache: dict = {}
NEWS_TTL = 1800  # кэш 30 минут

BTC_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]
GOLD_FEEDS = [
    "https://www.kitco.com/rss/",
    "https://feeds.reuters.com/reuters/businessNews",
]

BULLISH_WORDS = [
    "bull", "surge", "rally", "gain", "rise", "soar", "jump", "high",
    "breakout", "buy", "long", "positive", "adoption", "approval",
    "etf", "growth", "support", "recovery", "rebound", "record",
    "boost", "strong", "milestone", "inflow"
]
BEARISH_WORDS = [
    "bear", "crash", "drop", "fall", "plunge", "dump", "sell",
    "short", "negative", "ban", "hack", "fear", "panic",
    "warning", "risk", "concern", "weak", "loss", "decline",
    "collapse", "trouble", "restrict", "regulation", "outflow"
]

def _fetch_news(symbol: str) -> tuple[int, str]:
    """Возвращает (score, текст): +1 бычьи, 0 нейтральные, -1 медвежьи."""
    now = time.time()
    if symbol in _news_cache:
        ts, score, text = _news_cache[symbol]
        if now - ts < NEWS_TTL:
            return score, text

    feeds = BTC_FEEDS if "BTC" in symbol else GOLD_FEEDS
    bull, bear = 0, 0

    for url in feeds:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                t = entry.get("title", "").lower()
                bull += sum(1 for w in BULLISH_WORDS if w in t)
                bear += sum(1 for w in BEARISH_WORDS if w in t)
        except Exception as e:
            logger.warning(f"RSS ошибка {url}: {e}")

    if bull > bear:
        score, text = 1, f"Позитивный (🟢{bull} бычьих / 🔴{bear} медвежьих)"
    elif bear > bull:
        score, text = -1, f"Негативный (🔴{bear} медвежьих / 🟢{bull} бычьих)"
    else:
        score, text = 0, "Нейтральный"

    _news_cache[symbol] = (now, score, text)
    logger.info(f"Новости {symbol}: {text}")
    return score, text

# ================================================================
# ТЕХНИЧЕСКИЙ АНАЛИЗ
# ================================================================

def _get_bbands(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    bb = ta.bbands(series, length=20, std=2)
    if bb is None or bb.empty:
        return series * 1.02, series * 0.98
    u = [c for c in bb.columns if c.startswith("BBU")][0]
    l = [c for c in bb.columns if c.startswith("BBL")][0]
    return bb[u], bb[l]

def _score_technical(rsi, macd, macd_s, price, bbu, bbl, trend_bull, vol_ratio):
    bs, ss, br, sr = 0, 0, [], []

    if rsi < 30:   bs += 1; br.append(f"RSI перепродан ({rsi})")
    elif rsi > 70: ss += 1; sr.append(f"RSI перекуплен ({rsi})")

    if macd > macd_s: bs += 1; br.append("MACD бычий импульс")
    else:             ss += 1; sr.append("MACD медвежий импульс")

    if price <= bbl * 1.005:   bs += 1; br.append("Цена у нижней BB (зона покупки)")
    elif price >= bbu * 0.995: ss += 1; sr.append("Цена у верхней BB (зона продажи)")

    if trend_bull: bs += 1; br.append("4H тренд восходящий (EMA20 > EMA50)")
    else:          ss += 1; sr.append("4H тренд нисходящий (EMA20 < EMA50)")

    if vol_ratio >= 1.5:
        if bs >= ss: bs += 1; br.append(f"Объём подтверждает рост (×{round(vol_ratio,1)})")
        else:        ss += 1; sr.append(f"Объём подтверждает падение (×{round(vol_ratio,1)})")

    return bs, ss, br, sr

def _analyze(ticker_symbol: str, display_symbol: str, tp: float, sl: float) -> dict:
    """
    Полный анализ: 5 технических индикаторов + новости.
    Запускается в отдельном потоке (asyncio.to_thread).
    """
    # --- Технический анализ ---
    df = yf.Ticker(ticker_symbol).history(period="7d", interval="1h")
    if df.empty:
        raise ValueError(f"Нет данных: {display_symbol}")

    df.columns = [c.lower() for c in df.columns]
    df["e20"]  = ta.ema(df["close"], 20)
    df["e50"]  = ta.ema(df["close"], 50)
    trend      = bool(df.iloc[-1]["e20"] > df.iloc[-1]["e50"])
    df["rsi"]  = ta.rsi(df["close"], 14)
    m          = ta.macd(df["close"])
    df["macd"] = m["MACD_12_26_9"]
    df["ms"]   = m["MACDs_12_26_9"]
    df["bbu"], df["bbl"] = _get_bbands(df["close"])
    df["vm"]   = ta.sma(df["volume"], 20)

    last = df.iloc[-1]
    p    = float(last["close"])
    rsi  = round(float(last["rsi"]), 2)
    vm   = float(last["vm"]) or 1.0
    vr   = float(last["volume"]) / vm

    bs, ss, br, sr = _score_technical(
        rsi, float(last["macd"]), float(last["ms"]),
        p, float(last["bbu"]), float(last["bbl"]), trend, vr
    )

    # --- Новости ---
    news_score, news_text = _fetch_news(display_symbol)

    def _strength(sc: int) -> str:
        if sc == 5: return "🔥 СИЛЬНЫЙ"
        if sc >= 4: return "✅ ХОРОШИЙ"
        return "—"

    # --- Сборка результата ---
    if bs >= 4:
        # Новости подтверждают LONG → добавляем в подтверждения
        if news_score > 0:
            br.append(f"📰 Новостной фон: {news_text}")
            news_warning = None
        else:
            news_warning = f"📰 Новостной фон: {news_text}"
        return {
            "symbol": display_symbol,
            "direction": "🟢 ПОКУПАЙ (LONG)",
            "strength": _strength(bs),
            "score": f"{bs}/5",
            "entry": round(p, 2),
            "take_profit": round(p * (1 + tp), 2),
            "stop_loss": round(p * (1 - sl), 2),
            "rsi": rsi,
            "reasons": br,
            "news_warning": news_warning,
        }

    elif ss >= 4:
        # Новости подтверждают SHORT → добавляем в подтверждения
        if news_score < 0:
            sr.append(f"📰 Новостной фон: {news_text}")
            news_warning = None
        else:
            news_warning = f"📰 Новостной фон: {news_text}"
        return {
            "symbol": display_symbol,
            "direction": "🔴 ПРОДАВАЙ (SHORT)",
            "strength": _strength(ss),
            "score": f"{ss}/5",
            "entry": round(p, 2),
            "take_profit": round(p * (1 - tp), 2),
            "stop_loss": round(p * (1 + sl), 2),
            "rsi": rsi,
            "reasons": sr,
            "news_warning": news_warning,
        }

    else:
        return {
            "symbol": display_symbol,
            "direction": "⚪️ ЖДАТЬ",
            "strength": "—",
            "score": f"{max(bs, ss)}/5",
            "entry": round(p, 2),
            "take_profit": None,
            "stop_loss": None,
            "rsi": rsi,
            "reasons": ["Недостаточно подтверждений — ждём лучшей точки входа"],
            "news_warning": f"📰 Новостной фон: {news_text}",
        }

def _btc():  return _analyze("BTC-USD", "BTC/USD", 0.04,  0.02)
def _gold(): return _analyze("GC=F",    "XAUUSD",  0.015, 0.0075)

# ================================================================
# ФОРМАТИРОВАНИЕ
# ================================================================

def fmt(data: dict, is_auto: bool = False) -> str:
    sym    = data["symbol"]
    header = f"🔔 *Автосигнал {sym}*\n\n" if is_auto else f"📊 *Сигнал {sym}*\n\n"

    if data["take_profit"]:
        reasons   = "\n".join(f"  ✅ {r}" for r in data["reasons"])
        news_line = f"\n⚠️ *Осторожно:* {data['news_warning']}" if data.get("news_warning") else ""
        return (
            f"{header}"
            f"Направление: {data['direction']}\n"
            f"Сила: {data['strength']} `({data['score']})`\n\n"
            f"📍 Точка входа: `${data['entry']}`\n"
            f"🎯 Тейк-профит: `${data['take_profit']}`\n"
            f"🛡 Стоп-лосс: `${data['stop_loss']}`\n\n"
            f"📋 *Подтверждения:*\n{reasons}"
            f"{news_line}\n\n"
            f"📈 RSI: `{data['rsi']}`\n\n"
            f"⚠️ Не является финансовым советом"
        )

    news_line = f"{data.get('news_warning', '')}\n\n" if data.get("news_warning") else ""
    return (
        f"{header}"
        f"Направление: {data['direction']}\n"
        f"Подтверждений: `{data['score']}` — нужно минимум 4/5\n\n"
        f"💵 Цена: `${data['entry']}`\n"
        f"📈 RSI: `{data['rsi']}`\n\n"
        f"{news_line}"
        f"⏳ Ждём чёткого сигнала...\n\n"
        f"⚠️ Не является финансовым советом"
    )

# ================================================================
# КОМАНДЫ БОТА
# ================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"👋 Привет, {u.first_name}!\n\n"
        f"🤖 *AlphaX Trade* — твой ИИ-помощник для трейдинга.\n\n"
        f"Анализирую рынок по *6 факторам:*\n"
        f"📊 5 технических индикаторов\n"
        f"📰 Анализ мировых новостей (CoinTelegraph, Reuters)\n\n"
        f"Сигнал только при *4/5 подтверждениях*.\n\n"
        f"📌 Команды:\n"
        f"/signal — получить сигнал прямо сейчас\n"
        f"/subscribe — автосигналы каждые 4 часа\n"
        f"/unsubscribe — отключить автосигналы\n"
        f"/help — как работает бот",
        parse_mode="Markdown"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Как работает AlphaX Trade:*\n\n"
        "*5 технических индикаторов:*\n"
        "1️⃣ RSI — перекупленность/перепроданность\n"
        "2️⃣ MACD — направление импульса\n"
        "3️⃣ Bollinger Bands — цена на краю канала\n"
        "4️⃣ Тренд 4H — глобальное направление рынка\n"
        "5️⃣ Объём — деньги подтверждают движение\n\n"
        "*📰 Анализ новостей:*\n"
        "Читаю CoinTelegraph, CoinDesk, Reuters.\n"
        "Если новости совпадают с сигналом — усиливают его.\n"
        "Если противоречат — предупреждаю тебя.\n\n"
        "✅ *4/5* = Хороший сигнал\n"
        "🔥 *5/5* = Сильный сигнал\n"
        "⚪️ *3/5 и меньше* = Ждём\n\n"
        "/signal — сигнал сейчас\n"
        "/subscribe — автосигналы каждые 4ч\n"
        "/unsubscribe — отключить\n\n"
        "⚠️ Не является финансовым советом.",
        parse_mode="Markdown"
    )

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[
        InlineKeyboardButton("₿ BTC/USD", callback_data="sig_btc"),
        InlineKeyboardButton("🥇 XAUUSD",  callback_data="sig_gold"),
    ]]
    await update.message.reply_text("Выбери актив:", reply_markup=InlineKeyboardMarkup(kb))

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    if cid in get_subscribers():
        await update.message.reply_text("✅ Ты уже подписан на автосигналы!")
    else:
        add_subscriber(cid)
        await update.message.reply_text(
            "✅ *Автосигналы активированы!*\n\n"
            "Каждые 4 часа анализирую BTC и Золото.\n"
            "Слабые сигналы пропускаю — только 4/5 и выше.\n"
            "Новости проверяю автоматически.\n\n"
            "Отключить: /unsubscribe",
            parse_mode="Markdown"
        )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    if cid in get_subscribers():
        remove_subscriber(cid)
        await update.message.reply_text("❌ Автосигналы отключены.\nВернуться: /subscribe")
    else:
        await update.message.reply_text("Ты не был подписан.")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("🔍 Анализирую рынок + читаю новости, 10-15 сек...")
    try:
        fn   = _btc if q.data == "sig_btc" else _gold
        data = await asyncio.to_thread(fn)
        await q.edit_message_text(fmt(data), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка сигнала: {e}")
        await q.edit_message_text(f"❌ Ошибка: {e}")

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    n = count_subscribers()
    await update.message.reply_text(
        f"📊 *Статистика AlphaX Trade*\n\n"
        f"👥 Подписчиков на автосигналы: *{n}*",
        parse_mode="Markdown"
    )

# ================================================================
# АВТОСИГНАЛЫ
# ================================================================

async def send_auto_signals(context: ContextTypes.DEFAULT_TYPE):
    subs = get_subscribers()
    if not subs:
        logger.info("Нет подписчиков — пропускаем")
        return

    logger.info(f"Автосигналы: {len(subs)} подписчиков")
    to_send = []

    for name, fn in [("BTC", _btc), ("Gold", _gold)]:
        try:
            data = await asyncio.to_thread(fn)
            if data["direction"] != "⚪️ ЖДАТЬ":
                to_send.append(data)
                logger.info(f"{name}: {data['direction']} ({data['score']})")
            else:
                logger.info(f"{name}: ЖДАТЬ — пропущен")
        except Exception as e:
            logger.error(f"Ошибка {name}: {e}")

    if not to_send:
        logger.info("Нет сигналов — рассылка пропущена")
        return

    dead = set()
    for cid in subs.copy():
        for d in to_send:
            try:
                await context.bot.send_message(cid, fmt(d, is_auto=True), parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Ошибка отправки {cid}: {e}")
                dead.add(cid)
    for cid in dead:
        remove_subscriber(cid)

# ================================================================
# ЗАПУСК
# ================================================================

def main():
    init_storage()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("signal",      signal_command))
    app.add_handler(CommandHandler("subscribe",   subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("help",        help_command))
    app.add_handler(CommandHandler("users",       users_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.job_queue.run_repeating(send_auto_signals, interval=14400, first=10)

    mode = "PostgreSQL ☁️" if DATABASE_URL else "JSON (локально)"
    print(f"✅ AlphaX Trade запущен! Хранилище: {mode}")
    logger.info(f"Бот запущен. Хранилище: {mode}")
    app.run_polling()

if __name__ == "__main__":
    main()