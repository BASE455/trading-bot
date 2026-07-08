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

NEWS_WEIGHT = 2   # новости весят как 2 обычных индикатора
MAX_SCORE   = 7   # 5 (техника) + 2 (новости)
MIN_SCORE   = 5   # минимум для сигнала

# ================================================================
# АКТИВЫ
# ================================================================

ASSETS = {
    "btc":    {"ticker": "BTC-USD",  "name": "BTC/USD", "emoji": "₿",  "tp": 0.040,  "sl": 0.020,  "class": "crypto"},
    "gold":   {"ticker": "GC=F",     "name": "XAUUSD",  "emoji": "🥇", "tp": 0.015,  "sl": 0.0075, "class": "gold"},
    "eur":    {"ticker": "EURUSD=X", "name": "EUR/USD", "emoji": "💶", "tp": 0.008,  "sl": 0.004,  "class": "forex"},
    "nasdaq": {"ticker": "^IXIC",    "name": "NASDAQ",  "emoji": "📈", "tp": 0.020,  "sl": 0.010,  "class": "stock"},
    "aapl":   {"ticker": "AAPL",     "name": "AAPL",    "emoji": "🍎", "tp": 0.025,  "sl": 0.0125, "class": "stock"},
    "tsla":   {"ticker": "TSLA",     "name": "TSLA",    "emoji": "🚗", "tp": 0.035,  "sl": 0.0175, "class": "stock"},
}

PERIOD_BY_CLASS = {
    "crypto": "7d",
    "gold":   "7d",
    "forex":  "7d",
    "stock":  "30d",
}

# ================================================================
# ХРАНИЛИЩЕ
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
NEWS_TTL = 1800  # кэш на 30 минут

FEEDS_BY_CLASS = {
    "crypto": ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"],
    "gold":   ["https://www.kitco.com/rss/", "https://feeds.reuters.com/reuters/businessNews"],
    "forex":  ["https://feeds.reuters.com/reuters/businessNews", "http://feeds.marketwatch.com/marketwatch/topstories/"],
    "stock":  ["http://feeds.marketwatch.com/marketwatch/topstories/", "https://feeds.reuters.com/reuters/businessNews"],
}

BULLISH_WORDS = [
    "bull", "surge", "rally", "gain", "rise", "soar", "jump", "high",
    "breakout", "buy", "long", "positive", "adoption", "approval",
    "etf", "growth", "support", "recovery", "rebound", "record",
    "boost", "strong", "milestone", "inflow", "beat", "upgrade",
    "outperform", "buyback",
]
BEARISH_WORDS = [
    "bear", "crash", "drop", "fall", "plunge", "dump", "sell",
    "short", "negative", "ban", "hack", "fear", "panic",
    "warning", "risk", "concern", "weak", "loss", "decline",
    "collapse", "trouble", "restrict", "regulation", "outflow",
    "miss", "downgrade", "underperform", "layoffs", "recall",
]

def _fetch_news(symbol: str, asset_class: str) -> tuple[int, str]:
    """
    Возвращает (score, текст).
    score: +1 бычьи слова перевешивают, -1 медвежьи перевешивают, 0 нейтрально.
    Числа в тексте — это подсчёт КЛЮЧЕВЫХ СЛОВ в заголовках, а не готовые
    подтверждения из шкалы 0-7 (не путать одно с другим).
    """
    now = time.time()
    if symbol in _news_cache:
        ts, score, text = _news_cache[symbol]
        if now - ts < NEWS_TTL:
            return score, text

    feeds = FEEDS_BY_CLASS.get(asset_class, FEEDS_BY_CLASS["stock"])
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
        score, text = 1, f"🟢 Бычий (в заголовках: {bull} слов за рост / {bear} за падение)"
    elif bear > bull:
        score, text = -1, f"🔴 Медвежий (в заголовках: {bear} слов за падение / {bull} за рост)"
    else:
        score, text = 0, "⚪ Нейтральный"

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
    """
    Каждый индикатор даёт 1 балл ЛИБО покупке (bs), ЛИБО продаже (ss).
    Если индикатор нейтрален (например RSI между 30 и 70) — он не даёт
    ничего никому, поэтому bs + ss не всегда равно 5.
    """
    bs, ss, br, sr = 0, 0, [], []

    if rsi < 30:
        bs += 1
        br.append(f"RSI = {rsi} (ниже 30 — актив перепродан, часто следует разворот вверх)")
    elif rsi > 70:
        ss += 1
        sr.append(f"RSI = {rsi} (выше 70 — актив перекуплен, часто следует разворот вниз)")

    if macd > macd_s:
        bs += 1
        br.append("MACD выше сигнальной линии — восходящий импульс набирает силу")
    else:
        ss += 1
        sr.append("MACD ниже сигнальной линии — нисходящий импульс набирает силу")

    if price <= bbl * 1.005:
        bs += 1
        br.append("Цена у нижней границы Bollinger Bands — статистически «дёшево», вероятен отскок вверх")
    elif price >= bbu * 0.995:
        ss += 1
        sr.append("Цена у верхней границы Bollinger Bands — статистически «дорого», вероятен откат вниз")

    if trend_bull:
        bs += 1
        br.append("Старший таймфрейм (4H): EMA20 выше EMA50 — общий тренд вверх")
    else:
        ss += 1
        sr.append("Старший таймфрейм (4H): EMA20 ниже EMA50 — общий тренд вниз")

    if vol_ratio >= 1.5:
        if bs >= ss:
            bs += 1
            br.append(f"Объём торгов ×{round(vol_ratio,1)} от среднего — крупные игроки заходят в рост")
        else:
            ss += 1
            sr.append(f"Объём торгов ×{round(vol_ratio,1)} от среднего — крупные игроки продают")

    return bs, ss, br, sr

def build_result(symbol, direction, total_score, tech_score, price, tp_pct, sl_pct,
                  rsi, reasons, news_text, conflicts, news_helped):
    if total_score == MAX_SCORE:
        strength = "🔥 СИЛЬНЫЙ"
    elif total_score == MAX_SCORE - 1:
        strength = "💪 ОЧЕНЬ ХОРОШИЙ"
    else:
        strength = "✅ ХОРОШИЙ"

    if direction == "LONG":
        entry_dir   = "🟢 ПОКУПАЙ (LONG)"
        take_profit = round(price * (1 + tp_pct), 2)
        stop_loss   = round(price * (1 - sl_pct), 2)
    else:
        entry_dir   = "🔴 ПРОДАВАЙ (SHORT)"
        take_profit = round(price * (1 - tp_pct), 2)
        stop_loss   = round(price * (1 + sl_pct), 2)

    return {
        "symbol": symbol, "direction": entry_dir, "strength": strength,
        "score": f"{total_score}/{MAX_SCORE}", "tech_score": f"{tech_score}/5",
        "entry": round(price, 2),
        "take_profit": take_profit, "stop_loss": stop_loss,
        "rsi": rsi, "reasons": reasons,
        "news_text": news_text, "news_conflicts": conflicts,
        "news_helped": news_helped,
    }

def wait_result(symbol, tech_score, total_score, price, rsi, news_text, news_helped):
    return {
        "symbol": symbol, "direction": "⚪️ ЖДАТЬ", "strength": "—",
        "score": f"{total_score}/{MAX_SCORE}", "tech_score": f"{tech_score}/5",
        "entry": round(price, 2),
        "take_profit": None, "stop_loss": None, "rsi": rsi,
        "reasons": ["Недостаточно подтверждений — ждём лучшей точки входа"],
        "news_text": news_text, "news_conflicts": False,
        "news_helped": news_helped,
    }

def _analyze(asset_key: str) -> dict:
    """
    Анализ актива: 5 технических индикаторов + новости (вес x2, максимум 7 баллов).
    Выполняется в отдельном потоке через asyncio.to_thread — не блокирует бота.
    """
    cfg = ASSETS[asset_key]
    ticker_symbol, display_symbol = cfg["ticker"], cfg["name"]
    tp, sl, asset_class = cfg["tp"], cfg["sl"], cfg["class"]
    period = PERIOD_BY_CLASS.get(asset_class, "7d")

    df = yf.Ticker(ticker_symbol).history(period=period, interval="1h")
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
    tech_bs, tech_ss = bs, ss  # технический счёт ДО добавления новостей

    news_score, news_text = _fetch_news(display_symbol, asset_class)
    if news_score > 0:
        bs += NEWS_WEIGHT
        br.append(f"Новости: {news_text} — усиливают сигнал на покупку")
    elif news_score < 0:
        ss += NEWS_WEIGHT
        sr.append(f"Новости: {news_text} — усиливают сигнал на продажу")

    if bs >= MIN_SCORE and bs > ss:
        return build_result(display_symbol, "LONG", bs, tech_bs, p, tp, sl, rsi, br,
                             news_text, conflicts=(news_score < 0),
                             news_helped=(news_score > 0))
    if ss >= MIN_SCORE and ss > bs:
        return build_result(display_symbol, "SHORT", ss, tech_ss, p, tp, sl, rsi, sr,
                             news_text, conflicts=(news_score > 0),
                             news_helped=(news_score < 0))

    if bs >= ss:
        return wait_result(display_symbol, tech_bs, bs, p, rsi, news_text,
                            news_helped=(news_score > 0))
    return wait_result(display_symbol, tech_ss, ss, p, rsi, news_text,
                        news_helped=(news_score < 0))

# ================================================================
# ФОРМАТИРОВАНИЕ
# ================================================================

def fmt(data: dict, is_auto: bool = False) -> str:
    sym    = data["symbol"]
    header = f"🔔 *Автосигнал {sym}*\n\n" if is_auto else f"📊 *Сигнал {sym}*\n\n"

    if data["news_helped"]:
        score_line = f"техника {data['tech_score']} + новости 📰 = *{data['score']}*"
    else:
        score_line = f"техника {data['tech_score']} = *{data['score']}* (новости не в счёт)"

    if data["take_profit"]:
        reasons  = "\n".join(f"{i+1}. {r}" for i, r in enumerate(data["reasons"]))
        conflict = (
            f"\n\n⚠️ *Осторожно:* новости против сигнала ({data['news_text']})"
            if data["news_conflicts"] else ""
        )
        return (
            f"{header}"
            f"Направление: {data['direction']}\n"
            f"Сила: {data['strength']} — {score_line}\n\n"
            f"📍 Точка входа: `${data['entry']}`\n"
            f"🎯 Тейк-профит: `${data['take_profit']}`\n"
            f"🛡 Стоп-лосс: `${data['stop_loss']}`\n\n"
            f"📋 *Почему этот сигнал:*\n{reasons}"
            f"{conflict}\n\n"
            f"📈 RSI: `{data['rsi']}`\n\n"
            f"⚠️ Не является финансовым советом"
        )

    return (
        f"{header}"
        f"Направление: {data['direction']}\n"
        f"Подтверждений: {score_line} — нужно минимум {MIN_SCORE}/{MAX_SCORE}\n\n"
        f"💵 Цена: `${data['entry']}`\n"
        f"📈 RSI: `{data['rsi']}`\n\n"
        f"📰 Новости: {data['news_text']}\n\n"
        f"⏳ Ждём чёткого сигнала...\n\n"
        f"⚠️ Не является финансовым советом"
    )

# ================================================================
# КОМАНДЫ БОТА
# ================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    assets_list = ", ".join(f"{c['emoji']}{c['name']}" for c in ASSETS.values())
    await update.message.reply_text(
        f"👋 Привет, {u.first_name}!\n\n"
        f"🤖 *AlphaX Trade* — твой ИИ-помощник для трейдинга.\n\n"
        f"📊 Слежу за: {assets_list}\n\n"
        f"Анализирую по *{MAX_SCORE} факторам*: 5 технических индикаторов "
        f"+ новости (весят как 2 индикатора).\n"
        f"Сигнал только при *{MIN_SCORE}/{MAX_SCORE}* подтверждениях.\n\n"
        f"Каждый сигнал объясняю по пунктам — почему он сработал.\n\n"
        f"📌 Команды:\n"
        f"/signal — сигнал прямо сейчас\n"
        f"/subscribe — автосигналы каждые 2.5 часа\n"
        f"/unsubscribe — отключить\n"
        f"/help — как это работает",
        parse_mode="Markdown"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    assets_list = "\n".join(f"{c['emoji']} {c['name']}" for c in ASSETS.values())
    await update.message.reply_text(
        "🤖 *Как работает AlphaX Trade:*\n\n"
        f"*Активы:*\n{assets_list}\n\n"
        "*5 технических индикаторов (1 балл каждый):*\n"
        "1. RSI — перекупленность/перепроданность\n"
        "2. MACD — направление импульса\n"
        "3. Bollinger Bands — цена на краю канала\n"
        "4. Тренд 4H — глобальное направление рынка\n"
        "5. Объём — деньги подтверждают движение\n\n"
        "*📰 Новости (до 2 баллов):*\n"
        "Читаю CoinTelegraph, CoinDesk, Reuters, MarketWatch, Kitco.\n"
        "Если технических подтверждений уже 3-4 и новости совпадают по "
        "направлению — этого хватит для сигнала, все 5 технических ждать не нужно.\n\n"
        "⚠️ *Важно:* «Медвежий» сигнал — не «плохо», это направление рынка вниз. "
        "Бот одинаково даёт сигналы и на ПОКУПКУ (LONG), и на ПРОДАЖУ (SHORT). "
        "Цифры вида «5 слов за падение» в новостях — это подсчёт ключевых слов "
        "в заголовках, а не готовые баллы из шкалы 0-7.\n\n"
        f"✅ {MIN_SCORE}/{MAX_SCORE} = Хороший сигнал\n"
        f"💪 {MAX_SCORE-1}/{MAX_SCORE} = Очень хороший\n"
        f"🔥 {MAX_SCORE}/{MAX_SCORE} = Сильный\n\n"
        "/signal — сигнал сейчас\n"
        "/subscribe — автосигналы каждые 2.5ч\n"
        "/unsubscribe — отключить\n\n"
        "⚠️ Акции и NASDAQ обновляются только в часы торгов биржи "
        "(будни, US-время). Крипта и золото — почти круглосуточно.\n\n"
        "⚠️ Не является финансовым советом.",
        parse_mode="Markdown"
    )

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    buttons = [
        InlineKeyboardButton(f"{cfg['emoji']} {cfg['name']}", callback_data=f"sig_{key}")
        for key, cfg in ASSETS.items()
    ]
    kb = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    await update.message.reply_text("Выбери актив:", reply_markup=InlineKeyboardMarkup(kb))

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    if cid in get_subscribers():
        await update.message.reply_text("✅ Ты уже подписан на автосигналы!")
    else:
        add_subscriber(cid)
        await update.message.reply_text(
            "✅ *Автосигналы активированы!*\n\n"
            "Каждые 2.5 часа анализирую все активы.\n"
            "Слабые сигналы пропускаю.\n"
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
    if not q.data.startswith("sig_"):
        return
    asset_key = q.data.removeprefix("sig_")
    if asset_key not in ASSETS:
        return

    await q.edit_message_text("🔍 Анализирую рынок + читаю новости, 10-15 сек...")
    try:
        data = await asyncio.to_thread(_analyze, asset_key)
        await q.edit_message_text(fmt(data), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Ошибка сигнала {asset_key}: {e}")
        await q.edit_message_text(f"❌ Ошибка: {e}")

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    n = count_subscribers()
    await update.message.reply_text(
        f"📊 *Статистика AlphaX Trade*\n\n"
        f"👥 Подписчиков: *{n}*\n"
        f"📈 Активов в анализе: *{len(ASSETS)}*",
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

    logger.info(f"Автосигналы: {len(subs)} подписчиков, {len(ASSETS)} активов")
    to_send = []

    for key, cfg in ASSETS.items():
        try:
            data = await asyncio.to_thread(_analyze, key)
            if data["direction"] != "⚪️ ЖДАТЬ":
                to_send.append(data)
                logger.info(f"{cfg['name']}: {data['direction']} ({data['score']})")
            else:
                logger.info(f"{cfg['name']}: ЖДАТЬ — пропущен")
        except Exception as e:
            logger.error(f"Ошибка {cfg['name']}: {e}")
        await asyncio.sleep(1)

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

    app.job_queue.run_repeating(send_auto_signals, interval=9000, first=10)

    mode = "PostgreSQL ☁️" if DATABASE_URL else "JSON (локально)"
    print(f"✅ AlphaX Trade запущен! Активов: {len(ASSETS)}. Хранилище: {mode}")
    logger.info(f"Бот запущен. Активов: {len(ASSETS)}. Хранилище: {mode}")
    app.run_polling()

if __name__ == "__main__":
    main()