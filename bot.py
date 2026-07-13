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
import ccxt

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
# Для крипты (class="crypto") цена берётся с биржи, выбранной пользователем
# (см. _fetch_ohlcv_dataframe). "ticker" для крипты — это резервный тикер
# Yahoo Finance на случай, если ОБЕ крипто-биржи вдруг недоступны.

ASSETS = {
    "btc":    {"ticker": "BTC-USD",  "crypto_symbol": "BTC/USDT", "name": "BTC/USD", "emoji": "₿",  "tp": 0.040, "sl": 0.020,  "class": "crypto"},
    "eth":    {"ticker": "ETH-USD",  "crypto_symbol": "ETH/USDT", "name": "ETH/USD", "emoji": "Ξ",  "tp": 0.040, "sl": 0.020,  "class": "crypto"},
    "sol":    {"ticker": "SOL-USD",  "crypto_symbol": "SOL/USDT", "name": "SOL/USD", "emoji": "◎",  "tp": 0.050, "sl": 0.025,  "class": "crypto"},
    "gold":   {"ticker": "GC=F",     "name": "XAUUSD",  "emoji": "🥇", "tp": 0.015,  "sl": 0.0075, "class": "gold"},
    "eur":    {"ticker": "EURUSD=X", "name": "EUR/USD", "emoji": "💶", "tp": 0.008,  "sl": 0.004,  "class": "forex"},
    "nasdaq": {"ticker": "^IXIC",    "name": "NASDAQ",  "emoji": "📈", "tp": 0.020,  "sl": 0.010,  "class": "stock"},
    "sp500":  {"ticker": "^GSPC",    "name": "S&P 500", "emoji": "🏛", "tp": 0.020,  "sl": 0.010,  "class": "stock"},
    "aapl":   {"ticker": "AAPL",     "name": "AAPL",    "emoji": "🍎", "tp": 0.025,  "sl": 0.0125, "class": "stock"},
    "tsla":   {"ticker": "TSLA",     "name": "TSLA",    "emoji": "🚗", "tp": 0.035,  "sl": 0.0175, "class": "stock"},
}

PERIOD_BY_CLASS = {
    "crypto": "7d",
    "gold":   "7d",
    "forex":  "7d",
    "stock":  "30d",
}

DEFAULT_EXCHANGE = "binance"

# ================================================================
# ХРАНИЛИЩЕ (подписчики + язык + биржа пользователя)
# ================================================================

if DATABASE_URL:
    import psycopg2

    def _conn():
        return psycopg2.connect(DATABASE_URL, sslmode="require")

    def init_storage():
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS subscribers (chat_id BIGINT PRIMARY KEY);
                    CREATE TABLE IF NOT EXISTS user_lang (
                        chat_id BIGINT PRIMARY KEY,
                        lang TEXT NOT NULL DEFAULT 'ru'
                    );
                    CREATE TABLE IF NOT EXISTS user_exchange (
                        chat_id BIGINT PRIMARY KEY,
                        exchange TEXT NOT NULL DEFAULT 'binance'
                    );
                """)
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

    def get_language(chat_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT lang FROM user_lang WHERE chat_id=%s", (chat_id,))
                row = cur.fetchone()
                return row[0] if row else None

    def set_language(chat_id: int, lang: str):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_lang (chat_id, lang) VALUES (%s, %s)
                    ON CONFLICT (chat_id) DO UPDATE SET lang = EXCLUDED.lang
                """, (chat_id, lang))
            c.commit()

    def get_crypto_exchange(chat_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT exchange FROM user_exchange WHERE chat_id=%s", (chat_id,))
                row = cur.fetchone()
                return row[0] if row else None

    def set_crypto_exchange(chat_id: int, exchange: str):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_exchange (chat_id, exchange) VALUES (%s, %s)
                    ON CONFLICT (chat_id) DO UPDATE SET exchange = EXCLUDED.exchange
                """, (chat_id, exchange))
            c.commit()

else:
    SUBS_FILE = "subscribers.json"
    LANG_FILE = "languages.json"
    EXCHANGE_FILE = "exchanges.json"

    def init_storage(): pass

    def _load_j(f, default):
        return json.load(open(f)) if os.path.exists(f) else default

    def _save_j(f, d):
        with open(f, "w") as fp:
            json.dump(d, fp, ensure_ascii=False, indent=2)

    def get_subscribers() -> set: return set(_load_j(SUBS_FILE, []))
    def add_subscriber(cid: int): s = get_subscribers(); s.add(cid); _save_j(SUBS_FILE, list(s))
    def remove_subscriber(cid: int): s = get_subscribers(); s.discard(cid); _save_j(SUBS_FILE, list(s))
    def count_subscribers() -> int: return len(get_subscribers())

    def get_language(chat_id: int):
        return _load_j(LANG_FILE, {}).get(str(chat_id))

    def set_language(chat_id: int, lang: str):
        d = _load_j(LANG_FILE, {})
        d[str(chat_id)] = lang
        _save_j(LANG_FILE, d)

    def get_crypto_exchange(chat_id: int):
        return _load_j(EXCHANGE_FILE, {}).get(str(chat_id))

    def set_crypto_exchange(chat_id: int, exchange: str):
        d = _load_j(EXCHANGE_FILE, {})
        d[str(chat_id)] = exchange
        _save_j(EXCHANGE_FILE, d)

# ================================================================
# ТЕКСТЫ RU / EN
# ================================================================

LANG = {
    "ru": {
        "choose_language": "🌐 Выбери язык / Choose language:",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇺🇸 English",
        "language_set": "✅ Язык установлен: Русский",

        "choose_exchange": "Выбери биржу для крипто-сигналов (BTC, ETH, SOL):",
        "btn_binance": "🟡 Binance",
        "btn_bybit": "⚫ Bybit",
        "exchange_set_binance": "✅ Биржа для крипто-сигналов: Binance\n\nСменить: /exchange",
        "exchange_set_bybit": "✅ Биржа для крипто-сигналов: Bybit\n\nСменить: /exchange",

        "start": (
            "👋 Привет, {name}!\n\n"
            "🤖 *AlphaX Trade* — твой ИИ-помощник для трейдинга.\n\n"
            "📊 Слежу за: {assets_list}\n\n"
            "Анализирую по *{max_score} факторам*: 5 технических индикаторов "
            "+ новости (весят как 2 индикатора).\n"
            "Сигнал только при *{min_score}/{max_score}* подтверждениях.\n\n"
            "Каждый сигнал объясняю по пунктам и указываю время самих рыночных данных.\n\n"
            "📌 Команды:\n"
            "/signal — сигнал прямо сейчас\n"
            "/subscribe — автосигналы каждые 2.5 часа\n"
            "/unsubscribe — отключить\n"
            "/exchange — биржа для крипто-сигналов (Binance/Bybit)\n"
            "/language — сменить язык\n"
            "/help — как это работает"
        ),

        "help": (
            "🤖 *Как работает AlphaX Trade:*\n\n"
            "*Активы:*\n{assets_list}\n\n"
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
            "🕐 Каждый сигнал показывает время самих рыночных данных — с их родным "
            "смещением UTC, а не время нашего сервера.\n\n"
            "💱 Крипта (BTC, ETH, SOL) — данные с выбранной тобой биржи, "
            "Binance или Bybit (/exchange). Если биржа вдруг недоступна — бот "
            "автоматически переключится на другую, а затем на Yahoo Finance.\n"
            "Золото, EUR/USD, NASDAQ, S&P 500, AAPL, TSLA — всегда Yahoo Finance, "
            "у крипто-бирж таких инструментов просто нет.\n\n"
            "⚠️ *Важно:* «медвежий» сигнал — не «плохо», это просто направление рынка вниз. "
            "Бот одинаково даёт сигналы и на ПОКУПКУ (LONG), и на ПРОДАЖУ (SHORT).\n\n"
            "✅ {min_score}/{max_score} = Хороший сигнал\n"
            "💪 {near_max}/{max_score} = Очень хороший\n"
            "🔥 {max_score}/{max_score} = Сильный\n\n"
            "/signal — сигнал сейчас\n"
            "/subscribe — автосигналы каждые 2.5ч\n"
            "/unsubscribe — отключить\n"
            "/exchange — биржа для крипты\n"
            "/language — сменить язык\n\n"
            "⚠️ Акции, NASDAQ и S&P 500 обновляются только в часы торгов биржи "
            "(будни, US-время). Крипта и золото — почти круглосуточно.\n\n"
            "⚠️ Не является финансовым советом."
        ),

        "choose_asset": "Выбери актив:",
        "analyzing": "🔍 Анализирую рынок + читаю новости, 10-15 сек...",
        "error": "❌ Ошибка: {error}",

        "sub_already": "✅ Ты уже подписан на автосигналы!",
        "sub_activated": (
            "✅ *Автосигналы активированы!*\n\n"
            "Каждые 2.5 часа анализирую все активы.\n"
            "Слабые сигналы пропускаю.\n"
            "Новости проверяю автоматически.\n\n"
            "Отключить: /unsubscribe"
        ),
        "unsub_done": "❌ Автосигналы отключены.\nВернуться: /subscribe",
        "unsub_none": "Ты не был подписан.",

        "header_signal": "📊 *Сигнал {symbol}*\n\n",
        "header_auto": "🔔 *Автосигнал {symbol}*\n\n",

        "dir_buy": "🟢 ПОКУПАЙ (LONG)",
        "dir_sell": "🔴 ПРОДАВАЙ (SHORT)",
        "dir_wait": "⚪️ ЖДАТЬ",

        "str_strong": "🔥 СИЛЬНЫЙ",
        "str_verygood": "💪 ОЧЕНЬ ХОРОШИЙ",
        "str_good": "✅ ХОРОШИЙ",
        "str_dash": "—",

        "lbl_direction": "Направление",
        "lbl_strength": "Сила",
        "lbl_entry": "📍 Точка входа",
        "lbl_tp": "🎯 Тейк-профит",
        "lbl_sl": "🛡 Стоп-лосс",
        "lbl_reasons": "📋 *Почему этот сигнал:*",
        "lbl_rsi": "📈 RSI",
        "lbl_price": "💵 Цена",
        "lbl_confirmations": "Подтверждений",
        "lbl_news": "📰 Новости",
        "lbl_waiting": "⏳ Ждём чёткого сигнала...",
        "lbl_disclaimer": "⚠️ Не является финансовым советом",
        "lbl_signal_time": "🕐 Время котировки: {time}",

        "score_combined": "техника {tech}/5 + новости 📰 = *{total}/7*",
        "score_tech_only": "техника {tech}/5 = *{total}/7* (новости не в счёт)",
        "need_min": "нужно минимум {min_score}/{max_score}",

        "conflict_warning": "\n\n⚠️ *Осторожно:* новости против сигнала ({news_text})",

        "reason_rsi_oversold": "RSI = {rsi} (ниже 30 — актив перепродан, часто следует разворот вверх)",
        "reason_rsi_overbought": "RSI = {rsi} (выше 70 — актив перекуплен, часто следует разворот вниз)",
        "reason_macd_bull": "MACD выше сигнальной линии — восходящий импульс набирает силу",
        "reason_macd_bear": "MACD ниже сигнальной линии — нисходящий импульс набирает силу",
        "reason_bb_lower": "Цена у нижней границы Bollinger Bands — статистически «дёшево», вероятен отскок вверх",
        "reason_bb_upper": "Цена у верхней границы Bollinger Bands — статистически «дорого», вероятен откат вниз",
        "reason_trend_up": "Старший таймфрейм (4H): EMA20 выше EMA50 — общий тренд вверх",
        "reason_trend_down": "Старший таймфрейм (4H): EMA20 ниже EMA50 — общий тренд вниз",
        "reason_vol_bull": "Объём торгов ×{ratio} от среднего — крупные игроки заходят в рост",
        "reason_vol_bear": "Объём торгов ×{ratio} от среднего — крупные игроки продают",
        "reason_news_buy": "Новости: {news_text} — усиливают сигнал на покупку",
        "reason_news_sell": "Новости: {news_text} — усиливают сигнал на продажу",

        "news_bullish": "🟢 Бычий (в заголовках: {bull} слов за рост / {bear} за падение)",
        "news_bearish": "🔴 Медвежий (в заголовках: {bear} слов за падение / {bull} за рост)",
        "news_neutral": "⚪ Нейтральный",
    },
    "en": {
        "choose_language": "🌐 Выбери язык / Choose language:",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇺🇸 English",
        "language_set": "✅ Language set: English",

        "choose_exchange": "Choose an exchange for crypto signals (BTC, ETH, SOL):",
        "btn_binance": "🟡 Binance",
        "btn_bybit": "⚫ Bybit",
        "exchange_set_binance": "✅ Crypto signal exchange: Binance\n\nChange: /exchange",
        "exchange_set_bybit": "✅ Crypto signal exchange: Bybit\n\nChange: /exchange",

        "start": (
            "👋 Hi, {name}!\n\n"
            "🤖 *AlphaX Trade* — your AI trading assistant.\n\n"
            "📊 Tracking: {assets_list}\n\n"
            "I analyze using *{max_score} factors*: 5 technical indicators "
            "+ news (weighted as 2 indicators).\n"
            "A signal fires only at *{min_score}/{max_score}* confirmations.\n\n"
            "Every signal is explained point by point, with the actual market data time.\n\n"
            "📌 Commands:\n"
            "/signal — get a signal right now\n"
            "/subscribe — auto-signals every 2.5 hours\n"
            "/unsubscribe — turn off\n"
            "/exchange — exchange for crypto signals (Binance/Bybit)\n"
            "/language — change language\n"
            "/help — how it works"
        ),

        "help": (
            "🤖 *How AlphaX Trade works:*\n\n"
            "*Assets:*\n{assets_list}\n\n"
            "*5 technical indicators (1 point each):*\n"
            "1. RSI — overbought/oversold\n"
            "2. MACD — momentum direction\n"
            "3. Bollinger Bands — price at the edge of the channel\n"
            "4. 4H trend — overall market direction\n"
            "5. Volume — money confirming the move\n\n"
            "*📰 News (up to 2 points):*\n"
            "I read CoinTelegraph, CoinDesk, Reuters, MarketWatch, Kitco.\n"
            "If there are already 3-4 technical confirmations and news agrees with the "
            "direction — that's enough for a signal, you don't need all 5 technical ones.\n\n"
            "🕐 Every signal shows the time of the actual market data — with its own "
            "UTC offset, not our server's time.\n\n"
            "💱 Crypto (BTC, ETH, SOL) — data from your chosen exchange, "
            "Binance or Bybit (/exchange). If that exchange is unavailable, the bot "
            "automatically switches to the other one, then to Yahoo Finance.\n"
            "Gold, EUR/USD, NASDAQ, S&P 500, AAPL, TSLA — always Yahoo Finance, "
            "crypto exchanges simply don't have these instruments.\n\n"
            "⚠️ *Important:* a «bearish» signal isn't «bad» — it's just a downward market direction. "
            "The bot gives signals for both BUY (LONG) and SELL (SHORT) equally.\n\n"
            "✅ {min_score}/{max_score} = Good signal\n"
            "💪 {near_max}/{max_score} = Very good\n"
            "🔥 {max_score}/{max_score} = Strong\n\n"
            "/signal — get a signal now\n"
            "/subscribe — auto-signals every 2.5h\n"
            "/unsubscribe — turn off\n"
            "/exchange — crypto data exchange\n"
            "/language — change language\n\n"
            "⚠️ Stocks, NASDAQ and S&P 500 only update during exchange trading hours "
            "(weekdays, US time). Crypto and gold — nearly 24/7.\n\n"
            "⚠️ Not financial advice."
        ),

        "choose_asset": "Choose an asset:",
        "analyzing": "🔍 Analyzing the market + reading the news, 10-15 sec...",
        "error": "❌ Error: {error}",

        "sub_already": "✅ You're already subscribed to auto-signals!",
        "sub_activated": (
            "✅ *Auto-signals activated!*\n\n"
            "I analyze all assets every 2.5 hours.\n"
            "Weak signals are skipped.\n"
            "News is checked automatically.\n\n"
            "Turn off: /unsubscribe"
        ),
        "unsub_done": "❌ Auto-signals turned off.\nCome back: /subscribe",
        "unsub_none": "You weren't subscribed.",

        "header_signal": "📊 *{symbol} Signal*\n\n",
        "header_auto": "🔔 *{symbol} Auto-signal*\n\n",

        "dir_buy": "🟢 BUY (LONG)",
        "dir_sell": "🔴 SELL (SHORT)",
        "dir_wait": "⚪️ WAIT",

        "str_strong": "🔥 STRONG",
        "str_verygood": "💪 VERY GOOD",
        "str_good": "✅ GOOD",
        "str_dash": "—",

        "lbl_direction": "Direction",
        "lbl_strength": "Strength",
        "lbl_entry": "📍 Entry point",
        "lbl_tp": "🎯 Take-profit",
        "lbl_sl": "🛡 Stop-loss",
        "lbl_reasons": "📋 *Why this signal:*",
        "lbl_rsi": "📈 RSI",
        "lbl_price": "💵 Price",
        "lbl_confirmations": "Confirmations",
        "lbl_news": "📰 News",
        "lbl_waiting": "⏳ Waiting for a clear signal...",
        "lbl_disclaimer": "⚠️ Not financial advice",
        "lbl_signal_time": "🕐 Quote time: {time}",

        "score_combined": "technicals {tech}/5 + news 📰 = *{total}/7*",
        "score_tech_only": "technicals {tech}/5 = *{total}/7* (news not counted)",
        "need_min": "need at least {min_score}/{max_score}",

        "conflict_warning": "\n\n⚠️ *Careful:* news contradicts the signal ({news_text})",

        "reason_rsi_oversold": "RSI = {rsi} (below 30 — asset is oversold, a reversal up often follows)",
        "reason_rsi_overbought": "RSI = {rsi} (above 70 — asset is overbought, a reversal down often follows)",
        "reason_macd_bull": "MACD above the signal line — upward momentum building",
        "reason_macd_bear": "MACD below the signal line — downward momentum building",
        "reason_bb_lower": "Price at the lower Bollinger Band — statistically «cheap», a bounce up is likely",
        "reason_bb_upper": "Price at the upper Bollinger Band — statistically «expensive», a pullback down is likely",
        "reason_trend_up": "Higher timeframe (4H): EMA20 above EMA50 — overall trend is up",
        "reason_trend_down": "Higher timeframe (4H): EMA20 below EMA50 — overall trend is down",
        "reason_vol_bull": "Trading volume ×{ratio} of average — large players are buying the rally",
        "reason_vol_bear": "Trading volume ×{ratio} of average — large players are selling",
        "reason_news_buy": "News: {news_text} — reinforces the buy signal",
        "reason_news_sell": "News: {news_text} — reinforces the sell signal",

        "news_bullish": "🟢 Bullish (in headlines: {bull} words for growth / {bear} for decline)",
        "news_bearish": "🔴 Bearish (in headlines: {bear} words for decline / {bull} for growth)",
        "news_neutral": "⚪ Neutral",
    },
}


def render(lang: str, key: str, **params) -> str:
    """Достаёт шаблон по ключу и языку, подставляет параметры."""
    return LANG[lang][key].format(**params)

# ================================================================
# АНАЛИЗ НОВОСТЕЙ (RSS + ключевые слова) — язык-независимый слой
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

def _fetch_news(symbol: str, asset_class: str) -> tuple[int, str, dict]:
    """
    Возвращает (score, ключ_шаблона, параметры) — БЕЗ привязки к языку.
    score: +1 бычьи слова перевешивают, -1 медвежьи перевешивают, 0 нейтрально.
    """
    now = time.time()
    if symbol in _news_cache:
        ts, score, key, params = _news_cache[symbol]
        if now - ts < NEWS_TTL:
            return score, key, params

    feeds = FEEDS_BY_CLASS.get(asset_class, FEEDS_BY_CLASS["stock"])
    bull, bear = 0, 0

    for url in feeds:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                title = entry.get("title", "").lower()
                bull += sum(1 for w in BULLISH_WORDS if w in title)
                bear += sum(1 for w in BEARISH_WORDS if w in title)
        except Exception as e:
            logger.warning(f"RSS ошибка {url}: {e}")

    if bull > bear:
        score, key, params = 1, "news_bullish", {"bull": bull, "bear": bear}
    elif bear > bull:
        score, key, params = -1, "news_bearish", {"bull": bull, "bear": bear}
    else:
        score, key, params = 0, "news_neutral", {}

    _news_cache[symbol] = (now, score, key, params)
    logger.info(f"Новости {symbol}: bull={bull} bear={bear} -> {key}")
    return score, key, params

# ================================================================
# ИСТОЧНИКИ ЦЕН: крипто-биржи (ccxt) + Yahoo Finance
# ================================================================

def _make_exchange(exchange_name: str):
    if exchange_name == "bybit":
        return ccxt.bybit({"enableRateLimit": True})
    return ccxt.binance({"enableRateLimit": True})

def _fetch_crypto_ohlcv(symbol: str, exchange_name: str) -> pd.DataFrame:
    exchange = _make_exchange(exchange_name)
    raw = exchange.fetch_ohlcv(symbol, timeframe="1h", limit=200)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df.set_index("ts")[["close", "volume"]]

def _fetch_yahoo_ohlcv(cfg: dict) -> pd.DataFrame:
    period = PERIOD_BY_CLASS.get(cfg["class"], "7d")
    df = yf.Ticker(cfg["ticker"]).history(period=period, interval="1h")
    df.columns = [c.lower() for c in df.columns]
    return df[["close", "volume"]]

def _fetch_ohlcv_dataframe(cfg: dict, exchange_pref: str = None) -> pd.DataFrame:
    """
    Крипта: пробуем предпочтение пользователя, при сбое — вторую крипто-биржу,
    и только потом Yahoo как последний резерв. Остальные классы — сразу Yahoo,
    у бирж этих инструментов просто нет.
    """
    if cfg["class"] == "crypto":
        preferred = exchange_pref or DEFAULT_EXCHANGE
        other = "bybit" if preferred == "binance" else "binance"
        for exch in (preferred, other):
            try:
                return _fetch_crypto_ohlcv(cfg["crypto_symbol"], exch)
            except Exception as e:
                logger.warning(f"{exch} недоступен для {cfg['crypto_symbol']}: {e}")
        logger.warning(f"Обе крипто-биржи недоступны для {cfg['crypto_symbol']}, использую Yahoo")

    return _fetch_yahoo_ohlcv(cfg)

# ================================================================
# ТЕХНИЧЕСКИЙ АНАЛИЗ — язык-независимый слой
# ================================================================

def _get_bbands(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    bb = ta.bbands(series, length=20, std=2)
    if bb is None or bb.empty:
        return series * 1.02, series * 0.98
    u = [c for c in bb.columns if c.startswith("BBU")][0]
    l = [c for c in bb.columns if c.startswith("BBL")][0]
    return bb[u], bb[l]

def _format_market_time(ts) -> str:
    """
    ts — pandas Timestamp последней свечи (уже привязан к часовому поясу
    конкретного рынка/биржи). Показываем как есть, с явным смещением UTC —
    это время самих данных, а не время сервера бота.
    """
    if ts.tzinfo is None:
        return ts.strftime("%d.%m %H:%M")
    offset = ts.utcoffset()
    if offset is None:
        return ts.strftime("%d.%m %H:%M")
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    hh, mm = divmod(abs(total_minutes), 60)
    offset_str = f"UTC{sign}{hh}" + (f":{mm:02d}" if mm else "")
    return f"{ts.strftime('%d.%m %H:%M')} ({offset_str})"

def _score_technical(rsi, macd, macd_s, price, bbu, bbl, trend_bull, vol_ratio):
    """Каждый индикатор возвращает (ключ_шаблона, параметры), а не готовый текст."""
    bs, ss = 0, 0
    br, sr = [], []

    if rsi < 30:
        bs += 1
        br.append(("reason_rsi_oversold", {"rsi": rsi}))
    elif rsi > 70:
        ss += 1
        sr.append(("reason_rsi_overbought", {"rsi": rsi}))

    if macd > macd_s:
        bs += 1
        br.append(("reason_macd_bull", {}))
    else:
        ss += 1
        sr.append(("reason_macd_bear", {}))

    if price <= bbl * 1.005:
        bs += 1
        br.append(("reason_bb_lower", {}))
    elif price >= bbu * 0.995:
        ss += 1
        sr.append(("reason_bb_upper", {}))

    if trend_bull:
        bs += 1
        br.append(("reason_trend_up", {}))
    else:
        ss += 1
        sr.append(("reason_trend_down", {}))

    if vol_ratio >= 1.5:
        if bs >= ss:
            bs += 1
            br.append(("reason_vol_bull", {"ratio": round(vol_ratio, 1)}))
        else:
            ss += 1
            sr.append(("reason_vol_bear", {"ratio": round(vol_ratio, 1)}))

    return bs, ss, br, sr

def _analyze(asset_key: str, exchange_pref: str = None) -> dict:
    """
    Анализ актива: 5 технических индикаторов + новости (вес x2, максимум 7 баллов).
    Результат полностью язык-независим — рендер под конкретный язык делает fmt().
    exchange_pref используется только для активов класса "crypto".
    Выполняется в отдельном потоке через asyncio.to_thread — не блокирует бота.
    """
    cfg = ASSETS[asset_key]
    display_symbol = cfg["name"]
    tp, sl = cfg["tp"], cfg["sl"]

    df = _fetch_ohlcv_dataframe(cfg, exchange_pref)
    if df.empty:
        raise ValueError(f"Нет данных: {display_symbol}")

    bar_time = df.index[-1]  # время последней свечи — родное для этого источника

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
    tech_bs, tech_ss = bs, ss

    news_score, news_key, news_params = _fetch_news(display_symbol, cfg["class"])
    if news_score > 0:
        bs += NEWS_WEIGHT
    elif news_score < 0:
        ss += NEWS_WEIGHT

    common = {
        "symbol": display_symbol,
        "rsi": rsi,
        "entry": round(p, 2),
        "news_key": news_key,
        "news_params": news_params,
        "data_time": bar_time,
    }

    if bs >= MIN_SCORE and bs > ss:
        return {
            **common,
            "direction": "LONG",
            "total_score": bs, "tech_score": tech_bs,
            "take_profit": round(p * (1 + tp), 2),
            "stop_loss": round(p * (1 - sl), 2),
            "reasons": br,
            "news_conflicts": news_score < 0,
            "news_helped": news_score > 0,
        }

    if ss >= MIN_SCORE and ss > bs:
        return {
            **common,
            "direction": "SHORT",
            "total_score": ss, "tech_score": tech_ss,
            "take_profit": round(p * (1 - tp), 2),
            "stop_loss": round(p * (1 + sl), 2),
            "reasons": sr,
            "news_conflicts": news_score > 0,
            "news_helped": news_score < 0,
        }

    if bs >= ss:
        return {
            **common, "direction": "WAIT",
            "total_score": bs, "tech_score": tech_bs,
            "take_profit": None, "stop_loss": None,
            "reasons": [], "news_conflicts": False,
            "news_helped": news_score > 0,
        }
    return {
        **common, "direction": "WAIT",
        "total_score": ss, "tech_score": tech_ss,
        "take_profit": None, "stop_loss": None,
        "reasons": [], "news_conflicts": False,
        "news_helped": news_score < 0,
    }

# ================================================================
# ФОРМАТИРОВАНИЕ — единственное место, где нужен язык
# ================================================================

def fmt(data: dict, lang: str, is_auto: bool = False) -> str:
    t = LANG[lang]
    sym = data["symbol"]
    header = (t["header_auto"] if is_auto else t["header_signal"]).format(symbol=sym)

    direction_label = {
        "LONG": t["dir_buy"], "SHORT": t["dir_sell"], "WAIT": t["dir_wait"]
    }[data["direction"]]

    time_line = t["lbl_signal_time"].format(time=_format_market_time(data["data_time"]))
    news_text = render(lang, data["news_key"], **data["news_params"])

    if data["direction"] == "WAIT":
        strength = t["str_dash"]
    elif data["total_score"] == MAX_SCORE:
        strength = t["str_strong"]
    elif data["total_score"] == MAX_SCORE - 1:
        strength = t["str_verygood"]
    else:
        strength = t["str_good"]

    score_line = (t["score_combined"] if data["news_helped"] else t["score_tech_only"]).format(
        tech=data["tech_score"], total=data["total_score"]
    )

    if data["direction"] != "WAIT":
        rendered = [render(lang, k, **p) for k, p in data["reasons"]]
        if data["news_helped"]:
            key = "reason_news_buy" if data["direction"] == "LONG" else "reason_news_sell"
            rendered.append(t[key].format(news_text=news_text))
        reasons_block = "\n".join(f"{i+1}. {r}" for i, r in enumerate(rendered))

        conflict = t["conflict_warning"].format(news_text=news_text) if data["news_conflicts"] else ""

        return (
            f"{header}"
            f"{t['lbl_direction']}: {direction_label}\n"
            f"{t['lbl_strength']}: {strength} — {score_line}\n\n"
            f"{t['lbl_entry']}: `${data['entry']}`\n"
            f"{t['lbl_tp']}: `${data['take_profit']}`\n"
            f"{t['lbl_sl']}: `${data['stop_loss']}`\n\n"
            f"{t['lbl_reasons']}\n{reasons_block}"
            f"{conflict}\n\n"
            f"{t['lbl_rsi']}: `{data['rsi']}`\n"
            f"{time_line}\n\n"
            f"{t['lbl_disclaimer']}"
        )

    return (
        f"{header}"
        f"{t['lbl_direction']}: {direction_label}\n"
        f"{t['lbl_confirmations']}: {score_line} — "
        f"{t['need_min'].format(min_score=MIN_SCORE, max_score=MAX_SCORE)}\n\n"
        f"{t['lbl_price']}: `${data['entry']}`\n"
        f"{t['lbl_rsi']}: `{data['rsi']}`\n\n"
        f"{t['lbl_news']}: {news_text}\n\n"
        f"{t['lbl_waiting']}\n"
        f"{time_line}\n\n"
        f"{t['lbl_disclaimer']}"
    )

# ================================================================
# ВЫБОР ЯЗЫКА / БИРЖИ
# ================================================================

def _lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(LANG["ru"]["btn_ru"], callback_data="lang_ru"),
        InlineKeyboardButton(LANG["en"]["btn_en"], callback_data="lang_en"),
    ]])

def _exchange_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(LANG["ru"]["btn_binance"], callback_data="exch_binance"),
        InlineKeyboardButton(LANG["ru"]["btn_bybit"], callback_data="exch_bybit"),
    ]])

async def _send_start(chat_id: int, context: ContextTypes.DEFAULT_TYPE, user, lang: str):
    assets_list = ", ".join(f"{c['emoji']}{c['name']}" for c in ASSETS.values())
    text = LANG[lang]["start"].format(
        name=user.first_name, assets_list=assets_list,
        max_score=MAX_SCORE, min_score=MIN_SCORE
    )
    await context.bot.send_message(chat_id, text, parse_mode="Markdown")

async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(LANG["ru"]["choose_language"], reply_markup=_lang_keyboard())

async def exchange_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_language(update.effective_user.id) or "ru"
    await update.message.reply_text(LANG[lang]["choose_exchange"], reply_markup=_exchange_keyboard())

# ================================================================
# КОМАНДЫ ПОЛЬЗОВАТЕЛЯ
# ================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    lang = get_language(u.id)
    if lang is None:
        await update.message.reply_text(LANG["ru"]["choose_language"], reply_markup=_lang_keyboard())
        return
    await _send_start(update.effective_chat.id, context, u, lang)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_language(update.effective_user.id) or "ru"
    assets_list = "\n".join(f"{c['emoji']} {c['name']}" for c in ASSETS.values())
    text = LANG[lang]["help"].format(
        assets_list=assets_list, min_score=MIN_SCORE, max_score=MAX_SCORE, near_max=MAX_SCORE - 1
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_language(update.effective_user.id) or "ru"
    buttons = [
        InlineKeyboardButton(f"{cfg['emoji']} {cfg['name']}", callback_data=f"sig_{key}")
        for key, cfg in ASSETS.items()
    ]
    kb = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    await update.message.reply_text(LANG[lang]["choose_asset"], reply_markup=InlineKeyboardMarkup(kb))

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    lang = get_language(update.effective_user.id) or "ru"
    if cid in get_subscribers():
        await update.message.reply_text(LANG[lang]["sub_already"])
    else:
        add_subscriber(cid)
        await update.message.reply_text(LANG[lang]["sub_activated"], parse_mode="Markdown")

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    lang = get_language(update.effective_user.id) or "ru"
    if cid in get_subscribers():
        remove_subscriber(cid)
        await update.message.reply_text(LANG[lang]["unsub_done"])
    else:
        await update.message.reply_text(LANG[lang]["unsub_none"])

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data.startswith("lang_"):
        lang = q.data.removeprefix("lang_")
        if lang not in LANG:
            return
        set_language(q.from_user.id, lang)
        await q.edit_message_text(LANG[lang]["language_set"])
        await _send_start(q.message.chat_id, context, q.from_user, lang)
        return

    if q.data.startswith("exch_"):
        exch = q.data.removeprefix("exch_")
        if exch not in ("binance", "bybit"):
            return
        set_crypto_exchange(q.from_user.id, exch)
        lang = get_language(q.from_user.id) or "ru"
        key = "exchange_set_binance" if exch == "binance" else "exchange_set_bybit"
        await q.edit_message_text(LANG[lang][key])
        return

    if q.data.startswith("sig_"):
        asset_key = q.data.removeprefix("sig_")
        if asset_key not in ASSETS:
            return
        lang = get_language(q.from_user.id) or "ru"
        exch_pref = get_crypto_exchange(q.from_user.id) or DEFAULT_EXCHANGE
        await q.edit_message_text(LANG[lang]["analyzing"])
        try:
            data = await asyncio.to_thread(_analyze, asset_key, exch_pref)
            await q.edit_message_text(fmt(data, lang), parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка сигнала {asset_key}: {e}")
            await q.edit_message_text(LANG[lang]["error"].format(error=e))

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

    exch_by_sub = {cid: (get_crypto_exchange(cid) or DEFAULT_EXCHANGE) for cid in subs}
    needed_exchanges = set(exch_by_sub.values())

    # Некрипто-активы считаем один раз — они не зависят от биржи
    non_crypto = []
    for key, cfg in ASSETS.items():
        if cfg["class"] == "crypto":
            continue
        try:
            data = await asyncio.to_thread(_analyze, key)
            if data["direction"] != "WAIT":
                non_crypto.append(data)
                logger.info(f"{cfg['name']}: {data['direction']} ({data['total_score']}/{MAX_SCORE})")
            else:
                logger.info(f"{cfg['name']}: WAIT — пропущен")
        except Exception as e:
            logger.error(f"Ошибка {cfg['name']}: {e}")
        await asyncio.sleep(1)

    # Крипто-активы считаем один раз НА КАЖДУЮ нужную биржу среди подписчиков
    crypto_by_exch = {}
    for exch in needed_exchanges:
        signals = []
        for key, cfg in ASSETS.items():
            if cfg["class"] != "crypto":
                continue
            try:
                data = await asyncio.to_thread(_analyze, key, exch)
                if data["direction"] != "WAIT":
                    signals.append(data)
                    logger.info(f"{cfg['name']} [{exch}]: {data['direction']} ({data['total_score']}/{MAX_SCORE})")
                else:
                    logger.info(f"{cfg['name']} [{exch}]: WAIT — пропущен")
            except Exception as e:
                logger.error(f"Ошибка {cfg['name']} [{exch}]: {e}")
            await asyncio.sleep(1)
        crypto_by_exch[exch] = signals

    dead = set()
    for cid in subs.copy():
        lang = get_language(cid) or "ru"
        to_send = non_crypto + crypto_by_exch.get(exch_by_sub[cid], [])
        if not to_send:
            continue
        for d in to_send:
            try:
                await context.bot.send_message(cid, fmt(d, lang, is_auto=True), parse_mode="Markdown")
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
    app.add_handler(CommandHandler("language",    language_command))
    app.add_handler(CommandHandler("exchange",    exchange_command))
    app.add_handler(CommandHandler("users",       users_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.job_queue.run_repeating(send_auto_signals, interval=9000, first=10)

    mode = "PostgreSQL ☁️" if DATABASE_URL else "JSON (локально)"
    print(f"✅ AlphaX Trade запущен! Активов: {len(ASSETS)}. Хранилище: {mode}")
    logger.info(f"Бот запущен. Активов: {len(ASSETS)}. Хранилище: {mode}")
    app.run_polling()

if __name__ == "__main__":
    main()