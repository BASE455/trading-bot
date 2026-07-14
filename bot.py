import os
import json
import logging
import asyncio
import time
from datetime import datetime
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
MAX_SCORE   = 8   # 6 (техника) + 2 (новости)
MIN_SCORE   = 6   # минимум для сигнала (75%)

ATR_SL_MULTIPLIER = 1.5  # стоп = 1.5×ATR, тейк = 2× от этого расстояния (R:R 2:1 сохраняем)

DEFAULT_EXCHANGE = "binance"

# ================================================================
# АКТИВЫ
# ================================================================
# tp/sl больше не хранятся здесь — теперь считаются динамически через ATR,
# одинаковая логика для всех активов, без ручной подстройки процентов.

ASSETS = {
    "btc":    {"ticker": "BTC-USD",  "crypto_symbol": "BTC/USDT", "name": "BTC/USD", "emoji": "₿",  "class": "crypto"},
    "eth":    {"ticker": "ETH-USD",  "crypto_symbol": "ETH/USDT", "name": "ETH/USD", "emoji": "Ξ",  "class": "crypto"},
    "sol":    {"ticker": "SOL-USD",  "crypto_symbol": "SOL/USDT", "name": "SOL/USD", "emoji": "◎",  "class": "crypto"},
    "gold":   {"ticker": "GC=F",     "name": "XAUUSD",  "emoji": "🥇", "class": "gold"},
    "eur":    {"ticker": "EURUSD=X", "name": "EUR/USD", "emoji": "💶", "class": "forex"},
    "nasdaq": {"ticker": "^IXIC",    "name": "NASDAQ",  "emoji": "📈", "class": "stock"},
    "sp500":  {"ticker": "^GSPC",    "name": "S&P 500", "emoji": "🏛", "class": "stock"},
    "aapl":   {"ticker": "AAPL",     "name": "AAPL",    "emoji": "🍎", "class": "stock"},
    "tsla":   {"ticker": "TSLA",     "name": "TSLA",    "emoji": "🚗", "class": "stock"},
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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_lang (
                        chat_id BIGINT PRIMARY KEY,
                        lang TEXT NOT NULL DEFAULT 'ru'
                    );
                    CREATE TABLE IF NOT EXISTS user_exchange (
                        chat_id BIGINT PRIMARY KEY,
                        exchange TEXT NOT NULL DEFAULT 'binance'
                    );
                    CREATE TABLE IF NOT EXISTS tracked_signals (
                        id SERIAL PRIMARY KEY,
                        asset_key TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        entry DOUBLE PRECISION NOT NULL,
                        take_profit DOUBLE PRECISION NOT NULL,
                        stop_loss DOUBLE PRECISION NOT NULL,
                        issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        status TEXT NOT NULL DEFAULT 'open',
                        resolved_at TIMESTAMPTZ,
                        resolved_price DOUBLE PRECISION
                    );
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        chat_id BIGINT NOT NULL,
                        asset_key TEXT NOT NULL,
                        PRIMARY KEY (chat_id, asset_key)
                    );
                """)
            c.commit()
        logger.info("PostgreSQL инициализирован")

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

    def add_tracked_signal(asset_key, symbol, direction, entry, tp, sl):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    INSERT INTO tracked_signals (asset_key, symbol, direction, entry, take_profit, stop_loss)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (asset_key, symbol, direction, entry, tp, sl))
            c.commit()

    def has_open_signal(asset_key) -> bool:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM tracked_signals WHERE asset_key=%s AND status='open' LIMIT 1",
                    (asset_key,)
                )
                return cur.fetchone() is not None

    def get_open_tracked_signals() -> list:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    SELECT id, asset_key, direction, take_profit, stop_loss
                    FROM tracked_signals WHERE status='open'
                """)
                rows = cur.fetchall()
        return [
            {"id": r[0], "asset_key": r[1], "direction": r[2], "take_profit": r[3], "stop_loss": r[4]}
            for r in rows
        ]

    def resolve_tracked_signal(signal_id, status, resolved_price):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_signals SET status=%s, resolved_at=NOW(), resolved_price=%s
                    WHERE id=%s
                """, (status, resolved_price, signal_id))
            c.commit()

    def get_tracker_stats() -> dict:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT status, COUNT(*) FROM tracked_signals GROUP BY status")
                rows = dict(cur.fetchall())
        wins, losses, open_ = rows.get("win", 0), rows.get("loss", 0), rows.get("open", 0)
        total = wins + losses
        return {
            "wins": wins, "losses": losses, "open": open_, "total_closed": total,
            "win_rate": round(100 * wins / total, 1) if total > 0 else None,
        }

    def get_user_subscriptions(chat_id: int) -> set:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT asset_key FROM subscriptions WHERE chat_id=%s", (chat_id,))
                return {r[0] for r in cur.fetchall()}

    def toggle_subscription(chat_id: int, asset_key: str):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM subscriptions WHERE chat_id=%s AND asset_key=%s",
                    (chat_id, asset_key)
                )
                if cur.fetchone():
                    cur.execute(
                        "DELETE FROM subscriptions WHERE chat_id=%s AND asset_key=%s",
                        (chat_id, asset_key)
                    )
                else:
                    cur.execute(
                        "INSERT INTO subscriptions (chat_id, asset_key) VALUES (%s, %s)",
                        (chat_id, asset_key)
                    )
            c.commit()

    def get_subscribers_for_asset(asset_key: str) -> set:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT chat_id FROM subscriptions WHERE asset_key=%s", (asset_key,))
                return {r[0] for r in cur.fetchall()}

    def get_all_subscribed_chat_ids() -> set:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT DISTINCT chat_id FROM subscriptions")
                return {r[0] for r in cur.fetchall()}

    def remove_all_subscriptions(chat_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM subscriptions WHERE chat_id=%s", (chat_id,))
            c.commit()

    def count_subscribers() -> int:
        return len(get_all_subscribed_chat_ids())

else:
    LANG_FILE = "languages.json"
    EXCHANGE_FILE = "exchanges.json"
    SIGNALS_FILE = "tracked_signals.json"
    SUBS_FILE = "subscriptions.json"  # {chat_id_str: [asset_key, ...]}

    def init_storage(): pass

    def _load_j(f, default):
        return json.load(open(f)) if os.path.exists(f) else default

    def _save_j(f, d):
        with open(f, "w") as fp:
            json.dump(d, fp, ensure_ascii=False, indent=2)

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

    def add_tracked_signal(asset_key, symbol, direction, entry, tp, sl):
        signals = _load_j(SIGNALS_FILE, [])
        new_id = max((s["id"] for s in signals), default=0) + 1
        signals.append({
            "id": new_id, "asset_key": asset_key, "symbol": symbol, "direction": direction,
            "entry": entry, "take_profit": tp, "stop_loss": sl,
            "issued_at": datetime.now().isoformat(), "status": "open",
            "resolved_at": None, "resolved_price": None,
        })
        _save_j(SIGNALS_FILE, signals)

    def has_open_signal(asset_key) -> bool:
        return any(s["asset_key"] == asset_key and s["status"] == "open" for s in _load_j(SIGNALS_FILE, []))

    def get_open_tracked_signals() -> list:
        return [s for s in _load_j(SIGNALS_FILE, []) if s["status"] == "open"]

    def resolve_tracked_signal(signal_id, status, resolved_price):
        signals = _load_j(SIGNALS_FILE, [])
        for s in signals:
            if s["id"] == signal_id:
                s["status"] = status
                s["resolved_price"] = resolved_price
                s["resolved_at"] = datetime.now().isoformat()
                break
        _save_j(SIGNALS_FILE, signals)

    def get_tracker_stats() -> dict:
        signals = _load_j(SIGNALS_FILE, [])
        wins = sum(1 for s in signals if s["status"] == "win")
        losses = sum(1 for s in signals if s["status"] == "loss")
        open_ = sum(1 for s in signals if s["status"] == "open")
        total = wins + losses
        return {
            "wins": wins, "losses": losses, "open": open_, "total_closed": total,
            "win_rate": round(100 * wins / total, 1) if total > 0 else None,
        }

    def get_user_subscriptions(chat_id) -> set:
        return set(_load_j(SUBS_FILE, {}).get(str(chat_id), []))

    def toggle_subscription(chat_id, asset_key):
        d = _load_j(SUBS_FILE, {})
        key = str(chat_id)
        current = set(d.get(key, []))
        if asset_key in current:
            current.discard(asset_key)
        else:
            current.add(asset_key)
        if current:
            d[key] = list(current)
        else:
            d.pop(key, None)
        _save_j(SUBS_FILE, d)

    def get_subscribers_for_asset(asset_key) -> set:
        d = _load_j(SUBS_FILE, {})
        return {int(cid) for cid, assets in d.items() if asset_key in assets}

    def get_all_subscribed_chat_ids() -> set:
        return {int(cid) for cid in _load_j(SUBS_FILE, {}).keys()}

    def remove_all_subscriptions(chat_id):
        d = _load_j(SUBS_FILE, {})
        d.pop(str(chat_id), None)
        _save_j(SUBS_FILE, d)

    def count_subscribers() -> int:
        return len(get_all_subscribed_chat_ids())

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
            "Анализирую по *{max_score} факторам*: 6 технических индикаторов "
            "+ новости (весят как 2 индикатора).\n"
            "Сигнал только при *{min_score}/{max_score}* подтверждениях.\n\n"
            "SL/TP считаются динамически от текущей волатильности (ATR), а не по "
            "фиксированному проценту.\n\n"
            "📌 Команды:\n"
            "/signal — сигнал прямо сейчас\n"
            "/subscribe — выбрать активы для автосигналов (раз в час)\n"
            "/unsubscribe — отключить всё\n"
            "/stats — реальная статистика бота (винрейт)\n"
            "/exchange — биржа для крипто-сигналов (Binance/Bybit)\n"
            "/language — сменить язык\n"
            "/help — как это работает"
        ),

        "help": (
            "🤖 *Как работает AlphaX Trade:*\n\n"
            "*Активы:*\n{assets_list}\n\n"
            "*6 технических индикаторов (1 балл каждый):*\n"
            "1. RSI — перекупленность/перепроданность\n"
            "2. MACD — направление импульса\n"
            "3. Bollinger Bands — цена на краю канала\n"
            "4. Тренд 4H — глобальное направление рынка (честный ресемплинг часовых свечей в 4-часовые)\n"
            "5. Объём — деньги подтверждают движение\n"
            "6. Stochastic — второе подтверждение перекупленности/перепроданности\n\n"
            "*📰 Новости (до 2 баллов):*\n"
            "Читаю CoinTelegraph, CoinDesk, Reuters, MarketWatch, Kitco.\n\n"
            "*📏 SL/TP от ATR:* стоп ставится на 1.5×ATR от входа, тейк — вдвое дальше "
            "(R:R 2:1 сохраняется). Это подстраивается под текущую волатильность каждого "
            "актива, а не фиксированный % как раньше.\n\n"
            "🕐 Каждый сигнал показывает время самих рыночных данных — с их родным "
            "смещением UTC, а не время нашего сервера.\n\n"
            "💱 Крипта (BTC, ETH, SOL) — данные с выбранной тобой биржи (/exchange). "
            "Золото, EUR/USD, NASDAQ, S&P 500, AAPL, TSLA — всегда Yahoo Finance.\n\n"
            "📊 *Статистика (/stats):* бот сам проверяет каждый автосигнал — дошёл ли он "
            "до тейка или до стопа — и честно показывает реальный винрейт, включая убытки.\n\n"
            "⚠️ *Важно:* «медвежий» сигнал — не «плохо», это просто направление рынка вниз.\n\n"
            "✅ {min_score}/{max_score} = Хороший сигнал\n"
            "💪 {near_max}/{max_score} = Очень хороший\n"
            "🔥 {max_score}/{max_score} = Сильный\n\n"
            "/signal — сигнал сейчас\n"
            "/subscribe — выбрать активы для автосигналов (раз в час)\n"
            "/unsubscribe — отключить всё\n"
            "/stats — статистика бота\n"
            "/exchange — биржа для крипты\n"
            "/language — сменить язык\n\n"
            "⚠️ Акции, NASDAQ и S&P 500 обновляются только в часы торгов биржи. "
            "Крипта и золото — почти круглосуточно.\n\n"
            "⚠️ Не является финансовым советом."
        ),

        "choose_asset": "Выбери актив:",
        "analyzing": "🔍 Анализирую рынок + читаю новости, 10-15 сек...",
        "error": "❌ Ошибка: {error}",

        "sub_menu_title": "Выбери активы, по которым хочешь получать автосигналы (раз в час). Нажимай, чтобы включить/выключить:",
        "btn_done": "✅ Готово",
        "sub_confirmed": "✅ Подписка обновлена!\n\nАктивов выбрано: {count}\nИзменить: /subscribe",
        "sub_none_selected": "Подписка снята — ничего не выбрано.\n\nВключить снова: /subscribe",
        "unsub_done": "❌ Все автосигналы отключены.\nВключить снова: /subscribe",

        "stats_title": "📊 *Статистика AlphaX Trade*",
        "stats_body": (
            "Всего закрытых сигналов: {total_closed}\n"
            "✅ Побед: {wins}\n"
            "❌ Убытков: {losses}\n"
            "📈 Винрейт: {win_rate}\n\n"
            "⏳ Сейчас открыто: {open}\n\n"
            "Статистика ведётся с момента запуска трекера — сигналы до этого не учитываются."
        ),
        "stats_no_data": "Статистика пока пустая — трекер только что запущен. Первые результаты появятся, как только один из открытых сигналов дойдёт до тейка или стопа.",
        "win_rate_na": "ещё нет закрытых сигналов",

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
        "lbl_atr": "📏 ATR (волатильность): `${atr}` — SL/TP считаются от неё",
        "lbl_reasons": "📋 *Почему этот сигнал:*",
        "lbl_rsi": "📈 RSI",
        "lbl_price": "💵 Цена",
        "lbl_confirmations": "Подтверждений",
        "lbl_news": "📰 Новости",
        "lbl_waiting": "⏳ Ждём чёткого сигнала...",
        "lbl_disclaimer": "⚠️ Не является финансовым советом",
        "lbl_signal_time": "🕐 Время котировки: {time}",

        "score_combined": "техника {tech}/6 + новости 📰 = *{total}/8*",
        "score_tech_only": "техника {tech}/6 = *{total}/8* (новости не в счёт)",
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
        "reason_stoch_oversold": "Stochastic = {stoch} (ниже 20 — перепродан, второе подтверждение разворота вверх)",
        "reason_stoch_overbought": "Stochastic = {stoch} (выше 80 — перекуплен, второе подтверждение разворота вниз)",
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
            "I analyze using *{max_score} factors*: 6 technical indicators "
            "+ news (weighted as 2 indicators).\n"
            "A signal fires only at *{min_score}/{max_score}* confirmations.\n\n"
            "SL/TP are computed dynamically from current volatility (ATR), not a fixed percent.\n\n"
            "📌 Commands:\n"
            "/signal — get a signal right now\n"
            "/subscribe — choose assets for auto-signals (hourly)\n"
            "/unsubscribe — turn everything off\n"
            "/stats — the bot's real track record (win rate)\n"
            "/exchange — exchange for crypto signals (Binance/Bybit)\n"
            "/language — change language\n"
            "/help — how it works"
        ),

        "help": (
            "🤖 *How AlphaX Trade works:*\n\n"
            "*Assets:*\n{assets_list}\n\n"
            "*6 technical indicators (1 point each):*\n"
            "1. RSI — overbought/oversold\n"
            "2. MACD — momentum direction\n"
            "3. Bollinger Bands — price at the edge of the channel\n"
            "4. 4H trend — overall market direction (real resampling of hourly candles into 4H)\n"
            "5. Volume — money confirming the move\n"
            "6. Stochastic — second confirmation of overbought/oversold\n\n"
            "*📰 News (up to 2 points):*\n"
            "I read CoinTelegraph, CoinDesk, Reuters, MarketWatch, Kitco.\n\n"
            "*📏 ATR-based SL/TP:* stop is placed 1.5×ATR from entry, target twice that "
            "distance (2:1 R:R kept). This adapts to each asset's current volatility "
            "instead of a fixed percent like before.\n\n"
            "🕐 Every signal shows the time of the actual market data — with its own "
            "UTC offset, not our server's time.\n\n"
            "💱 Crypto (BTC, ETH, SOL) — data from your chosen exchange (/exchange). "
            "Gold, EUR/USD, NASDAQ, S&P 500, AAPL, TSLA — always Yahoo Finance.\n\n"
            "📊 *Stats (/stats):* the bot checks every auto-signal itself — whether it hit "
            "take-profit or stop-loss — and shows the real win rate honestly, losses included.\n\n"
            "⚠️ *Important:* a «bearish» signal isn't «bad» — it's just a downward market direction.\n\n"
            "✅ {min_score}/{max_score} = Good signal\n"
            "💪 {near_max}/{max_score} = Very good\n"
            "🔥 {max_score}/{max_score} = Strong\n\n"
            "/signal — get a signal now\n"
            "/subscribe — choose assets for auto-signals (hourly)\n"
            "/unsubscribe — turn everything off\n"
            "/stats — bot's stats\n"
            "/exchange — crypto data exchange\n"
            "/language — change language\n\n"
            "⚠️ Stocks, NASDAQ and S&P 500 only update during exchange trading hours. "
            "Crypto and gold — nearly 24/7.\n\n"
            "⚠️ Not financial advice."
        ),

        "choose_asset": "Choose an asset:",
        "analyzing": "🔍 Analyzing the market + reading the news, 10-15 sec...",
        "error": "❌ Error: {error}",

        "sub_menu_title": "Choose which assets you want hourly auto-signals for. Tap to toggle on/off:",
        "btn_done": "✅ Done",
        "sub_confirmed": "✅ Subscriptions updated!\n\nAssets selected: {count}\nChange: /subscribe",
        "sub_none_selected": "Subscription cleared — nothing selected.\n\nTurn back on: /subscribe",
        "unsub_done": "❌ All auto-signals turned off.\nTurn back on: /subscribe",

        "stats_title": "📊 *AlphaX Trade Stats*",
        "stats_body": (
            "Total closed signals: {total_closed}\n"
            "✅ Wins: {wins}\n"
            "❌ Losses: {losses}\n"
            "📈 Win rate: {win_rate}\n\n"
            "⏳ Currently open: {open}\n\n"
            "Tracking started when this update went live — earlier signals aren't included."
        ),
        "stats_no_data": "No stats yet — the tracker just started. First results will show up once an open signal hits take-profit or stop-loss.",
        "win_rate_na": "no closed signals yet",

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
        "lbl_atr": "📏 ATR (volatility): `${atr}` — SL/TP are based on this",
        "lbl_reasons": "📋 *Why this signal:*",
        "lbl_rsi": "📈 RSI",
        "lbl_price": "💵 Price",
        "lbl_confirmations": "Confirmations",
        "lbl_news": "📰 News",
        "lbl_waiting": "⏳ Waiting for a clear signal...",
        "lbl_disclaimer": "⚠️ Not financial advice",
        "lbl_signal_time": "🕐 Quote time: {time}",

        "score_combined": "technicals {tech}/6 + news 📰 = *{total}/8*",
        "score_tech_only": "technicals {tech}/6 = *{total}/8* (news not counted)",
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
        "reason_stoch_oversold": "Stochastic = {stoch} (below 20 — oversold, second confirmation of a reversal up)",
        "reason_stoch_overbought": "Stochastic = {stoch} (above 80 — overbought, second confirmation of a reversal down)",
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
NEWS_TTL = 1800

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
    return df.set_index("ts")[["high", "low", "close", "volume"]]

def _fetch_yahoo_ohlcv(cfg: dict) -> pd.DataFrame:
    period = PERIOD_BY_CLASS.get(cfg["class"], "7d")
    df = yf.Ticker(cfg["ticker"]).history(period=period, interval="1h")
    df.columns = [c.lower() for c in df.columns]
    return df[["high", "low", "close", "volume"]]

def _fetch_ohlcv_dataframe(cfg: dict, exchange_pref: str = None) -> pd.DataFrame:
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

def _get_stoch_k(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    st = ta.stoch(high, low, close)
    if st is None or st.empty:
        return pd.Series([50.0] * len(close), index=close.index)
    k_col = [c for c in st.columns if c.startswith("STOCHk")][0]
    return st[k_col]

def _resample_4h_trend(df: pd.DataFrame) -> bool:
    """Честный ресемплинг часовых свечей в 4-часовые для тренда, без доп. запросов."""
    df_4h = df["close"].resample("4h").last().dropna()
    if len(df_4h) < 20:
        e20 = ta.ema(df["close"], 20)
        e50 = ta.ema(df["close"], 50)
        return bool(e20.iloc[-1] > e50.iloc[-1])
    e20 = ta.ema(df_4h, 20)
    e50 = ta.ema(df_4h, 50)
    return bool(e20.iloc[-1] > e50.iloc[-1])

def _score_technical(rsi, macd, macd_s, price, bbu, bbl, trend_bull, vol_ratio, stoch_k):
    """Каждый индикатор возвращает (ключ_шаблона, параметры), а не готовый текст."""
    bs, ss = 0, 0
    br, sr = [], []

    if rsi < 30:
        bs += 1; br.append(("reason_rsi_oversold", {"rsi": rsi}))
    elif rsi > 70:
        ss += 1; sr.append(("reason_rsi_overbought", {"rsi": rsi}))

    if macd > macd_s:
        bs += 1; br.append(("reason_macd_bull", {}))
    else:
        ss += 1; sr.append(("reason_macd_bear", {}))

    if price <= bbl * 1.005:
        bs += 1; br.append(("reason_bb_lower", {}))
    elif price >= bbu * 0.995:
        ss += 1; sr.append(("reason_bb_upper", {}))

    if trend_bull:
        bs += 1; br.append(("reason_trend_up", {}))
    else:
        ss += 1; sr.append(("reason_trend_down", {}))

    if vol_ratio >= 1.5:
        if bs >= ss:
            bs += 1; br.append(("reason_vol_bull", {"ratio": round(vol_ratio, 1)}))
        else:
            ss += 1; sr.append(("reason_vol_bear", {"ratio": round(vol_ratio, 1)}))

    if stoch_k < 20:
        bs += 1; br.append(("reason_stoch_oversold", {"stoch": stoch_k}))
    elif stoch_k > 80:
        ss += 1; sr.append(("reason_stoch_overbought", {"stoch": stoch_k}))

    return bs, ss, br, sr

def _analyze(asset_key: str, exchange_pref: str = None) -> dict:
    """
    Анализ актива: 6 технических индикаторов + новости (вес x2, максимум 8 баллов).
    SL/TP считаются от ATR, а не от фиксированного процента.
    Результат полностью язык-независим — рендер под конкретный язык делает fmt().
    """
    cfg = ASSETS[asset_key]
    display_symbol = cfg["name"]

    df = _fetch_ohlcv_dataframe(cfg, exchange_pref)
    if df.empty:
        raise ValueError(f"Нет данных: {display_symbol}")

    bar_time = df.index[-1]
    trend = _resample_4h_trend(df)

    df["rsi"] = ta.rsi(df["close"], 14)
    m = ta.macd(df["close"])
    df["macd"] = m["MACD_12_26_9"]
    df["ms"] = m["MACDs_12_26_9"]
    df["bbu"], df["bbl"] = _get_bbands(df["close"])
    df["vm"] = ta.sma(df["volume"], 20)
    df["stoch_k"] = _get_stoch_k(df["high"], df["low"], df["close"])
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)

    last = df.iloc[-1]
    p = float(last["close"])
    rsi = round(float(last["rsi"]), 2)
    vm = float(last["vm"]) or 1.0
    vr = float(last["volume"]) / vm
    stoch_k = round(float(last["stoch_k"]), 1)
    current_atr = float(last["atr"])

    bs, ss, br, sr = _score_technical(
        rsi, float(last["macd"]), float(last["ms"]),
        p, float(last["bbu"]), float(last["bbl"]), trend, vr, stoch_k
    )
    tech_bs, tech_ss = bs, ss

    news_score, news_key, news_params = _fetch_news(display_symbol, cfg["class"])
    if news_score > 0:
        bs += NEWS_WEIGHT
    elif news_score < 0:
        ss += NEWS_WEIGHT

    sl_distance = current_atr * ATR_SL_MULTIPLIER
    tp_distance = sl_distance * 2  # R:R 2:1, как и раньше

    common = {
        "symbol": display_symbol,
        "rsi": rsi,
        "entry": round(p, 2),
        "atr": round(current_atr, 4),
        "news_key": news_key,
        "news_params": news_params,
        "data_time": bar_time,
    }

    if bs >= MIN_SCORE and bs > ss:
        return {
            **common, "direction": "LONG",
            "total_score": bs, "tech_score": tech_bs,
            "take_profit": round(p + tp_distance, 2),
            "stop_loss": round(p - sl_distance, 2),
            "reasons": br,
            "news_conflicts": news_score < 0,
            "news_helped": news_score > 0,
        }

    if ss >= MIN_SCORE and ss > bs:
        return {
            **common, "direction": "SHORT",
            "total_score": ss, "tech_score": tech_ss,
            "take_profit": round(p - tp_distance, 2),
            "stop_loss": round(p + sl_distance, 2),
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

def _format_market_time(ts) -> str:
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
        atr_line = t["lbl_atr"].format(atr=data["atr"])

        return (
            f"{header}"
            f"{t['lbl_direction']}: {direction_label}\n"
            f"{t['lbl_strength']}: {strength} — {score_line}\n\n"
            f"{t['lbl_entry']}: `${data['entry']}`\n"
            f"{t['lbl_tp']}: `${data['take_profit']}`\n"
            f"{t['lbl_sl']}: `${data['stop_loss']}`\n"
            f"{atr_line}\n\n"
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
# ТРЕКЕР ВИНРЕЙТА
# ================================================================

async def check_open_signals():
    open_signals = get_open_tracked_signals()
    if not open_signals:
        return

    for sig in open_signals:
        cfg = ASSETS.get(sig["asset_key"])
        if not cfg:
            continue
        try:
            df = await asyncio.to_thread(_fetch_ohlcv_dataframe, cfg, DEFAULT_EXCHANGE)
            current_price = float(df.iloc[-1]["close"])
        except Exception as e:
            logger.error(f"Не смог проверить цену {sig['asset_key']}: {e}")
            continue

        if sig["direction"] == "LONG":
            if current_price >= sig["take_profit"]:
                resolve_tracked_signal(sig["id"], "win", current_price)
            elif current_price <= sig["stop_loss"]:
                resolve_tracked_signal(sig["id"], "loss", current_price)
        else:
            if current_price <= sig["take_profit"]:
                resolve_tracked_signal(sig["id"], "win", current_price)
            elif current_price >= sig["stop_loss"]:
                resolve_tracked_signal(sig["id"], "loss", current_price)

# ================================================================
# ВЫБОР ЯЗЫКА / БИРЖИ / ПОДПИСОК
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

def _subscription_keyboard(subscribed: set, lang: str) -> InlineKeyboardMarkup:
    buttons = []
    for key, cfg in ASSETS.items():
        mark = "☑️" if key in subscribed else "⬜"
        buttons.append(InlineKeyboardButton(f"{mark} {cfg['emoji']} {cfg['name']}", callback_data=f"subtoggle_{key}"))
    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    rows.append([InlineKeyboardButton(LANG[lang]["btn_done"], callback_data="subdone")])
    return InlineKeyboardMarkup(rows)

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

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_language(update.effective_user.id) or "ru"
    cid = update.message.chat_id
    subscribed = get_user_subscriptions(cid)
    await update.message.reply_text(
        LANG[lang]["sub_menu_title"],
        reply_markup=_subscription_keyboard(subscribed, lang)
    )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    lang = get_language(update.effective_user.id) or "ru"
    remove_all_subscriptions(cid)
    await update.message.reply_text(LANG[lang]["unsub_done"])

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_language(update.effective_user.id) or "ru"
    s = get_tracker_stats()
    if s["total_closed"] == 0 and s["open"] == 0:
        await update.message.reply_text(LANG[lang]["stats_no_data"])
        return
    win_rate_str = f"{s['win_rate']}%" if s["win_rate"] is not None else LANG[lang]["win_rate_na"]
    text = LANG[lang]["stats_title"] + "\n\n" + LANG[lang]["stats_body"].format(
        total_closed=s["total_closed"], wins=s["wins"], losses=s["losses"],
        win_rate=win_rate_str, open=s["open"]
    )
    await update.message.reply_text(text, parse_mode="Markdown")

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

    if q.data.startswith("subtoggle_"):
        asset_key = q.data.removeprefix("subtoggle_")
        if asset_key not in ASSETS:
            return
        cid = q.message.chat_id
        toggle_subscription(cid, asset_key)
        lang = get_language(q.from_user.id) or "ru"
        subscribed = get_user_subscriptions(cid)
        await q.edit_message_reply_markup(reply_markup=_subscription_keyboard(subscribed, lang))
        return

    if q.data == "subdone":
        lang = get_language(q.from_user.id) or "ru"
        cid = q.message.chat_id
        subscribed = get_user_subscriptions(cid)
        if subscribed:
            await q.edit_message_text(LANG[lang]["sub_confirmed"].format(count=len(subscribed)))
        else:
            await q.edit_message_text(LANG[lang]["sub_none_selected"])
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
        f"👥 Уникальных подписчиков: *{n}*\n"
        f"📈 Активов в анализе: *{len(ASSETS)}*",
        parse_mode="Markdown"
    )

# ================================================================
# АВТОСИГНАЛЫ — раз в час, с учётом подписки по конкретным активам
# ================================================================

async def send_auto_signals(context: ContextTypes.DEFAULT_TYPE):
    await check_open_signals()

    # Не-крипто активы считаем один раз — не зависят от биржи пользователя
    non_crypto_signals = {}
    for key, cfg in ASSETS.items():
        if cfg["class"] == "crypto":
            continue
        try:
            data = await asyncio.to_thread(_analyze, key)
            if data["direction"] != "WAIT":
                non_crypto_signals[key] = data
                logger.info(f"{cfg['name']}: {data['direction']} ({data['total_score']}/{MAX_SCORE})")
                if not has_open_signal(key):
                    add_tracked_signal(key, data["symbol"], data["direction"],
                                        data["entry"], data["take_profit"], data["stop_loss"])
            else:
                logger.info(f"{cfg['name']}: WAIT — пропущен")
        except Exception as e:
            logger.error(f"Ошибка {cfg['name']}: {e}")
        await asyncio.sleep(1)

    # Для крипты нужны только те биржи, что реально выбраны подписчиками крипто-активов
    crypto_keys = [k for k, c in ASSETS.items() if c["class"] == "crypto"]
    subscriber_exchanges = set()
    for key in crypto_keys:
        for cid in get_subscribers_for_asset(key):
            subscriber_exchanges.add(get_crypto_exchange(cid) or DEFAULT_EXCHANGE)
    needed_exchanges = subscriber_exchanges or {DEFAULT_EXCHANGE}  # трекер винрейта работает даже без подписчиков

    crypto_signals_by_exch = {}
    for exch in needed_exchanges:
        signals = {}
        for key in crypto_keys:
            cfg = ASSETS[key]
            try:
                data = await asyncio.to_thread(_analyze, key, exch)
                if data["direction"] != "WAIT":
                    signals[key] = data
                    logger.info(f"{cfg['name']} [{exch}]: {data['direction']} ({data['total_score']}/{MAX_SCORE})")
                    if exch == DEFAULT_EXCHANGE and not has_open_signal(key):
                        add_tracked_signal(key, data["symbol"], data["direction"],
                                            data["entry"], data["take_profit"], data["stop_loss"])
                else:
                    logger.info(f"{cfg['name']} [{exch}]: WAIT — пропущен")
            except Exception as e:
                logger.error(f"Ошибка {cfg['name']} [{exch}]: {e}")
            await asyncio.sleep(1)
        crypto_signals_by_exch[exch] = signals

    all_chat_ids = get_all_subscribed_chat_ids()
    if not all_chat_ids:
        logger.info("Нет подписчиков — рассылка пропущена (проверка сигналов уже прошла)")
        return

    dead = set()
    for cid in all_chat_ids:
        lang = get_language(cid) or "ru"
        my_assets = get_user_subscriptions(cid)
        exch = get_crypto_exchange(cid) or DEFAULT_EXCHANGE
        to_send = []
        for key in my_assets:
            cfg = ASSETS.get(key)
            if not cfg:
                continue
            if cfg["class"] == "crypto":
                d = crypto_signals_by_exch.get(exch, {}).get(key)
            else:
                d = non_crypto_signals.get(key)
            if d:
                to_send.append(d)
        for d in to_send:
            try:
                await context.bot.send_message(cid, fmt(d, lang, is_auto=True), parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Ошибка отправки {cid}: {e}")
                dead.add(cid)
    for cid in dead:
        remove_all_subscriptions(cid)

# ================================================================
# ЗАПУСК
# ================================================================

def main():
    init_storage()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("signal",      signal_command))
    app.add_handler(CommandHandler("subscribe",   subscribe_command))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("stats",       stats_command))
    app.add_handler(CommandHandler("help",        help_command))
    app.add_handler(CommandHandler("language",    language_command))
    app.add_handler(CommandHandler("exchange",    exchange_command))
    app.add_handler(CommandHandler("users",       users_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.job_queue.run_repeating(send_auto_signals, interval=3600, first=10)  # раз в час

    mode = "PostgreSQL ☁️" if DATABASE_URL else "JSON (локально)"
    print(f"✅ AlphaX Trade запущен! Активов: {len(ASSETS)}. Хранилище: {mode}")
    logger.info(f"Бот запущен. Активов: {len(ASSETS)}. Хранилище: {mode}")
    app.run_polling()

if __name__ == "__main__":
    main()