import os
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, PreCheckoutQueryHandler, filters, ContextTypes
)
import pandas as pd
import pandas_ta as ta
import yfinance as yf

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
KASPI_PHONE  = os.getenv("KASPI_PHONE")
KASPI_NAME   = os.getenv("KASPI_NAME")
DATABASE_URL = os.getenv("DATABASE_URL")

PREMIUM_DAYS  = 30
PREMIUM_STARS = 500

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
                    CREATE TABLE IF NOT EXISTS subscribers (
                        chat_id BIGINT PRIMARY KEY
                    );
                    CREATE TABLE IF NOT EXISTS premium_users (
                        user_id BIGINT PRIMARY KEY,
                        expiry TIMESTAMPTZ NOT NULL,
                        granted_at TIMESTAMPTZ DEFAULT NOW(),
                        days INT DEFAULT 30,
                        method TEXT DEFAULT 'manual'
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

    def is_premium(user_id: int) -> bool:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM premium_users WHERE user_id=%s AND expiry>NOW()",
                    (user_id,)
                )
                return cur.fetchone() is not None

    def add_premium(user_id: int, days: int, method: str = "manual") -> str:
        expiry = datetime.now() + timedelta(days=days)
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    INSERT INTO premium_users (user_id, expiry, days, method)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        expiry=EXCLUDED.expiry, granted_at=NOW(),
                        days=EXCLUDED.days, method=EXCLUDED.method
                """, (user_id, expiry, days, method))
            c.commit()
        return expiry.strftime("%d.%m.%Y")

    def remove_premium(user_id: int):
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM premium_users WHERE user_id=%s", (user_id,))
            c.commit()

    def get_expiry(user_id: int) -> str:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT expiry FROM premium_users WHERE user_id=%s", (user_id,))
                row = cur.fetchone()
                return row[0].strftime("%d.%m.%Y") if row else "—"

    def get_stats() -> dict:
        now = datetime.now()
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM subscribers")
                subs = cur.fetchone()[0]
                cur.execute(
                    "SELECT user_id, expiry, method FROM premium_users WHERE expiry>NOW() ORDER BY expiry"
                )
                rows = cur.fetchall()
        premium = []
        for uid, expiry, method in rows:
            days_left = (expiry.replace(tzinfo=None) - now).days
            premium.append({
                "user_id": uid,
                "expiry_str": expiry.strftime("%d.%m.%Y"),
                "days_left": days_left,
                "method": method or "—"
            })
        return {"subs": subs, "premium": premium}

else:
    SUBS_FILE = "subscribers.json"
    PREM_FILE = "premium.json"

    def init_storage(): pass

    def _load_j(f, default):
        return json.load(open(f)) if os.path.exists(f) else default

    def _save_j(f, d):
        with open(f, "w") as fp:
            json.dump(d, fp, ensure_ascii=False, indent=2)

    def get_subscribers() -> set:
        return set(_load_j(SUBS_FILE, []))

    def add_subscriber(chat_id: int):
        s = get_subscribers(); s.add(chat_id); _save_j(SUBS_FILE, list(s))

    def remove_subscriber(chat_id: int):
        s = get_subscribers(); s.discard(chat_id); _save_j(SUBS_FILE, list(s))

    def is_premium(user_id: int) -> bool:
        d = _load_j(PREM_FILE, {})
        k = str(user_id)
        return k in d and datetime.now() < datetime.fromisoformat(d[k]["expiry"])

    def add_premium(user_id: int, days: int, method: str = "manual") -> str:
        d = _load_j(PREM_FILE, {})
        expiry = datetime.now() + timedelta(days=days)
        d[str(user_id)] = {
            "expiry": expiry.isoformat(),
            "granted_at": datetime.now().isoformat(),
            "days": days, "method": method
        }
        _save_j(PREM_FILE, d)
        return expiry.strftime("%d.%m.%Y")

    def remove_premium(user_id: int):
        d = _load_j(PREM_FILE, {}); d.pop(str(user_id), None); _save_j(PREM_FILE, d)

    def get_expiry(user_id: int) -> str:
        d = _load_j(PREM_FILE, {})
        k = str(user_id)
        return datetime.fromisoformat(d[k]["expiry"]).strftime("%d.%m.%Y") if k in d else "—"

    def get_stats() -> dict:
        d = _load_j(PREM_FILE, {})
        now = datetime.now()
        premium = []
        for uid, v in d.items():
            expiry = datetime.fromisoformat(v["expiry"])
            if expiry > now:
                premium.append({
                    "user_id": uid,
                    "expiry_str": expiry.strftime("%d.%m.%Y"),
                    "days_left": (expiry - now).days,
                    "method": v.get("method", "—")
                })
        return {"subs": len(get_subscribers()), "premium": premium}

# ================================================================
# АНАЛИЗ РЫНКА — Yahoo Finance (работает с US серверов)
# ================================================================

def get_bbands(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    bb = ta.bbands(series, length=20, std=2)
    if bb is None or bb.empty:
        return series * 1.02, series * 0.98
    u = [c for c in bb.columns if c.startswith("BBU")][0]
    l = [c for c in bb.columns if c.startswith("BBL")][0]
    return bb[u], bb[l]

def score_signal(rsi, macd, macd_signal, price, bb_upper, bb_lower, trend_bullish, vol_ratio):
    bs, ss, br, sr = 0, 0, [], []
    if rsi < 30:
        bs += 1; br.append(f"RSI перепродан ({rsi})")
    elif rsi > 70:
        ss += 1; sr.append(f"RSI перекуплен ({rsi})")
    if macd > macd_signal:
        bs += 1; br.append("MACD бычий импульс")
    else:
        ss += 1; sr.append("MACD медвежий импульс")
    if price <= bb_lower * 1.005:
        bs += 1; br.append("Цена у нижней BB (зона покупки)")
    elif price >= bb_upper * 0.995:
        ss += 1; sr.append("Цена у верхней BB (зона продажи)")
    if trend_bullish:
        bs += 1; br.append("4H тренд восходящий (EMA20 > EMA50)")
    else:
        ss += 1; sr.append("4H тренд нисходящий (EMA20 < EMA50)")
    if vol_ratio >= 1.5:
        if bs >= ss:
            bs += 1; br.append(f"Объём подтверждает рост (×{round(vol_ratio,1)})")
        else:
            ss += 1; sr.append(f"Объём подтверждает падение (×{round(vol_ratio,1)})")
    return bs, ss, br, sr

def build_result(symbol, direction, score, price, tp_pct, sl_pct, rsi, reasons):
    strength = "🔥 СИЛЬНЫЙ" if score == 5 else "✅ ХОРОШИЙ"
    if direction == "LONG":
        return {
            "symbol": symbol, "direction": "🟢 ПОКУПАЙ (LONG)",
            "strength": strength, "score": f"{score}/5",
            "entry": round(price, 2),
            "take_profit": round(price * (1 + tp_pct), 2),
            "stop_loss": round(price * (1 - sl_pct), 2),
            "rsi": rsi, "reasons": reasons
        }
    return {
        "symbol": symbol, "direction": "🔴 ПРОДАВАЙ (SHORT)",
        "strength": strength, "score": f"{score}/5",
        "entry": round(price, 2),
        "take_profit": round(price * (1 - tp_pct), 2),
        "stop_loss": round(price * (1 + sl_pct), 2),
        "rsi": rsi, "reasons": reasons
    }

def wait_result(symbol, score, price, rsi):
    return {
        "symbol": symbol, "direction": "⚪️ ЖДАТЬ",
        "strength": "—", "score": f"{score}/5",
        "entry": round(price, 2),
        "take_profit": None, "stop_loss": None,
        "rsi": rsi,
        "reasons": ["Недостаточно подтверждений — ждём лучшей точки входа"]
    }

def _analyze_yf(ticker_symbol: str, display_symbol: str, tp_pct: float, sl_pct: float) -> dict:
    """Один запрос вместо двух — в 2 раза быстрее."""
    ticker = yf.Ticker(ticker_symbol)

    # Один запрос — 7 дней хватает для EMA50, RSI, MACD, BB
    df = ticker.history(period="7d", interval="1h")
    if df.empty:
        raise ValueError(f"Нет данных по {display_symbol}")
    df.columns = [c.lower() for c in df.columns]

    # Тренд на тех же данных
    df["e20"] = ta.ema(df["close"], length=20)
    df["e50"] = ta.ema(df["close"], length=50)
    trend = bool(df.iloc[-1]["e20"] > df.iloc[-1]["e50"])

    # Сигналы
    df["rsi"] = ta.rsi(df["close"], 14)
    m = ta.macd(df["close"])
    df["macd"] = m["MACD_12_26_9"]
    df["ms"]   = m["MACDs_12_26_9"]
    df["bbu"], df["bbl"] = get_bbands(df["close"])
    df["vm"] = ta.sma(df["volume"], 20)

    last = df.iloc[-1]
    p   = float(last["close"])
    rsi = round(float(last["rsi"]), 2)
    vm  = float(last["vm"]) if float(last["vm"]) > 0 else 1.0
    vr  = float(last["volume"]) / vm

    bs, ss, br, sr = score_signal(
        rsi, float(last["macd"]), float(last["ms"]),
        p, float(last["bbu"]), float(last["bbl"]), trend, vr
    )
    if bs >= 4: return build_result(display_symbol, "LONG",  bs, p, tp_pct, sl_pct, rsi, br)
    if ss >= 4: return build_result(display_symbol, "SHORT", ss, p, tp_pct, sl_pct, rsi, sr)
    return wait_result(display_symbol, max(bs, ss), p, rsi)

    # Сигналы на 1H
    df = ticker.history(period="5d", interval="1h")
    if df.empty:
        raise ValueError(f"Нет данных по {display_symbol} (1H)")
    df.columns = [c.lower() for c in df.columns]
    df["rsi"] = ta.rsi(df["close"], 14)
    m = ta.macd(df["close"])
    df["macd"] = m["MACD_12_26_9"]
    df["ms"]   = m["MACDs_12_26_9"]
    df["bbu"], df["bbl"] = get_bbands(df["close"])
    df["vm"] = ta.sma(df["volume"], 20)

    last = df.iloc[-1]
    p   = float(last["close"])
    rsi = round(float(last["rsi"]), 2)
    vm  = float(last["vm"]) if float(last["vm"]) > 0 else 1.0
    vr  = float(last["volume"]) / vm

    bs, ss, br, sr = score_signal(
        rsi, float(last["macd"]), float(last["ms"]),
        p, float(last["bbu"]), float(last["bbl"]), trend, vr
    )
    if bs >= 4: return build_result(display_symbol, "LONG",  bs, p, tp_pct, sl_pct, rsi, br)
    if ss >= 4: return build_result(display_symbol, "SHORT", ss, p, tp_pct, sl_pct, rsi, sr)
    return wait_result(display_symbol, max(bs, ss), p, rsi)

def get_signal_btc() -> dict:
    """BTC через Yahoo Finance — BTC-USD."""
    return _analyze_yf("BTC-USD", "BTC/USD", tp_pct=0.04, sl_pct=0.02)

def get_signal_gold() -> dict:
    """Gold через Yahoo Finance — GC=F."""
    return _analyze_yf("GC=F", "XAUUSD", tp_pct=0.015, sl_pct=0.0075)

# ================================================================
# ФОРМАТИРОВАНИЕ
# ================================================================

def format_free(data: dict) -> str:
    symbol = data["symbol"]
    if data["take_profit"]:
        return (
            f"📊 *Сигнал {symbol}*\n\n"
            f"Направление: {data['direction']}\n"
            f"Сила сигнала: {data['strength']} `({data['score']})`\n\n"
            f"📍 Точка входа: `🔒 Premium`\n"
            f"🎯 Тейк-профит: `🔒 Premium`\n"
            f"🛡 Стоп-лосс: `🔒 Premium`\n\n"
            f"📈 RSI: `{data['rsi']}`\n\n"
            f"💡 Сигнал найден — точки входа только для Premium.\n"
            f"/buy — оформить за 4 990 ₸/мес"
        )
    return (
        f"📊 *Сигнал {symbol}*\n\n"
        f"Направление: {data['direction']}\n"
        f"Подтверждений: `{data['score']}` — нужно минимум 4/5\n\n"
        f"💵 Цена: `${data['entry']}`\n"
        f"📈 RSI: `{data['rsi']}`\n\n"
        f"⏳ Ждём чёткого сигнала...\n\n"
        f"⚠️ Не является финансовым советом"
    )

def format_premium(data: dict, is_auto: bool = False) -> str:
    symbol = data["symbol"]
    header = f"🔔 *Автосигнал {symbol}*\n\n" if is_auto else f"📊 *Сигнал {symbol}*\n\n"
    if data["take_profit"]:
        reasons = "\n".join([f"  ✅ {r}" for r in data["reasons"]])
        return (
            f"{header}"
            f"Направление: {data['direction']}\n"
            f"Сила: {data['strength']} `({data['score']})`\n\n"
            f"📍 Точка входа: `${data['entry']}`\n"
            f"🎯 Тейк-профит: `${data['take_profit']}`\n"
            f"🛡 Стоп-лосс: `${data['stop_loss']}`\n\n"
            f"📋 *Подтверждения:*\n{reasons}\n\n"
            f"📈 RSI: `{data['rsi']}`\n\n"
            f"⚠️ Не является финансовым советом"
        )
    return (
        f"{header}"
        f"Направление: {data['direction']}\n"
        f"Подтверждений: `{data['score']}` — нужно 4/5\n\n"
        f"💵 Цена: `${data['entry']}`\n"
        f"📈 RSI: `{data['rsi']}`\n\n"
        f"⏳ {data['reasons'][0]}\n\n"
        f"⚠️ Не является финансовым советом"
    )

# ================================================================
# КОМАНДЫ ПОЛЬЗОВАТЕЛЯ
# ================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    status = "💎 Premium" if is_premium(user.id) else "👤 Free"
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n"
        f"Статус: {status}\n\n"
        f"🧠 Анализирую рынок по *5 индикаторам*.\n"
        f"Сигнал только при *4/5 подтверждениях*.\n\n"
        f"📌 Команды:\n"
        f"/signal — получить сигнал\n"
        f"/subscribe — автосигналы каждые 4 часа\n"
        f"/buy — Premium подписка\n"
        f"/mystatus — мой статус\n"
        f"/help — помощь",
        parse_mode="Markdown"
    )

async def mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_premium(user.id):
        await update.message.reply_text(
            f"💎 *Premium активен*\n\nДо: `{get_expiry(user.id)}`",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"👤 *Free тариф*\n\nОформи Premium: /buy",
            parse_mode="Markdown"
        )

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[
        InlineKeyboardButton("₿ BTC/USD", callback_data="sig_btc"),
        InlineKeyboardButton("🥇 XAUUSD", callback_data="sig_gold"),
    ]]
    await update.message.reply_text(
        "Выбери актив для анализа:",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    if cid in get_subscribers():
        await update.message.reply_text("✅ Ты уже подписан на автосигналы!")
    else:
        add_subscriber(cid)
        await update.message.reply_text(
            "✅ *Автосигналы активированы!*\n\n"
            "BTC и Золото каждые 4 часа.\n"
            "Слабые сигналы пропускаю — только 4/5 и выше.\n\n"
            "Отключить: /unsubscribe",
            parse_mode="Markdown"
        )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.message.chat_id
    if cid in get_subscribers():
        remove_subscriber(cid)
        await update.message.reply_text("❌ Автосигналы отключены. Вернуться: /subscribe")
    else:
        await update.message.reply_text("Ты не был подписан.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Команды:*\n\n"
        "/signal — сигнал сейчас\n"
        "/subscribe — автосигналы каждые 4ч\n"
        "/unsubscribe — отключить\n"
        "/buy — Premium подписка\n"
        "/mystatus — мой статус\n\n"
        "⚠️ Не является финансовым советом.",
        parse_mode="Markdown"
    )

async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_premium(user.id):
        await update.message.reply_text(
            f"💎 Premium активен до `{get_expiry(user.id)}`!",
            parse_mode="Markdown"
        )
        return
    kb = [
        [InlineKeyboardButton("⭐ Telegram Stars (~$9.9)", callback_data="pay_stars")],
        [InlineKeyboardButton("💳 Kaspi (4 990 ₸)", callback_data="pay_kaspi")],
    ]
    await update.message.reply_text(
        f"💎 *AlphaX Trade Premium — 30 дней*\n\n"
        f"✅ Точки входа в каждой сделке\n"
        f"✅ Тейк-профит и стоп-лосс\n"
        f"✅ 5-индикаторный анализ\n"
        f"✅ Автосигналы BTC + XAUUSD 24/7\n\n"
        f"Выбери способ оплаты:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )

# ================================================================
# ОБРАБОТЧИК КНОПОК
# ================================================================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data in ("sig_btc", "sig_gold"):
        await query.edit_message_text("🔍 Анализирую рынок по 5 индикаторам...")
        try:
            data = get_signal_btc() if query.data == "sig_btc" else get_signal_gold()
            text = format_premium(data) if is_premium(query.from_user.id) else format_free(data)
            await query.edit_message_text(text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка сигнала: {e}")
            await query.edit_message_text(f"❌ Ошибка: {str(e)}")

    elif query.data == "pay_stars":
        await query.message.delete()
        await context.bot.send_invoice(
            chat_id=query.from_user.id,
            title="AlphaX Trade Premium",
            description=f"Полный доступ к сигналам на {PREMIUM_DAYS} дней",
            payload="premium_30_days",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Premium 30 дней", PREMIUM_STARS)]
        )

    elif query.data == "pay_kaspi":
        user = query.from_user
        context.user_data["waiting_kaspi"] = True
        await query.edit_message_text(
            f"💳 *Оплата через Kaspi*\n\n"
            f"1️⃣ Открой Kaspi → Переводы\n"
            f"2️⃣ Переведи `4 990 ₸` на номер:\n"
            f"📱 `{KASPI_PHONE}` ({KASPI_NAME})\n"
            f"3️⃣ В комментарии напиши свой ID:\n"
            f"🆔 `{user.id}`\n"
            f"4️⃣ Отправь скриншот сюда 👇\n\n"
            f"⚡️ Открываем доступ за 15 минут",
            parse_mode="Markdown"
        )

# ================================================================
# TELEGRAM STARS
# ================================================================

async def precheckout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    expiry_str = add_premium(user.id, PREMIUM_DAYS, method="telegram_stars")
    logger.info(f"Stars оплата: {user.id} — до {expiry_str}")
    try:
        username = f"@{user.username}" if user.username else "без username"
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"💰 *Новая Stars оплата!*\n\n"
                f"👤 {user.first_name} ({username})\n"
                f"🆔 `{user.id}`\n"
                f"До: {expiry_str}"
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить админа: {e}")
    await update.message.reply_text(
        f"🎉 *Premium активирован!*\n\nДо: `{expiry_str}`\n\nПопробуй: /signal",
        parse_mode="Markdown"
    )

# ================================================================
# KASPI
# ================================================================

async def screenshot_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_kaspi"):
        return
    context.user_data["waiting_kaspi"] = False
    user = update.effective_user
    username = f"@{user.username}" if user.username else "без username"
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"💳 *Kaspi оплата!*\n\n"
                f"👤 {user.first_name} ({username})\n"
                f"🆔 `{user.id}`\n\n"
                f"Выдать доступ: `/approve {user.id}`"
            ),
            parse_mode="Markdown"
        )
        await context.bot.forward_message(
            ADMIN_ID, update.message.chat_id, update.message.message_id
        )
        await update.message.reply_text("✅ Скриншот получен! Открываем доступ за 15 минут 🎉")
        logger.info(f"Kaspi чек от {user.id} ({username})")
    except Exception as e:
        logger.error(f"Ошибка скриншота: {e}")
        await update.message.reply_text("❌ Ошибка. Попробуй /buy ещё раз.")

# ================================================================
# КОМАНДЫ АДМИНИСТРАТОРА
# ================================================================

async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Использование: `/approve USER_ID [дней]`", parse_mode="Markdown")
        return
    try:
        uid = int(context.args[0])
        days = int(context.args[1]) if len(context.args) > 1 else PREMIUM_DAYS
        expiry_str = add_premium(uid, days, method="kaspi")
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"🎉 *Premium активирован!*\n\nДо: `{expiry_str}`\n\nПопробуй: /signal",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не смог уведомить {uid}: {e}")
        await update.message.reply_text(
            f"✅ Premium выдан `{uid}` до {expiry_str}", parse_mode="Markdown"
        )
        logger.info(f"Admin approve: {uid} на {days} дней")
    except ValueError:
        await update.message.reply_text("❌ Пример: `/approve 123456789`", parse_mode="Markdown")

async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Использование: `/revoke USER_ID`", parse_mode="Markdown")
        return
    remove_premium(int(context.args[0]))
    await update.message.reply_text(f"✅ Premium отозван у {context.args[0]}")

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    stats = get_stats()
    text = (
        f"📊 *Статистика*\n\n"
        f"👥 Подписчиков: {stats['subs']}\n"
        f"💎 Premium активных: {len(stats['premium'])}\n\n"
    )
    if stats["premium"]:
        text += "*Premium пользователи:*\n"
        for p in stats["premium"]:
            text += f"• `{p['user_id']}` — {p['expiry_str']} ({p['days_left']}д) [{p['method']}]\n"
    else:
        text += "Нет активных Premium пользователей."
    await update.message.reply_text(text, parse_mode="Markdown")

# ================================================================
# АВТОСИГНАЛЫ
# ================================================================

async def send_auto_signals(context: ContextTypes.DEFAULT_TYPE):
    subs = get_subscribers()
    if not subs:
        logger.info("Нет подписчиков"); return

    logger.info(f"Автосигналы: {len(subs)} подписчиков")
    to_send = []

    for name, func in [("BTC", get_signal_btc), ("Gold", get_signal_gold)]:
        try:
            data = func()
            if data["direction"] != "⚪️ ЖДАТЬ":
                to_send.append(data)
                logger.info(f"{name}: {data['direction']} ({data['score']})")
            else:
                logger.info(f"{name}: ЖДАТЬ — пропущен")
        except Exception as e:
            logger.error(f"Ошибка {name}: {e}")

    if not to_send:
        logger.info("Нет сигналов — пропускаем"); return

    dead = set()
    for cid in subs.copy():
        for sd in to_send:
            try:
                text = format_premium(sd, is_auto=True) if is_premium(cid) else format_free(sd)
                await context.bot.send_message(cid, text, parse_mode="Markdown")
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

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("signal", signal_command))
    app.add_handler(CommandHandler("subscribe", subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("mystatus", mystatus))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("approve", approve))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("users", users_command))

    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(PreCheckoutQueryHandler(precheckout_handler))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    app.add_handler(MessageHandler(filters.PHOTO, screenshot_handler))

    app.job_queue.run_repeating(send_auto_signals, interval=14400, first=10)

    mode = "PostgreSQL ☁️" if DATABASE_URL else "JSON (локально)"
    print(f"✅ Бот запущен! Хранилище: {mode}")
    logger.info(f"Бот запущен. Хранилище: {mode}")
    app.run_polling()

if __name__ == "__main__":
    main()