# features_extra.py
import os
import time
import asyncio
from typing import Optional, List, Tuple

import aiosqlite
from aiogram import Dispatcher, Bot, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from dotenv import load_dotenv

# ================== CONFIG & DB PATH ==================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = set(int(x) for x in (os.getenv("ADMIN_IDS", "") or "").split(",") if x.strip())
DAILY_BONUS_POINTS = int(os.getenv("DAILY_BONUS_POINTS", "10") or 10)

# локальная БД как в основном проекте
APPDATA_DIR = os.path.join(os.path.expanduser("~"), "AppData", "Local", "mephi_dating")
os.makedirs(APPDATA_DIR, exist_ok=True)
DB_PATH = os.path.join(APPDATA_DIR, "bot.db")

# реферал-бонусы (можно править по вкусу)
REFERRAL_BONUS_INVITER = 20
REFERRAL_BONUS_INVITED = 5

# ================== ВСПОМОГАТЕЛЬНЫЕ КЛАВЫ ==================
def extra_main_menu() -> ReplyKeyboardMarkup:
    """
    Расширенное главное меню — те же кнопки, плюс Магазин/Support/Рефералка.
    При желании можно алиаснуть в проекте:
        from features_extra import extra_main_menu as main_menu
    """
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🧭 Режимы"))
    kb.add(KeyboardButton(text="👤 Анкета"))
    kb.add(KeyboardButton(text="💰 Магазин"))
    kb.add(KeyboardButton(text="🆘 Support"))
    kb.add(KeyboardButton(text="👥 Рефералка"))
    return kb.as_markup(resize_keyboard=True)

def shop_back_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    kb.add(KeyboardButton(text="💰 Магазин"))
    return kb.as_markup(resize_keyboard=True)

# ================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==================
def _now() -> int:
    return int(time.time())

def db():
    return aiosqlite.connect(DB_PATH)

async def get_role(tg_id:int)->str:
    async with db() as conn:
        cur = await conn.execute("SELECT role FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return (row[0] if row and row[0] else "user")

async def add_points(tg_id:int, delta:int):
    async with db() as conn:
        await conn.execute("UPDATE users SET points = COALESCE(points,0) + ? WHERE tg_id=?", (delta, tg_id))
        await conn.commit()

async def get_points(tg_id:int)->int:
    async with db() as conn:
        cur = await conn.execute("SELECT COALESCE(points,0) FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return int(row[0] if row else 0)

async def set_status(tg_id:int, title:Optional[str]):
    async with db() as conn:
        await conn.execute("UPDATE users SET status_title=? WHERE tg_id=?", (title, tg_id))
        await conn.commit()

async def ensure_user(tg_id:int):
    async with db() as conn:
        await conn.execute("INSERT OR IGNORE INTO users(tg_id) VALUES(?)", (tg_id,))
        if tg_id in ADMIN_IDS:
            await conn.execute("UPDATE users SET role='admin' WHERE tg_id=?", (tg_id,))
        await conn.commit()

# ================== ЛОКАЛЬНАЯ СХЕМА ЭКСТРА-ФИЧ ==================
EXTRA_SQL = """
-- Рефералки
CREATE TABLE IF NOT EXISTS referrals(
  invited_id INTEGER PRIMARY KEY,
  inviter_id INTEGER NOT NULL,
  ts INTEGER DEFAULT (strftime('%s','now')),
  rewarded INTEGER DEFAULT 0
);

-- Ленивая защита от дублей покупок статусов (не обязательно)
CREATE UNIQUE INDEX IF NOT EXISTS idx_purchases_unique
ON purchases(user_id, item_id);

-- Ежедневный бонус (опционально)
CREATE TABLE IF NOT EXISTS daily_bonus(
  uid INTEGER PRIMARY KEY,
  last_claim_day INTEGER  -- формат YYYYMMDD
);
"""

async def init_extra_schema():
    async with db() as conn:
        await conn.executescript(EXTRA_SQL)
        await conn.commit()

# ================== МАГАЗИН ==================
async def list_active_items() -> List[Tuple[int, str, int, str, Optional[str]]]:
    """
    Возвращает (id, name, price, type, payload)
    """
    async with db() as conn:
        cur = await conn.execute(
            "SELECT id,name,price,type,payload FROM shop_items WHERE is_active=1 ORDER BY id ASC"
        )
        rows = await cur.fetchall()
        return rows or []

def build_shop_markup(items: List[Tuple[int, str, int, str, Optional[str]]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for it in items:
        it_id, name, price, it_type, payload = it
        kb.button(text=f"Купить — {name} ({price})", callback_data=f"shop_buy:{it_id}")
    if not items:
        kb.button(text="Пока пусто", callback_data="noop")
    kb.adjust(1)
    return kb.as_markup()

async def apply_item_effect(user_id:int, item_id:int):
    async with db() as conn:
        cur = await conn.execute("SELECT type,payload FROM shop_items WHERE id=?", (item_id,))
        row = await cur.fetchone()
    if not row:
        return
    it_type, payload = row
    if it_type == "status":
        await set_status(user_id, payload or "✨ Без названия")
    # it_type == "privilege": тут можно включать флаги/привилегии в отдельной таблице, если появятся

async def handle_purchase(user_id:int, item_id:int) -> str:
    # достанем товар и проверим баланс
    async with db() as conn:
        cur = await conn.execute("SELECT name,price FROM shop_items WHERE id=? AND is_active=1", (item_id,))
        row = await cur.fetchone()
    if not row:
        return "❌ Товар не найден или отключён."
    name, price = row
    bal = await get_points(user_id)
    if bal < price:
        return f"Не хватает очков. Нужно {price}, у тебя {bal}."

    # списание + запись покупки
    async with db() as conn:
        await conn.execute("UPDATE users SET points=COALESCE(points,0)-? WHERE tg_id=?", (price, user_id))
        await conn.execute("INSERT INTO purchases(user_id,item_id) VALUES(?,?)", (user_id, item_id))
        await conn.commit()

    # применяем эффект
    await apply_item_effect(user_id, item_id)
    return f"✅ Покупка «{name}» успешна!"

# ================== САППОРТ ==================
class Support(StatesGroup):
    waiting_text = State()

def support_entry_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

async def save_support_msg(from_user: int, text: str, orig_msg_id: Optional[int]) -> int:
    async with db() as conn:
        cur = await conn.execute(
            "INSERT INTO support_msgs(from_user, orig_msg_id, text) VALUES(?,?,?)",
            (from_user, orig_msg_id or 0, text)
        )
        mid = cur.lastrowid
        await conn.commit()
        return int(mid)

# ================== РЕФЕРАЛКИ ==================
async def register_referral_if_needed(invited_id:int, maybe_payload:str) -> Optional[int]:
    """
    Ожидается payload вида 'ref12345'. Если ок — вставим запись и начислим бонусы.
    Возвращает inviter_id (или None).
    """
    payload = (maybe_payload or "").strip()
    if not payload.startswith("ref"):
        return None
    try:
        inviter_id = int(payload[3:])
    except Exception:
        return None
    if inviter_id == invited_id:
        return None

    # уже был?
    async with db() as conn:
        cur = await conn.execute("SELECT 1 FROM referrals WHERE invited_id=?", (invited_id,))
        if await cur.fetchone():
            return inviter_id

        # Зарегистрируем
        await conn.execute(
            "INSERT OR IGNORE INTO referrals(invited_id,inviter_id) VALUES(?,?)",
            (invited_id, inviter_id)
        )
        await conn.commit()

    # Бонусы: приглашённому чуть, пригласившему больше
    await add_points(inviter_id, REFERRAL_BONUS_INVITER)
    await add_points(invited_id, REFERRAL_BONUS_INVITED)
    return inviter_id

async def get_ref_stats(user_id:int) -> Tuple[int, int]:
    """
    count, total_bonus_approx
    """
    async with db() as conn:
        cur = await conn.execute("SELECT COUNT(*) FROM referrals WHERE inviter_id=?", (user_id,))
        cnt = (await cur.fetchone() or [0])[0]
    return int(cnt or 0), int((cnt or 0) * REFERRAL_BONUS_INVITER)

# ================== РЕГИСТРАЦИЯ ХЕНДЛЕРОВ ==================
def _build_shop_text(items) -> str:
    if not items:
        return "🛍 Магазин пуст. Загляни позже!"
    lines = ["🛍 <b>Магазин</b>\n"]
    for it_id, name, price, it_type, payload in items:
        t_emoji = "🏷" if it_type == "status" else "🎛"
        lines.append(f"{t_emoji} <b>{name}</b> — {price} очков")
    lines.append("\nНажми кнопку «Купить …», чтобы приобрести.")
    return "\n".join(lines)

async def setup_extra_features(dp: Dispatcher, bot: Bot):
    await init_extra_schema()

    # === ГЛОБАЛЬНЫЕ КНОПКИ ===
    @dp.message(F.text == "💰 Магазин")
    async def shop_open(m: Message):
        await ensure_user(m.from_user.id)
        items = await list_active_items()
        await m.answer(
            _build_shop_text(items),
            reply_markup=shop_back_kb()
        )
        await m.answer(
            "Выбери товар:",
            reply_markup=build_shop_markup(items)
        )

    @dp.callback_query(F.data.startswith("shop_buy:"))
    async def shop_buy_cb(c: CallbackQuery):
        try:
            item_id = int(c.data.split(":")[1])
        except Exception:
            await c.answer("Некорректный товар.", show_alert=True)
            return
        msg = await handle_purchase(c.from_user.id, item_id)
        bal = await get_points(c.from_user.id)
        await c.message.answer(f"{msg}\nТекущий баланс: {bal}", reply_markup=shop_back_kb())
        await c.answer("Готово")

    # === САППОРТ ===
    @dp.message(F.text == "🆘 Support")
    async def support_entry(m: Message, state: FSMContext):
        await ensure_user(m.from_user.id)
        await m.answer("Опиши проблему или вопрос одним сообщением. Я перешлю админам.",
                       reply_markup=support_entry_kb())
        await state.set_state(Support.waiting_text)

    @dp.message(Support.waiting_text, F.text)
    async def support_collect(m: Message, state: FSMContext):
        mid = await save_support_msg(m.from_user.id, m.text, m.message_id)
        # перешлём админам
        admins = ADMIN_IDS or set()
        for aid in admins:
            try:
                await bot.send_message(
                    aid,
                    f"🆘 <b>Саппорт</b>\nfrom: <code>{m.from_user.id}</code>\nmsg_id: <code>{mid}</code>\n\n{text_escape(m.text)}",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        await m.answer("Сообщение отправлено. Админы ответят в ближайшее время.",
                       reply_markup=extra_main_menu())
        await state.clear()

    # админ-ответ: /answer <user_id> <текст>
    @dp.message(Command("answer"))
    async def admin_answer(m: Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        parts = (m.text or "").split(maxsplit=2)
        if len(parts) < 3:
            await m.answer("Формат: /answer <user_id> <текст>")
            return
        try:
            target = int(parts[1])
        except Exception:
            await m.answer("user_id должен быть числом.")
            return
        text = parts[2].strip()
        if not text:
            await m.answer("Пустой ответ.")
            return
        try:
            await bot.send_message(target, f"📩 Ответ саппорта:\n{text}")
            await m.answer("Отправил.")
        except Exception as e:
            await m.answer(f"Не удалось отправить: {e!r}")

    # === РЕФЕРАЛКА ===
    @dp.message(F.text == "👥 Рефералка")
    async def referral_entry(m: Message):
        await ensure_user(m.from_user.id)
        me = await bot.get_me()
        bot_username = me.username
        link = f"https://t.me/{bot_username}?start=ref{m.from_user.id}"
        cnt, bonus = await get_ref_stats(m.from_user.id)
        bal = await get_points(m.from_user.id)
        txt = (
            "👥 <b>Реферальная программа</b>\n\n"
            f"Твоя ссылка:\n<code>{link}</code>\n\n"
            f"Приглашённых: <b>{cnt}</b>\n"
            f"Заработано за рефералку: <b>{bonus}</b> очков\n"
            f"Текущий баланс: <b>{bal}</b> очков\n\n"
            f"За каждого друга: +{REFERRAL_BONUS_INVITER} тебе, +{REFERRAL_BONUS_INVITED} другу."
        )
        await m.answer(txt, parse_mode=ParseMode.HTML, reply_markup=extra_main_menu())

    # перехват старт-пейлоада для рефералок (добавляется к уже имеющемуся /start)
    @dp.message(Command("start"))
    async def referral_hook_on_start(m: Message):
        await ensure_user(m.from_user.id)
        # payload после "/start "
        payload = ""
        if m.text:
            parts = m.text.split(maxsplit=1)
            if len(parts) == 2:
                payload = parts[1]
        inviter = await register_referral_if_needed(m.from_user.id, payload)
        if inviter:
            try:
                await m.answer("Ты пришёл по реферальной ссылке. Бонусы начислены! 🎉",
                               reply_markup=extra_main_menu())
            except Exception:
                pass  # без паники, /start основной хендлер тоже ответит

    # === АДМИН: добавление и управление товарами ===
    # /add_item <name> | <price:int> | <type:status|privilege> | <payload>
    @dp.message(Command("add_item"))
    async def admin_add_item(m: Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        raw = (m.text or "").replace("/add_item", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]
        if len(parts) < 4:
            await m.answer("Формат: /add_item Название | 100 | status|privilege | payload")
            return
        name, price_s, it_type, payload = parts[0], parts[1], parts[2], parts[3]
        if it_type not in ("status", "privilege"):
            await m.answer("type должен быть status или privilege")
            return
        try:
            price = int(price_s)
        except Exception:
            await m.answer("price должен быть числом.")
            return
        async with db() as conn:
            await conn.execute(
                "INSERT INTO shop_items(name,price,type,payload,is_active) VALUES(?,?,?,?,1)",
                (name, price, it_type, payload)
            )
            await conn.commit()
        await m.answer("Товар добавлен.")

    @dp.message(Command("toggle_item"))
    async def admin_toggle_item(m: Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await m.answer("Формат: /toggle_item <item_id>")
            return
        try:
            item_id = int(parts[1])
        except Exception:
            await m.answer("item_id должен быть числом.")
            return
        async with db() as conn:
            await conn.execute(
                "UPDATE shop_items SET is_active = CASE is_active WHEN 1 THEN 0 ELSE 1 END WHERE id=?",
                (item_id,)
            )
            await conn.commit()
        await m.answer("Готово, переключил активность.")

    # Быстрый вывод баланса и статуса
    @dp.message(Command("balance"))
    async def cmd_balance(m: Message):
        await ensure_user(m.from_user.id)
        bal = await get_points(m.from_user.id)
        async with db() as conn:
            cur = await conn.execute("SELECT status_title FROM users WHERE tg_id=?", (m.from_user.id,))
            row = await cur.fetchone()
            status = row[0] if row and row[0] else "—"
        await m.answer(f"🔢 Баланс: {bal}\n👑 Статус: {status}", reply_markup=extra_main_menu())

    # Кнопка «⬅️ В главное меню» — если используешь наше меню
    @dp.message(F.text == "⬅️ В главное меню")
    async def back_to_menu(m: Message, state: FSMContext):
        await state.clear()
        await m.answer("Главное меню.", reply_markup=extra_main_menu())

# ================== УТИЛЫ ==================
def text_escape(s: str) -> str:
    if not s:
        return s
    return (s
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))
