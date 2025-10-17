import asyncio
import os
import time
from math import ceil
from typing import Optional, Dict, Tuple
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
import secrets, string

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
try:
    # aiogram v3
    from aiogram.dispatcher.event.bases import SkipHandler
except Exception:
    # на всякий случай, если у тебя v2
    from aiogram.exceptions import SkipHandler  # type: ignore
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from dotenv import load_dotenv
# === Anonymizer: маскируем @user, t.me, tg://user?id=..., почту и телефоны
import re

USER_RE = re.compile(r'(?<!\w)@[\w_]{3,}', re.I)                 # @username
TME_RE  = re.compile(r'(?:https?://)?t\.me/[^\s]+', re.I)        # t.me/...
TGID_RE = re.compile(r'tg://user\?id=\d+', re.I)                 # tg://user?id=...
MAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+', re.I)            # email
PHON_RE = re.compile(r'(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)')        # телефон

def sanitize_text(s: str) -> str:
    s = TGID_RE.sub('[hidden]', s)
    s = TME_RE.sub('[link hidden]', s)
    s = USER_RE.sub('@hidden', s)
    s = MAIL_RE.sub('[email hidden]', s)
    s = PHON_RE.sub('[phone hidden]', s)
    return s

async def send_text_anonym(peer: int, text: str):
    # отключаем HTML-разметку, превью и запрещаем пересылку/сохранение
    await bot.send_message(
        peer,
        sanitize_text(text),
        parse_mode=None,
        disable_web_page_preview=True,
        protect_content=True
    )

def clean_cap(caption: Optional[str]) -> Optional[str]:
    return sanitize_text(caption) if caption else None

# ============================================================
#                         CONFIG
# ============================================================

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Админы (список id через запятую в .env)
ADMIN_IDS = set(int(x) for x in (os.getenv("ADMIN_IDS", "") or "").split(",") if x.strip())

SUPPORT_ENABLED = True
DAILY_BONUS_POINTS = 10
REF_BONUS_POINTS = 20                     # бонус за реферала
INACTIVITY_SECONDS = 180                  # авто-завершение при молчании
CHANNEL_USERNAME = "@nektomephi"          # юзернейм канала
CHANNEL_LINK = "https://t.me/nektomephi"  # ссылка-приглашение
RESOLVED_CHANNEL_ID: Optional[int] = None # будет заполнен в main()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

APPDATA_DIR = os.path.join(os.path.expanduser("~"), "AppData", "Local", "mephi_dating")
os.makedirs(APPDATA_DIR, exist_ok=True)
DB_PATH = os.path.join(APPDATA_DIR, "bot.db")

BLOCK_TXT = "Сейчас идёт анонимный чат. Доступны только команды: !stop, !next, !reveal."

INTRO_TEXT = (
    "⚠️ Перед использованием нужно подписаться на канал: t.me/nektomephi\n\n"
    "👋 Добро пожаловать! Это анонимный чат-бот <b>исключительно для студентов МИФИ</b>.\n\n"
    "!!! В НАСТОЯЩЕЕ ВРЕМЯ РАБОТАЕТ В ТЕСТОВОМ РЕЖИМЕ !!!\n"
    "Бот <b>не является официальным проектом университета</b> — это независимая студенческая инициатива, "
    "созданная для безопасного и комфортного общения внутри сообщества.\n\n"
    "Это гибрид дайвинчика и nekto.me: ты общаешься анонимно, а при взаимном согласии "
    "можно <b>раскрыть личности</b> через команду <code>!reveal</code> (только если у обоих заполнены анкеты).\n\n"
    "💡 На данный момент доступен только режим <b>Анонимный чат</b>.\n"
    "📇 Режим <b>Просмотр анкет</b> — находится в разработке.\n\n"
    "⚙️ Как пользоваться:\n"
    "1️⃣ Выбери свой пол и кого ищешь.\n"
    "2️⃣ По желанию заполни анкету — она нужна только для взаимного раскрытия.\n"
    "3️⃣ Нажми «🔎 Найти собеседника» и начни анонимный диалог.\n\n"
    "💬 Во время чата доступны команды:\n"
    "<code>!next</code> — следующий собеседник\n"
    "<code>!stop</code> — завершить диалог\n"
    "<code>!reveal</code> — запросить взаимное раскрытие\n\n"
    "⚠️ Если кто-то молчит более 180 секунд, диалог автоматически завершается у обоих."
)

FACULTIES = [
    "ИИКС", "ФБИУКС", "ИМО", "ИФТИС",
    "ИНТЭЛ", "ИФТЭБ", "ИФИБ", "ЛАПЛАЗ",
    "ИЯФИТ"
]

# ============================================================
#                      DATABASE & MIGRATIONS
# ============================================================

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS recent_partners(
  u_id INTEGER NOT NULL,
  partner_id INTEGER NOT NULL,
  block_left INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(u_id, partner_id)
);

PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users(
  tg_id INTEGER PRIMARY KEY,
  gender TEXT,
  seeking TEXT,
  reveal_ready INTEGER DEFAULT 0,
  first_name TEXT,
  last_name TEXT,
  faculty TEXT,
  age INTEGER,
  about TEXT,
  username TEXT,
  photo1 TEXT,
  photo2 TEXT,
  photo3 TEXT,
  created_at INTEGER DEFAULT (strftime('%s','now')),
  updated_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS shop_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  price INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload TEXT,
  is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS ratings(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id INTEGER NOT NULL,
  from_user INTEGER NOT NULL,
  to_user INTEGER NOT NULL,
  stars INTEGER NOT NULL CHECK(stars BETWEEN 1 AND 5),
  ts INTEGER DEFAULT (strftime('%s','now')),
  UNIQUE(match_id, from_user)
);

CREATE INDEX IF NOT EXISTS idx_ratings_to ON ratings(to_user);

CREATE TABLE IF NOT EXISTS complaints(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id INTEGER NOT NULL,
  from_user INTEGER NOT NULL,
  about_user INTEGER NOT NULL,
  text TEXT,
  ts INTEGER DEFAULT (strftime('%s','now')),
  status TEXT DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS purchases(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  item_id INTEGER NOT NULL,
  ts INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS user_statuses(
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  PRIMARY KEY(user_id, title)
);

CREATE TABLE IF NOT EXISTS support_msgs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_user INTEGER NOT NULL,
  to_admin INTEGER,
  orig_msg_id INTEGER,
  text TEXT,
  ts INTEGER DEFAULT (strftime('%s','now')),
  status TEXT DEFAULT 'open'
);

CREATE TRIGGER IF NOT EXISTS users_updated
AFTER UPDATE ON users
BEGIN
  UPDATE users SET updated_at=strftime('%s','now') WHERE tg_id=NEW.tg_id;
END;

CREATE TABLE IF NOT EXISTS queue(
  tg_id INTEGER PRIMARY KEY,
  gender TEXT,
  seeking TEXT,
  ts INTEGER
);

CREATE TABLE IF NOT EXISTS matches(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  a_id INTEGER,
  b_id INTEGER,
  active INTEGER DEFAULT 1,
  a_reveal INTEGER DEFAULT 0,
  b_reveal INTEGER DEFAULT 0,
  started_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS referrals(
  inviter INTEGER NOT NULL,
  invited INTEGER PRIMARY KEY,
  ts INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS settings(
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE INDEX IF NOT EXISTS idx_matches_active ON matches(active);
"""

ALTERS = [
    ("users", "role",         "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'"),
    ("users", "points",       "ALTER TABLE users ADD COLUMN points INTEGER DEFAULT 0"),
    ("users", "status_title", "ALTER TABLE users ADD COLUMN status_title TEXT"),
    ("users", "last_daily",   "ALTER TABLE users ADD COLUMN last_daily INTEGER DEFAULT 0")
]

def db():
    return aiosqlite.connect(DB_PATH)

async def init_db():
    async with db() as conn:
        await conn.executescript(CREATE_SQL)

        # мягкие ALTER'ы
        for table, col, sql in ALTERS:
            cur = await conn.execute(f"PRAGMA table_info({table})")
            cols = [r[1] for r in await cur.fetchall()]
            if col not in cols:
                try:
                    await conn.execute(sql)
                except Exception:
                    pass

        # миграция referrals (добавить недостающие колонки)
        cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='referrals'")
        has_ref = await cur.fetchone()
        if has_ref:
            cur = await conn.execute("PRAGMA table_info(referrals)")
            cols = {r[1] for r in await cur.fetchall()}
            if "inviter" not in cols:
                await conn.execute("ALTER TABLE referrals ADD COLUMN inviter INTEGER")
            if "ts" not in cols:
                await conn.execute("ALTER TABLE referrals ADD COLUMN ts INTEGER DEFAULT (strftime('%s','now'))")
            else:
                await conn.execute("""
                        CREATE TABLE IF NOT EXISTS referrals(
                          inviter INTEGER,
                          invited INTEGER PRIMARY KEY,
                          ts INTEGER DEFAULT (strftime('%s','now'))
                        )
                    """)

            # --- NEW: таблица непрозрачных реф-кодов ---
            await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ref_codes(
                      code TEXT PRIMARY KEY,
                      inviter INTEGER NOT NULL
                    )
                """)

            await conn.commit()


# ============================================================
#                         FSM STATES
# ============================================================

class ComplaintState(StatesGroup):
    wait_text = State()   # ждём текст жалобы; в state: mid, about_id

class GState(StatesGroup):
    pick_gender = State()
    pick_seeking = State()

class RevealForm(StatesGroup):
    name = State()
    faculty = State()
    age = State()
    about = State()
    photos = State()

class AdminGrantPoints(StatesGroup):
    wait_user_id = State()
    wait_amount = State()

class SupportState(StatesGroup):
    waiting = State()

class AdminAddItem(StatesGroup):
    wait_name = State()
    wait_price = State()
    wait_type = State()
    wait_payload = State()

class AdminShopDel(StatesGroup):
    wait_id = State()

class AdminSettings(StatesGroup):
    wait_value = State()   # ждём значение для конкретного ключа

class AdminAdmins(StatesGroup):
    mode = State()         # 'add' или 'del'
    wait_user_id = State()

class AdminBroadcast(StatesGroup):
    wait_text = State()

class AdminSupportReply(StatesGroup):
    wait_text = State()    # ответ для конкретного uid (храним в state.data['uid'])

# ============================================================
#                 RUNTIME (RAM) STRUCTURES & UTILS
# ============================================================

# ------------------ Runtime settings cache ------------------
SETTINGS: Dict[str, str] = {}  # key -> value (str)

DEFAULT_SETTINGS = {
    "inactivity_seconds": "180",   # ⏱️ тайм-аут молчания
    "ref_bonus_points":   "20",    # 🎯 реф-бонус
    "daily_bonus_points": "10",    # 🎁 ежедневный бонус
    "block_rounds":       "2",     # 🔁 сколько «раундов» не матчить ту же пару
    "support_enabled":    "1",     # 🆘 включен ли саппорт (1/0)
}

async def load_settings_cache():
    SETTINGS.clear()
    async with db() as conn:
        cur = await conn.execute("SELECT key, value FROM settings")
        for k, v in await cur.fetchall():
            SETTINGS[k] = str(v)
    # проставим дефолты, если чего-то нет
    async with db() as conn:
        for k, v in DEFAULT_SETTINGS.items():
            if k not in SETTINGS:
                await conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (k, v))
                SETTINGS[k] = v
        await conn.commit()

async def safe_edit_message(msg: Message, *, text: Optional[str] = None, reply_markup=None):
    """
    Аккуратно правит сообщение: если текст не меняется — редактирует только клавиатуру.
    Игнорит 'message is not modified'.
    """
    try:
        current_text = msg.text or ""
        if text is not None and text != current_text:
            await msg.edit_text(text, reply_markup=reply_markup)
        elif reply_markup is not None:
            await msg.edit_reply_markup(reply_markup=reply_markup)
        # иначе — менять нечего
    except TelegramBadRequest as e:
        # Телеграм ругается, если править тем же самым — просто пропускаем
        if "message is not modified" in str(e):
            return
        raise

async def set_setting(key: str, value: str):
    async with db() as conn:
        await conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(value)))
        await conn.commit()
    SETTINGS[key] = str(value)

def g_inactivity() -> int:
    return int(SETTINGS.get("inactivity_seconds", DEFAULT_SETTINGS["inactivity_seconds"]))

def g_ref_bonus() -> int:
    return int(SETTINGS.get("ref_bonus_points", DEFAULT_SETTINGS["ref_bonus_points"]))

def g_daily_bonus() -> int:
    return int(SETTINGS.get("daily_bonus_points", DEFAULT_SETTINGS["daily_bonus_points"]))

def g_block_rounds() -> int:
    return int(SETTINGS.get("block_rounds", DEFAULT_SETTINGS["block_rounds"]))

def g_support_enabled() -> bool:
    return SETTINGS.get("support_enabled", DEFAULT_SETTINGS["support_enabled"]) == "1"

def intro_text() -> str:
    t = int(SETTINGS.get("inactivity_seconds", "180"))
    return (
        "⚠️ Перед использованием нужно подписаться на канал: t.me/nektomephi\n\n"
        "👋 Добро пожаловать! Это анонимный чат-бот <b>исключительно для студентов МИФИ</b>.\n\n"
        "!!! В НАСТОЯЩЕЕ ВРЕМЯ РАБОТАЕТ В ТЕСТОВОМ РЕЖИМЕ !!!\n"
        "Бот <b>не является официальным проектом университета</b> — это независимая студенческая инициатива, "
        "созданная для безопасного и комфортного общения внутри сообщества.\n\n"
        "Это гибрид дайвинчика и nekto.me: ты общаешься анонимно, а при взаимном согласии "
        "можно <b>раскрыть личности</b> через команду <code>!reveal</code> (только если у обоих заполнены анкеты).\n\n"
        "💡 На данный момент доступен только режим <b>Анонимный чат</b>.\n"
        "📇 Режим <b>Просмотр анкет</b> — находится в разработке.\n\n"
        "⚙️ Как пользоваться:\n"
        "1️⃣ Выбери свой пол и кого ищешь.\n"
        "2️⃣ По желанию заполни анкету — она нужна только для взаимного раскрытия.\n"
        "3️⃣ Нажми «🔎 Найти собеседника» и начни анонимный диалог.\n\n"
        "💬 Во время чата доступны команды:\n"
        "<code>!next</code> — следующий собеседник\n"
        "<code>!stop</code> — завершить диалог\n"
        "<code>!reveal</code> — запросить взаимное раскрытие\n\n"
        f"⚠️ Если кто-то молчит более {t} секунд, диалог автоматически завершится у обоих."
    )

_nowm = time.monotonic  # монотоничные секунды

DEADLINE: Dict[int, float] = {}   # match_id -> monotonic deadline
LAST_SHOWN: Dict[int, int] = {}   # match_id -> последний показанный остаток
ACTIVE: Dict[int, Tuple[int, int]] = {}   # user_id -> (peer_id, match_id)
LAST_SEEN: Dict[int, float] = {}          # user_id -> last_seen_unix
WATCH: Dict[int, asyncio.Task] = {}       # match_id -> watcher task
WARNED: Dict[int, bool] = {}              # match_id -> warned for countdown
SUPPORT_RELAY: Dict[int, int] = {}        # msg_id_у_бота -> user_id
COUNTDOWN_TASKS: Dict[int, asyncio.Task] = {}                     # match_id -> task
COUNTDOWN_MSGS: Dict[int, Tuple[Optional[int], Optional[int]]] = {}  # match_id -> (msg_id_a, msg_id_b)

def _now() -> float:
    return time.time()

# ------------------ Generic chat gate ------------------

async def is_chat_active(user_id: int) -> bool:
    if user_id in ACTIVE:
        return True
    async with db() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM matches WHERE active=1 AND (a_id=? OR b_id=?) LIMIT 1",
            (user_id, user_id),
        )
        return (await cur.fetchone()) is not None

async def deny_actions_during_chat(m: Message) -> bool:
    """
    Вернёт True, если пользователь в активном чате: в этом случае
    отправляет блокирующее сообщение и скрывает клавиатуру.
    """
    if await is_chat_active(m.from_user.id):
        await _materialize_session_if_needed(m.from_user.id)  # оживим RAM при необходимости
        await m.answer(BLOCK_TXT, reply_markup=ReplyKeyboardRemove())
        return True
    return False

# ============================================================
#                       KEYBOARDS (UI)
# ============================================================

def main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🧭 Режимы"))
    kb.add(KeyboardButton(text="👤 Анкета"))
    kb.add(KeyboardButton(text="🆘 Поддержка"))
    kb.add(KeyboardButton(text="⭐️ Оценить собеседника"))
    kb.add(KeyboardButton(text="🚩 Пожаловаться"))
    return kb.as_markup(resize_keyboard=True)

def modes_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="📇 Просмотр анкет"))
    kb.add(KeyboardButton(text="🕵️ Анонимный чат"))
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    return kb.as_markup(resize_keyboard=True)

def subscription_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Подписаться", url=CHANNEL_LINK)
    kb.button(text="✅ Проверить подписку", callback_data="sub_check")
    kb.adjust(1)
    return kb.as_markup()

def anon_chat_menu_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🔎 Найти собеседника"))
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    return kb.as_markup(resize_keyboard=True)

def cancel_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

def rate_or_complain_kb(mid: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for i in range(1, 6):
        b.button(text=str(i), callback_data=f"rate:{mid}:{i}")
    b.button(text="🚩 Пожаловаться", callback_data=f"complain:{mid}")
    b.adjust(5, 1)
    return b.as_markup()

def shop_kb(items) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for (id_, name, price, type_, payload) in items:
        b.button(text=f"{name} — {price}💰", callback_data=f"shop_buy:{id_}")
    if not items:
        b.button(text="Пока пусто 😅", callback_data="noop")
    b.adjust(1)
    return b.as_markup()

def gender_self_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="Я девушка"))
    kb.add(KeyboardButton(text="Я парень"))
    return kb.as_markup(resize_keyboard=True)

def seeking_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="Девушки"))
    kb.add(KeyboardButton(text="Парни"))
    kb.add(KeyboardButton(text="Не важно"))
    return kb.as_markup(resize_keyboard=True)

def faculties_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for i, f in enumerate(FACULTIES):
        b.button(text=f, callback_data=f"fac:{i}")
    b.adjust(2)
    return b.as_markup()

def reveal_entry_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    kb.add(KeyboardButton(text="✏️ Заполнить / Перезаполнить"))
    return kb.as_markup(resize_keyboard=True)

def about_kb(refill: bool = False, has_prev: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="Пропустить"))
    if refill and has_prev:
        kb.add(KeyboardButton(text="Оставить текущее"))
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

def photos_empty_kb(refill: bool = False, has_prev: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    if refill and has_prev:
        kb.add(KeyboardButton(text="Оставить текущее"))
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

def photos_progress_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="Готово"))
    kb.add(KeyboardButton(text="Сбросить фото"))
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

def statuses_kb(inventory: list[str], current: Optional[str]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in inventory:
        mark = " ✅" if current and t == current else ""
        b.button(text=f"{t}{mark}", callback_data=f"use_status:{t}")
    if current:
        b.button(text="Снять статус", callback_data="use_status:__none__")
    b.adjust(1)
    return b.as_markup()

def admin_main_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🛍 Магазин", callback_data="admin:shop")
    b.button(text="⚙️ Настройки", callback_data="admin:settings")
    b.button(text="👥 Админы", callback_data="admin:admins")
    b.button(text="🧰 Поддержка", callback_data="admin:support")
    b.button(text="📣 Рассылка", callback_data="admin:broadcast")
    b.button(text="📊 Статистика", callback_data="admin:stats")
    # === NEW: кнопка выдачи очков
    b.button(text="💳 Выдать очки", callback_data="admin:grant")
    b.adjust(2, 2, 2, 1)
    return b.as_markup()

def admin_shop_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить товар", callback_data="admin:shop:add")
    b.button(text="📦 Список", callback_data="admin:shop:list")
    b.button(text="🗑 Удалить", callback_data="admin:shop:del")
    b.button(text="↩️ Назад", callback_data="admin:home")
    b.adjust(1)
    return b.as_markup()

def admin_settings_kb() -> InlineKeyboardMarkup:
    # показываем текущие значения из кеша
    b = InlineKeyboardBuilder()
    b.button(text=f"⏱️ Неактивность: {g_inactivity()} c", callback_data="admin:set:inactivity_seconds")
    b.button(text=f"🔁 Блок-раундов: {g_block_rounds()}", callback_data="admin:set:block_rounds")
    b.button(text=f"🎁 Daily: {g_daily_bonus()}", callback_data="admin:set:daily_bonus_points")
    b.button(text=f"🎯 Referral: {g_ref_bonus()}", callback_data="admin:set:ref_bonus_points")
    b.button(text=f"🆘 Support: {'ON' if g_support_enabled() else 'OFF'}", callback_data="admin:set:support_toggle")
    b.button(text="↩️ Назад", callback_data="admin:home")
    b.adjust(1)
    return b.as_markup()

def admin_admins_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить админа", callback_data="admin:admins:add")
    b.button(text="➖ Удалить админа", callback_data="admin:admins:del")
    b.button(text="↩️ Назад", callback_data="admin:home")
    b.adjust(1)
    return b.as_markup()

def chat_hint() -> str:
    return ("Команды в чате:\n"
            "<code>!next</code> — следующий собеседник\n"
            "<code>!stop</code> — закончить\n"
            "<code>!reveal</code> — взаимное раскрытие (если анкеты есть у обоих)\n")

# ============================================================
#                      DB HELPERS & QUERIES
# ============================================================

DEFAULT_FREE_STATUSES = ["Котик 12 кафедры", "Вайбкодер", "Странный чел"]

async def add_status_to_inventory(user_id: int, title: str):
    async with db() as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO user_statuses(user_id, title) VALUES(?,?)",
            (user_id, title)
        )
        await conn.commit()

async def can_take_daily_today(tg_id: int) -> bool:
    """
    Разрешаем 1 раз в календарные сутки по UTC.
    Сравниваем date('now') и date(last_daily, 'unixepoch').
    """
    async with db() as conn:
        cur = await conn.execute("SELECT COALESCE(last_daily, 0) FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        last = int(row[0] if row else 0)
        if last == 0:
            return True
        cur = await conn.execute("SELECT date('now') <> date(?, 'unixepoch')", (last,))
        # вернётся 1 если даты отличаются (значит ещё не брали сегодня)
        return bool((await cur.fetchone())[0])

async def mark_daily_taken(tg_id: int):
    await set_user_fields(tg_id, last_daily=int(time.time()))

async def get_avg_rating(user_id: int) -> tuple[Optional[float], int]:
    async with db() as conn:
        cur = await conn.execute("SELECT AVG(stars), COUNT(*) FROM ratings WHERE to_user=?", (user_id,))
        row = await cur.fetchone()
        avg = float(row[0]) if row and row[0] is not None else None
        cnt = int(row[1] or 0)
        return avg, cnt

async def last_match_info(user_id: int) -> Optional[tuple[int, int, int]]:
    """
    Возвращает (match_id, peer_id, active) для последнего матча пользователя.
    """
    async with db() as conn:
        cur = await conn.execute("""
            SELECT id,a_id,b_id,active FROM matches
            WHERE a_id=? OR b_id=?
            ORDER BY id DESC LIMIT 1
        """, (user_id, user_id))
        row = await cur.fetchone()
        if not row:
            return None
        mid, a, b, active = int(row[0]), int(row[1]), int(row[2]), int(row[3])
        peer = b if a == user_id else a
        return mid, peer, active

async def send_post_chat_feedback(user_id: int, peer_id: int, mid: int):
    try:
        await bot.send_message(
            user_id,
            "Как тебе собеседник? Поставь оценку (1–5) или подай жалобу:",
            reply_markup=rate_or_complain_kb(mid)
        )
    except Exception:
        pass

async def get_status_inventory(user_id: int) -> list[str]:
    async with db() as conn:
        cur = await conn.execute(
            "SELECT title FROM user_statuses WHERE user_id=? ORDER BY title ASC",
            (user_id,)
        )
        return [r[0] for r in await cur.fetchall()]

async def ensure_free_statuses(user_id: int):
    inv = await get_status_inventory(user_id)
    missing = [s for s in DEFAULT_FREE_STATUSES if s not in inv]
    if not missing:
        return
    async with db() as conn:
        await conn.executemany(
            "INSERT OR IGNORE INTO user_statuses(user_id, title) VALUES(?,?)",
            [(user_id, s) for s in missing]
        )
        await conn.commit()

async def ensure_user(tg_id: int):
    async with db() as conn:
        await conn.execute("INSERT OR IGNORE INTO users(tg_id) VALUES(?)", (tg_id,))
        if tg_id in ADMIN_IDS:
            await conn.execute("UPDATE users SET role='admin' WHERE tg_id=?", (tg_id,))
        await conn.commit()
    # NEW: гарантируем бесплатные статусы в инвентаре
    await ensure_free_statuses(tg_id)

async def set_user_fields(tg_id: int, **kwargs):
    if not kwargs:
        return
    cols = ", ".join([f"{k}=?" for k in kwargs.keys()])
    vals = list(kwargs.values()) + [tg_id]
    async with db() as conn:
        await conn.execute(f"UPDATE users SET {cols} WHERE tg_id=?", vals)
        await conn.commit()

async def get_user(tg_id: int):
    async with db() as conn:
        cur = await conn.execute("""
            SELECT tg_id,gender,seeking,reveal_ready,first_name,last_name,
                   faculty,age,about,username,photo1,photo2,photo3
            FROM users WHERE tg_id=?
        """, (tg_id,))
        return await cur.fetchone()

async def get_user_or_create(tg_id: int):
    u = await get_user(tg_id)
    if not u:
        await ensure_user(tg_id)
        u = await get_user(tg_id)
    return u

async def get_role(tg_id: int) -> str:
    async with db() as conn:
        cur = await conn.execute("SELECT role FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return row[0] if row else "user"

async def add_points(tg_id: int, delta: int):
    async with db() as conn:
        await conn.execute("UPDATE users SET points = COALESCE(points,0) + ? WHERE tg_id=?", (delta, tg_id))
        await conn.commit()

async def get_points(tg_id: int) -> int:
    async with db() as conn:
        cur = await conn.execute("SELECT COALESCE(points,0) FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return int(row[0] if row else 0)

async def list_items():
    async with db() as conn:
        cur = await conn.execute(
            "SELECT id,name,price,type,payload FROM shop_items WHERE is_active=1 ORDER BY price ASC, id ASC"
        )
        return await cur.fetchall()

async def add_item(name: str, price: int, type_: str, payload: str):
    async with db() as conn:
        await conn.execute(
            "INSERT INTO shop_items(name,price,type,payload) VALUES(?,?,?,?)",
            (name, price, type_, payload)
        )
        await conn.commit()

async def del_item(item_id: int):
    async with db() as conn:
        await conn.execute("DELETE FROM shop_items WHERE id=?", (item_id,))
        await conn.commit()

async def get_item(item_id: int):
    async with db() as conn:
        cur = await conn.execute("SELECT id,name,price,type,payload FROM shop_items WHERE id=?", (item_id,))
        return await cur.fetchone()

async def set_status(tg_id: int, title: Optional[str]):
    async with db() as conn:
        await conn.execute("UPDATE users SET status_title=? WHERE tg_id=?", (title, tg_id))
        await conn.commit()

async def get_status(tg_id: int) -> Optional[str]:
    async with db() as conn:
        cur = await conn.execute("SELECT status_title FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

async def has_required_prefs(tg_id: int) -> bool:
    u = await get_user(tg_id)
    if not u:
        return False
    gender = (u[1] or "").strip()
    seeking = (u[2] or "").strip()
    return bool(gender and seeking)

async def register_referral(inviter: int, invited: int) -> bool:
    if inviter == invited or inviter is None:
        return False
    async with db() as conn:
        cur = await conn.execute("SELECT 1 FROM referrals WHERE invited=?", (invited,))
        if await cur.fetchone():
            return False
        await conn.execute("INSERT INTO referrals(inviter, invited) VALUES(?,?)", (inviter, invited))
        await conn.commit()
    return True

async def count_referrals(inviter: int) -> int:
    async with db() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS referrals(inviter INTEGER, invited INTEGER PRIMARY KEY, ts INTEGER DEFAULT (strftime('%s','now')))"
        )
        cur = await conn.execute("PRAGMA table_info(referrals)")
        cols = {r[1] for r in await cur.fetchall()}
        if "inviter" not in cols:
            await conn.execute("ALTER TABLE referrals ADD COLUMN inviter INTEGER")
        cur = await conn.execute("SELECT COUNT(*) FROM referrals WHERE inviter=?", (inviter,))
        row = await cur.fetchone()
        return int(row[0] if row else 0)

async def purchases_summary(user_id: int) -> tuple[int, list[str]]:
    """Возвращает (сумма_по_покупкам, названия_последних_5_покупок)"""
    async with db() as conn:
        cur = await conn.execute("""
            SELECT COALESCE(SUM(si.price),0)
            FROM purchases p JOIN shop_items si ON si.id=p.item_id
            WHERE p.user_id=?
        """, (user_id,))
        total = int((await cur.fetchone())[0])
        cur = await conn.execute("""
            SELECT si.name
            FROM purchases p JOIN shop_items si ON si.id=p.item_id
            WHERE p.user_id=?
            ORDER BY p.ts DESC
            LIMIT 5
        """, (user_id,))
        names = [r[0] for r in await cur.fetchall()]
    return total, names

# --- REF-CODES (непрозрачные коды для рефералок) ---
ALPH = string.ascii_letters + string.digits

async def get_or_create_ref_code(inviter: int) -> str:
    async with db() as conn:
        cur = await conn.execute("SELECT code FROM ref_codes WHERE inviter=?", (inviter,))
        row = await cur.fetchone()
        if row:
            return row[0]
        import secrets
        code = ''.join(secrets.choice(ALPH) for _ in range(12))
        await conn.execute("INSERT INTO ref_codes(code,inviter) VALUES(?,?)", (code, inviter))
        await conn.commit()
        return code

async def inviter_by_code(code: str) -> Optional[int]:
    async with db() as conn:
        cur = await conn.execute("SELECT inviter FROM ref_codes WHERE code=?", (code,))
        row = await cur.fetchone()
        return int(row[0]) if row else None

# ============================================================
#                 SUBSCRIPTION (CHANNEL) GATE
# ============================================================

async def is_subscribed(user_id: int) -> bool:
    target = RESOLVED_CHANNEL_ID or CHANNEL_USERNAME
    try:
        cm = await bot.get_chat_member(target, user_id)
        status = str(getattr(cm, "status", "")).lower()
        if status in ("member", "administrator", "creator"):
            return True
        if hasattr(cm, "is_member") and bool(getattr(cm, "is_member")):
            return True
        return False
    except Exception as e:
        print("is_subscribed error:", repr(e))
        return False

async def gate_subscription(message: Message) -> bool:
    if await is_subscribed(message.from_user.id):
        return True
    text = (
        "🔔 Перед использованием бота нужно подписаться на наш канал.\n"
        "Это помогает держать всех в курсе обновлений и правил."
    )
    if RESOLVED_CHANNEL_ID is None:
        text += "\n\n⚠️ Техническая заметка: если подписка не определяется после вступления, добавьте бота администратором канала."
    await message.answer(text, reply_markup=subscription_kb())
    return False

# ============================================================
#                     MATCHING & SESSION HELPERS
# ============================================================

async def active_peer(tg_id: int) -> Optional[int]:
    if tg_id in ACTIVE:
        return ACTIVE[tg_id][0]
    async with db() as conn:
        cur = await conn.execute(
            "SELECT a_id,b_id FROM matches WHERE active=1 AND (a_id=? OR b_id=?) ORDER BY id DESC LIMIT 1",
            (tg_id, tg_id)
        )
        row = await cur.fetchone()
        if not row:
            return None
        a, b = row
        return b if a == tg_id else a

async def end_current_chat(tg_id: int):
    async with db() as conn:
        await conn.execute("UPDATE matches SET active=0 WHERE active=1 AND (a_id=? OR b_id=?)", (tg_id, tg_id))
        await conn.commit()

async def enqueue(tg_id: int, gender: str, seeking: str):
    async with db() as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO queue(tg_id, gender, seeking, ts) VALUES(?,?,?,strftime('%s','now'))",
            (tg_id, gender, seeking)
        )
        await conn.commit()

async def dequeue(tg_id: int):
    async with db() as conn:
        await conn.execute("DELETE FROM queue WHERE tg_id=?", (tg_id,))
        await conn.commit()

async def record_separation(a: int, b: int):
    async with db() as conn:
        br = g_block_rounds()
        await conn.execute(
            "INSERT INTO recent_partners(u_id,partner_id,block_left) VALUES(?,?,?) "
            "ON CONFLICT(u_id,partner_id) DO UPDATE SET block_left=?",
            (a, b, br, br)
        )
        await conn.execute(
            "INSERT INTO recent_partners(u_id,partner_id,block_left) VALUES(?,?,?) "
            "ON CONFLICT(u_id,partner_id) DO UPDATE SET block_left=?",
            (b, a, br, br)
        )
        await conn.commit()

async def decay_blocks(u_id: int):
    async with db() as conn:
        await conn.execute(
            "UPDATE recent_partners SET block_left=block_left-1 WHERE u_id=? AND block_left>0",
            (u_id,)
        )
        await conn.execute("DELETE FROM recent_partners WHERE u_id=? AND block_left<=0", (u_id,))
        await conn.commit()

async def is_recent_blocked(u_id: int, candidate_id: int) -> bool:
    async with db() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM recent_partners WHERE u_id=? AND partner_id=? AND block_left>0",
            (u_id, candidate_id)
        )
        return (await cur.fetchone()) is not None

async def in_queue(tg_id: int) -> bool:
    async with db() as conn:
        cur = await conn.execute("SELECT 1 FROM queue WHERE tg_id=?", (tg_id,))
        return (await cur.fetchone()) is not None

def format_profile_text(u: tuple) -> str:
    """
    u = (tg_id, gender, seeking, reveal_ready, first_name, last_name,
         faculty, age, about, username, photo1, photo2, photo3)
    """
    first = (u[4] or "").strip()
    last = (u[5] or "").strip()
    name = first or (last.split()[0] if last else "Без имени")
    age = u[7]
    age_str = str(age) if isinstance(age, int) else "—"
    faculty = (u[6] or "—").strip()

    about = (u[8] or "").strip()
    first_line, rest = "", ""
    if about:
        lines = [ln.strip() for ln in about.splitlines() if ln.strip()]
        if lines:
            first_line = lines[0]
            if len(lines) > 1:
                rest = "\n".join(lines[1:])

    header = f"{name}, {age_str}, 📍 {faculty}"
    if first_line:
        header += f" — {first_line}"
    body = f"\n{rest}" if rest else ""
    username = (u[9] or "").strip()
    tail = f"\n\n{username}" if username else ""
    return header + body + tail

async def find_partner(for_id: int) -> Optional[int]:
    async with db() as conn:
        cur = await conn.execute("SELECT gender,seeking FROM users WHERE tg_id=?", (for_id,))
        me = await cur.fetchone()
        if not me:
            return None
        my_gender, my_seek = me

        cur = await conn.execute("""
            SELECT q.tg_id
            FROM queue q
            JOIN users u ON u.tg_id = q.tg_id
            LEFT JOIN recent_partners rp
                   ON rp.u_id=? AND rp.partner_id=q.tg_id AND rp.block_left>0
            WHERE q.tg_id<>?
              AND ((?='Не важно') OR u.gender=CASE ? WHEN 'Парни' THEN 'Парень' WHEN 'Девушки' THEN 'Девушка' END)
              AND (u.seeking='Не важно' OR u.seeking=CASE ? WHEN 'Парень' THEN 'Парни' WHEN 'Девушка' THEN 'Девушки' END)
              AND rp.partner_id IS NULL
            ORDER BY q.ts ASC
            LIMIT 1
        """, (for_id, for_id, my_seek, my_seek, my_gender))
        row = await cur.fetchone()
        if not row:
            return None
        return int(row[0])

async def start_match(a: int, b: int):
    await decay_blocks(a)
    await decay_blocks(b)
    async with db() as conn:
        await conn.execute("DELETE FROM queue WHERE tg_id IN (?,?)", (a, b))
        cur = await conn.execute("INSERT INTO matches(a_id,b_id) VALUES(?,?)", (a, b))
        mid = cur.lastrowid
        await conn.commit()

    ACTIVE[a] = (b, mid)
    ACTIVE[b] = (a, mid)
    LAST_SEEN[a] = _now()
    LAST_SEEN[b] = _now()
    DEADLINE[mid] = _nowm() + g_inactivity()
    LAST_SHOWN.pop(mid, None)
    WATCH[mid] = asyncio.create_task(_watch_inactivity(mid, a, b))

    sa = await get_status(a)
    sb = await get_status(b)

    pa, pcnt = await get_avg_rating(b)  # рейтинг собеседника для A
    ma, mcnt = await get_avg_rating(a)  # собственный рейтинг A (видит свой)

    pb, bcnt = await get_avg_rating(a)  # рейтинг собеседника для B
    mb, bmcnt = await get_avg_rating(b) # собственный рейтинг B

    def fmt(avg, cnt):
        return f"{avg:.1f} ({cnt})" if avg is not None else "— (0)"

    def greet_line(self_status: Optional[str], peer_rating: str, my_rating: str) -> str:
        who = self_status or "без статуса"
        return (
            f"Ваш собеседник — {who}. Вы анонимны.\n"
            f"Рейтинг собеседника: {peer_rating}\n"
            f"Твой рейтинг: {my_rating}\n\n"
            "Команды в чате:\n"
            "<code>!next</code> — следующий собеседник\n"
            "<code>!stop</code> — закончить\n"
            "<code>!reveal</code> — взаимное раскрытие (если анкеты есть у обоих)\n"
        )

    await bot.send_message(a, greet_line(sb, fmt(pa, pcnt), fmt(ma, mcnt)), reply_markup=ReplyKeyboardRemove())
    await bot.send_message(b, greet_line(sa, fmt(pb, bcnt), fmt(mb, bmcnt)), reply_markup=ReplyKeyboardRemove())

async def try_match_now(tg_id: int):
    mate = await find_partner(tg_id)
    if mate:
        await start_match(tg_id, mate)

async def _materialize_session_if_needed(user_id: int) -> Optional[Tuple[int, int]]:
    if user_id in ACTIVE:
        peer_id, mid = ACTIVE[user_id]
        if mid not in WATCH or WATCH[mid].done():
            a = user_id
            b = peer_id
            WATCH[mid] = asyncio.create_task(_watch_inactivity(mid, a, b))
        return ACTIVE[user_id]

    async with db() as conn:
        cur = await conn.execute(
            "SELECT id, a_id, b_id FROM matches WHERE active=1 AND (a_id=? OR b_id=?) ORDER BY id DESC LIMIT 1",
            (user_id, user_id),
        )
        row = await cur.fetchone()

    if not row:
        return None

    mid, a, b = int(row[0]), int(row[1]), int(row[2])
    peer = b if a == user_id else a

    ACTIVE[a] = (b, mid)
    ACTIVE[b] = (a, mid)
    now_wall = _now()
    LAST_SEEN[a] = now_wall
    LAST_SEEN[b] = now_wall
    DEADLINE[mid] = _nowm() + g_inactivity()
    LAST_SHOWN.pop(mid, None)
    if mid not in WATCH or WATCH[mid].done():
        WATCH[mid] = asyncio.create_task(_watch_inactivity(mid, a, b))

    return (peer, mid)

# ============================================================
#                   INACTIVITY WATCHER & COUNTDOWN
# ============================================================

async def _watch_inactivity(mid: int, a: int, b: int):
    try:
        while True:
            await asyncio.sleep(1)
            if a not in ACTIVE or b not in ACTIVE:
                return
            if ACTIVE.get(a, (None, None))[1] != mid or ACTIVE.get(b, (None, None))[1] != mid:
                return

            now = _nowm()
            deadline = DEADLINE.get(mid, now + g_inactivity())
            remaining = ceil(deadline - now)

            if 0 < remaining <= 60 and not WARNED.get(mid):
                WARNED[mid] = True
                warn_text = (
                    f"⌛️ Тишина… Чат автоматически завершится через {remaining} сек.\n"
                    f"Напиши любое сообщение, чтобы продолжить разговор."
                )
                try:
                    ma = await bot.send_message(a, warn_text)
                    mb = await bot.send_message(b, warn_text)
                    COUNTDOWN_MSGS[mid] = (ma.message_id, mb.message_id)
                except Exception:
                    COUNTDOWN_MSGS[mid] = (None, None)
                COUNTDOWN_TASKS[mid] = asyncio.create_task(_countdown(mid, a, b))

            if remaining <= 0:
                await _stop_countdown(mid, a, b, delete_msgs=True)
                await end_current_chat(a)
                await end_current_chat(b)
                _cleanup_match(mid, a, b)
                DEADLINE.pop(mid, None)
                LAST_SHOWN.pop(mid, None)
                try:
                    await bot.send_message(a, "Чат завершён из-за неактивности.", reply_markup=(await menu_for(a)))
                    await send_post_chat_feedback(a, b, mid)
                    await send_post_chat_feedback(b, a, mid)
                except Exception:
                    pass
                try:
                    await bot.send_message(b, "Чат завершён из-за неактивности.", reply_markup=(await menu_for(b)))
                    await send_post_chat_feedback(a, b, mid)
                    await send_post_chat_feedback(b, a, mid)
                except Exception:
                    pass
                return
    except asyncio.CancelledError:
        return

def _cleanup_match(mid: int, a: int, b: int):
    ACTIVE.pop(a, None)
    ACTIVE.pop(b, None)
    LAST_SEEN.pop(a, None)
    LAST_SEEN.pop(b, None)
    t = WATCH.pop(mid, None)
    DEADLINE.pop(mid, None)
    LAST_SHOWN.pop(mid, None)
    if t and not t.done():
        t.cancel()
    WARNED.pop(mid, None)

    t2 = COUNTDOWN_TASKS.pop(mid, None)
    if t2 and not t2.done():
        t2.cancel()
    COUNTDOWN_MSGS.pop(mid, None)

async def _countdown(mid: int, a: int, b: int):
    try:
        while True:
            await asyncio.sleep(1)
            if a not in ACTIVE or b not in ACTIVE:
                return
            if ACTIVE.get(a, (None, None))[1] != mid or ACTIVE.get(b, (None, None))[1] != mid:
                return

            now = _nowm()
            deadline = DEADLINE.get(mid, now + g_inactivity())
            remaining = ceil(deadline - now)

            if remaining > 60:
                await _stop_countdown(mid, a, b, delete_msgs=True)
                return
            if remaining <= 0:
                return
            if LAST_SHOWN.get(mid) == remaining:
                continue
            LAST_SHOWN[mid] = remaining

            ids = COUNTDOWN_MSGS.get(mid)
            if ids:
                a_msg, b_msg = ids
                text = f"⌛️ Тишина… Осталось {remaining} сек.\nНапиши, чтобы продолжить."
                try:
                    if a_msg:
                        await bot.edit_message_text(chat_id=a, message_id=a_msg, text=text)
                except Exception:
                    pass
                try:
                    if b_msg:
                        await bot.edit_message_text(chat_id=b, message_id=b_msg, text=text)
                except Exception:
                    pass
    except asyncio.CancelledError:
        return

async def _stop_countdown(mid: int, a: int, b: int, delete_msgs: bool = True):
    t = COUNTDOWN_TASKS.pop(mid, None)
    if t and not t.done():
        t.cancel()

    ids = COUNTDOWN_MSGS.pop(mid, None)
    if delete_msgs and ids:
        a_msg, b_msg = ids
        try:
            if a_msg:
                await bot.delete_message(chat_id=a, message_id=a_msg)
        except Exception:
            pass
        try:
            if b_msg:
                await bot.delete_message(chat_id=b, message_id=b_msg)
        except Exception:
            pass
    WARNED.pop(mid, None)

# ============================================================
#                    MISC HELPERS (FORM ETC.)
# ============================================================

async def _require_username(m: Message) -> bool:
    uname = m.from_user.username or ""
    if uname:
        return True
    await m.answer(
        "ℹ️ Для анкеты нужен @username в Telegram.\n"
        "Открой «Настройки → Изменить имя пользователя», установи его и вернись сюда.",
        reply_markup=main_menu()
    )
    return False

# ============================================================
#                         CALLBACKS
# ============================================================

@dp.callback_query(F.data.regexp(r"^rate:\d+:\d$"))
async def cb_rate(c: CallbackQuery):
    try:
        _, mid_s, stars_s = c.data.split(":")
        mid = int(mid_s); stars = int(stars_s)
        assert 1 <= stars <= 5
    except Exception:
        return await c.answer("Некорректная оценка.", show_alert=True)

    # проверяем, что пользователь был участником этого матча и узнаём peer
    async with db() as conn:
        cur = await conn.execute("SELECT a_id,b_id FROM matches WHERE id=?", (mid,))
        row = await cur.fetchone()
    if not row:
        return await c.answer("Матч не найден.", show_alert=True)

    a_id, b_id = int(row[0]), int(row[1])
    if c.from_user.id not in (a_id, b_id):
        return await c.answer("Это не твой диалог.", show_alert=True)

    to_user = b_id if c.from_user.id == a_id else a_id

    # пишем/фиксируем оценку (один раз за матч)
    try:
        async with db() as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO ratings(match_id,from_user,to_user,stars) VALUES(?,?,?,?)",
                (mid, c.from_user.id, to_user, stars)
            )
            await conn.commit()
    except Exception:
        pass

    try:
        await safe_edit_message(c.message, text="Спасибо! Оценка сохранена.", reply_markup=None)
    except Exception:
        pass
    await c.answer("Оценка учтена.")

@dp.callback_query(F.data.regexp(r"^complain:\d+$"))
async def cb_complain(c: CallbackQuery, state: FSMContext):
    mid = int(c.data.split(":")[1])

    async with db() as conn:
        cur = await conn.execute("SELECT a_id,b_id FROM matches WHERE id=?", (mid,))
        row = await cur.fetchone()
    if not row:
        return await c.answer("Матч не найден.", show_alert=True)

    a_id, b_id = int(row[0]), int(row[1])
    about_id = b_id if c.from_user.id == a_id else a_id
    await state.set_state(ComplaintState.wait_text)
    await state.update_data(mid=mid, about_id=about_id)

    await c.answer()
    try:
        await safe_edit_message(c.message, text="Опиши жалобу одним сообщением. Чем подробнее — тем лучше.", reply_markup=None)
    except Exception:
        pass

@dp.callback_query(F.data == "sub_check")
async def sub_check(c: CallbackQuery):
    if await is_subscribed(c.from_user.id):
        try:
            await c.message.edit_text("✅ Спасибо за подписку!")
        except Exception:
            pass
        await c.answer("Подписка подтверждена!")
        await bot.send_message(
            c.from_user.id, intro_text(), disable_web_page_preview=True, reply_markup=main_menu()
        )
    else:
        await c.answer(
            "Похоже, ты ещё не подписался. Нажми «Подписаться», а потом снова «Проверить».",
            show_alert=False
        )

@dp.callback_query(F.data.startswith("shop_buy:"))
async def shop_buy(c: CallbackQuery):
    if await get_role(c.from_user.id) == "admin":
        await c.answer("Админ не может покупать товары.", show_alert=True)
        return
    item_id = int(c.data.split(":")[1])
    item = await get_item(item_id)
    if not item:
        await c.answer("Товар уже недоступен.", show_alert=True)
        return
    _id, name, price, type_, payload = item
    pts = await get_points(c.from_user.id)
    if pts < price:
        await c.answer(f"Не хватает очков. Нужно {price}, у тебя {pts}.", show_alert=True)
        return

    await add_points(c.from_user.id, -price)
    applied_msg = ""
    if type_ == "status":
        # Кладём в инвентарь и сразу экипируем
        await add_status_to_inventory(c.from_user.id, payload)
        await set_status(c.from_user.id, payload)
        applied_msg = f"Теперь твой статус: «{payload}». (Добавлен в инвентарь)"
    elif type_ == "privilege":
        applied_msg = f"Привилегия активирована: {payload}"
    async with db() as conn:
        await conn.execute("INSERT INTO purchases(user_id,item_id) VALUES(?,?)", (c.from_user.id, _id))
        await conn.commit()
    new_pts = await get_points(c.from_user.id)
    try:
        await c.message.edit_text(
            f"✅ Покупка «{name}» за {price}💰 успешна!\n{applied_msg}\nБаланс: {new_pts}.", reply_markup=None
        )
    except Exception:
        pass
    await c.answer("Готово!")

# ============================================================
#                        COMMAND GUARD
# ============================================================

@dp.message(F.text.in_({"🧭 Режимы", "👤 Анкета", "🆘 Поддержка", "📇 Просмотр анкет",
                        "🕵️ Анонимный чат", "💰 Баланс", "⭐️ Оценить собеседника", "🚩 Пожаловаться"}))
async def block_menu_buttons_in_chat(m: Message):
    if await is_chat_active(m.from_user.id):
        # Ничего не делаем, чтобы relay_chat обработал сообщение как обычный текст
        raise SkipHandler
    # если чата нет — этот хэндлер пропускаем, чтобы сработали целевые обработчики
    raise SkipHandler

@dp.message(ComplaintState.wait_text)
async def complaint_text(m: Message, state: FSMContext):
    d = await state.get_data()
    mid = int(d.get("mid")); about_id = int(d.get("about_id"))
    text = (m.text or "").strip()

    async with db() as conn:
        await conn.execute(
            "INSERT INTO complaints(match_id,from_user,about_user,text) VALUES(?,?,?,?)",
            (mid, m.from_user.id, about_id, text)
        )
        await conn.commit()

    # шлём админам
    for admin_id in (ADMIN_IDS or []):
        try:
            await bot.send_message(
                admin_id,
                f"🚩 Жалоба от <code>{m.from_user.id}</code> на <code>{about_id}</code>\n"
                f"Матч: <code>{mid}</code>\n\n{text}"
            )
        except Exception:
            pass

    await state.clear()
    await m.answer("Жалоба отправлена админам. Спасибо!", reply_markup=(await menu_for(m.from_user.id)))

@dp.message(F.text == "⭐️ Оценить собеседника")
async def rate_from_menu(m: Message):
    if await deny_actions_during_chat(m):
        return
    info = await last_match_info(m.from_user.id)
    if not info:
        return await m.answer("Пока не с кем — ещё не было диалогов.", reply_markup=(await menu_for(m.from_user.id)))
    mid, peer, _active = info

    # проверим, не оценивал ли уже этот матч
    async with db() as conn:
        cur = await conn.execute("SELECT 1 FROM ratings WHERE match_id=? AND from_user=?", (mid, m.from_user.id))
        done = await cur.fetchone()
    if done:
        return await m.answer("Последний диалог уже оценён. Спасибо!", reply_markup=(await menu_for(m.from_user.id)))

    await m.answer("Оцени последнего собеседника:", reply_markup=rate_or_complain_kb(mid))

@dp.message(F.text == "🚩 Пожаловаться")
async def complain_from_menu(m: Message, state: FSMContext):
    if await deny_actions_during_chat(m):
        return
    info = await last_match_info(m.from_user.id)
    if not info:
        return await m.answer("Пока не на кого — ещё не было диалогов.", reply_markup=(await menu_for(m.from_user.id)))
    mid, peer, _active = info
    await state.set_state(ComplaintState.wait_text)
    await state.update_data(mid=mid, about_id=peer)
    await m.answer("Опиши жалобу одним сообщением. Чем подробнее — тем лучше.", reply_markup=cancel_kb())

@dp.message(F.text.regexp(r"^/"))
async def block_slash_cmds_in_chat(m: Message):
    if await is_chat_active(m.from_user.id):
        await _materialize_session_if_needed(m.from_user.id)
        await m.answer(BLOCK_TXT, reply_markup=ReplyKeyboardRemove())
        return
    # важное: отдать обработку конкретным командным хэндлерам
    raise SkipHandler

# ============================================================
#                         BASIC COMMANDS
# ============================================================

@dp.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    if await deny_actions_during_chat(m):
        return
    if not await gate_subscription(m):
        return

    await ensure_user(m.from_user.id)

    # deep-link /start ref_<id>
    try:
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) == 2:
            arg = parts[1]
            inviter_id: Optional[int] = None
            if arg.startswith("r_"):               # новый формат непрозрачного кода
                code = arg[2:]
                inviter_id = await inviter_by_code(code)
            elif arg.startswith("ref_"):           # старый формат (на всякий случай)
                inviter_id = int(arg[4:])
            if inviter_id and await register_referral(inviter_id, m.from_user.id):
                await add_points(inviter_id, g_ref_bonus())
                try:
                    await bot.send_message(inviter_id, f"🎉 По твоей ссылке пришёл новый пользователь! +{g_ref_bonus()} очков.")
                except Exception:
                    pass
    except Exception:
        pass

    u = await get_user(m.from_user.id)
    if not u or not u[1] or not u[2]:
        await m.answer(intro_text(), disable_web_page_preview=True)
        await m.answer(
            "Сначала выберем твой пол и кого ищешь. Затем, при желании, можно заполнить анкету для деанонимизации.",
            reply_markup=gender_self_kb()
        )
        await state.update_data(start_form_after_prefs=True, is_refill=False)
        await state.set_state(GState.pick_gender)
        if not (m.from_user.username or ""):
            await m.answer("ℹ️ Для деанонимизации (анкеты) нужен @username в Telegram. Его можно создать позже.")
        return

    await m.answer("Главное меню.", reply_markup=(await menu_for(m.from_user.id)))

@dp.message(Command("help"))
async def cmd_help(m: Message):
    if await deny_actions_during_chat(m):
        return
    await m.answer(
        "ℹ️ Помощь\n\n"
        "Основное:\n"
        "• /profile — твой профиль, баланс, покупки\n"
        "• /market — магазин статусов и привилегий\n"
        "• /ref — реферальная ссылка и статистика\n"
        "• /help — эта справка\n\n"
        "Анонимный чат:\n"
        "• «🕵️ Анонимный чат» → «🔎 Найти собеседника»\n"
        "• !next — следующий собеседник\n"
        "• !stop — завершить чат\n"
        "• !reveal — запросить взаимное раскрытие (если анкеты у обоих)\n\n"
        "Навигация:\n"
        "• «❌ Отмена» — выйти из текущего режима к главному меню.",
        reply_markup=(await menu_for(m.from_user.id))
    )

@dp.message(Command("profile"))
async def cmd_profile(m: Message):
    if await deny_actions_during_chat(m):
        return

    await ensure_user(m.from_user.id)
    u = await get_user_or_create(m.from_user.id)
    await ensure_free_statuses(m.from_user.id)  # гарантируем бесплатные статусы
    pts = await get_points(m.from_user.id)
    status = await get_status(m.from_user.id) or "—"
    avg, cnt = await get_avg_rating(m.from_user.id)
    rate_line = f"• Рейтинг: {avg:.1f} ({cnt})" if avg is not None else "• Рейтинг: — (0)"
    ref_cnt = await count_referrals(m.from_user.id)
    spent_total, last5 = await purchases_summary(m.from_user.id)

    gender = u[1] or "—"
    seeking = u[2] or "—"
    ready = "Да" if (u[3] == 1) else "Нет"
    uname = u[9] or "—"

    inv = await get_status_inventory(m.from_user.id)
    inv_txt = "нет" if not inv else ", ".join(inv)

    lines = [
        "<b>Профиль</b>",
        "",
        "<b>Основное:</b>",
        f"• ID: <code>{m.from_user.id}</code>",
        f"• Username: {uname}",
        f"• Пол/интерес: {gender} → {seeking}",
        f"• Анкета заполнена: {ready}",
        f"• Баланс: <b>{pts}</b> очков",
        f"• Текущий статус: {status}",
        rate_line,
        "",
        "<b>Инвентарь статусов:</b>",
        inv_txt if inv_txt else "нет",
        "",
        f"👥 Рефералов: {ref_cnt}",
        f"🛒 Сумма покупок: {spent_total} очков",
    ]
    if last5:
        lines.append("\n<b>Последние покупки:</b>")
        lines += [f"• {n}" for n in last5]

    kb = statuses_kb(inv, await get_status(m.from_user.id)) if inv else None
    await m.answer("\n".join(lines), reply_markup=kb or (await menu_for(m.from_user.id)))

@dp.message(Command("market"))
async def cmd_market(m: Message):
    if await deny_actions_during_chat(m):
        return
    if await get_role(m.from_user.id) == "admin":
        return await m.answer("Ты админ и не можешь покупать. Используй /admin.", reply_markup=(await menu_for(m.from_user.id)))
    items = await list_items()
    if not items:
        return await m.answer("🛍 Магазин пока пуст.", reply_markup=(await menu_for(m.from_user.id)))
    await m.answer("🛍 Магазин статусов и привилегий. Выбери товар:", reply_markup=shop_kb(items))

@dp.message(Command("ref"))
async def cmd_ref(m: Message):
    if await deny_actions_during_chat(m):
        return
    await ensure_user(m.from_user.id)
    me = await bot.get_me()
    bot_user = me.username or ""
    code = await get_or_create_ref_code(m.from_user.id)
    link = f"https://t.me/{bot_user}?start=r_{code}" if bot_user else "—"
    cnt = await count_referrals(m.from_user.id)
    bonus = cnt * g_ref_bonus()
    await m.answer(
        "👥 Реферальная программа\n\n"
        f"Твоя ссылка: {link}\n"
        f"Приведено пользователей: <b>{cnt}</b>\n"
        f"Начислено бонусов: <b>{bonus}</b> очков\n\n"
        f"Начисление: +{g_ref_bonus()} очков за каждого нового пользователя.",
        disable_web_page_preview=True
    )

@dp.message(Command("daily"))
async def daily(m: Message):
    if await deny_actions_during_chat(m):
        return
    await ensure_user(m.from_user.id)

    can_take, remaining = await can_take_daily_cooldown(m.from_user.id)
    if not can_take:
        return await m.answer(
            f"Сегодня бонус уже получен. Снова можно через {_fmt_hhmmss(remaining)}."
        )

    await add_points(m.from_user.id, g_daily_bonus())
    await mark_daily_taken(m.from_user.id)
    pts = await get_points(m.from_user.id)
    await m.answer(f"🎁 Ежедневный бонус +{g_daily_bonus()} очков! Текущий баланс: {pts}.")

COOLDOWN_SECONDS = 24 * 60 * 60  # 24 часа

def _fmt_hhmmss(sec: int) -> str:
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

async def can_take_daily_cooldown(tg_id: int) -> tuple[bool, int]:
    """
    Возвращает (can_take, remaining_seconds).
    can_take == True, если с последнего забора прошло >= 24 часов.
    """
    async with db() as conn:
        cur = await conn.execute("SELECT COALESCE(last_daily, 0) FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        last = int(row[0] if row else 0)

    if last == 0:
        return True, 0

    elapsed = int(time.time()) - last
    if elapsed >= COOLDOWN_SECONDS:
        return True, 0
    return False, COOLDOWN_SECONDS - elapsed

async def mark_daily_taken(tg_id: int):
    await set_user_fields(tg_id, last_daily=int(time.time()))

@dp.message(Command("admin"))
async def admin_panel(m: Message, state: FSMContext):
    if await deny_actions_during_chat(m):
        return
    await ensure_user(m.from_user.id)
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        await m.answer("Доступ запрещён.")
        return
    await state.clear()
    await m.answer("🛠 Панель администратора", reply_markup=admin_main_kb())

@dp.callback_query(F.data == "admin:home")
async def admin_home(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.", show_alert=True)
    await state.clear()
    await safe_edit_message(c.message, text="🛠 Панель администратора", reply_markup=admin_main_kb())

@dp.callback_query(F.data.startswith("use_status:"))
async def cb_use_status(c: CallbackQuery):
    val = c.data.split(":", 1)[1]
    if val == "__none__":
        await set_status(c.from_user.id, None)
        await c.answer("Статус снят.")
    else:
        inv = await get_status_inventory(c.from_user.id)
        if val not in inv:
            return await c.answer("У тебя нет такого статуса.", show_alert=True)
        await set_status(c.from_user.id, val)
        await c.answer(f"Выбран статус: «{val}».")
    # обновим клавиатуру инвентаря прямо под профилем
    inv = await get_status_inventory(c.from_user.id)
    try:
        await safe_edit_message(c.message, reply_markup=statuses_kb(inv, await get_status(c.from_user.id)))
    except Exception:
        pass

@dp.callback_query(F.data == "admin:shop")
async def admin_shop(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.", show_alert=True)
    await safe_edit_message(c.message, text="🛍 Магазин", reply_markup=admin_shop_kb())

@dp.callback_query(F.data == "admin:shop:list")
async def admin_shop_list(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    items = await list_items()
    txt = "📦 Товары:\n" + ("\n".join([f"{i[0]}. {i[1]} — {i[2]}💰 [{i[3]}] {i[4] or ''}" for i in items]) or "пусто")
    await safe_edit_message(c.message, text=txt, reply_markup=admin_shop_kb())

@dp.callback_query(F.data == "admin:shop:add")
async def admin_shop_add(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminAddItem.wait_name)
    await c.message.edit_text("🧩 Шаг 1/4: Введи название товара\n\nНапример: <code>Самый Скромный</code>")

@dp.callback_query(F.data == "admin:grant")
async def admin_grant_start(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.", show_alert=True)
    await state.set_state(AdminGrantPoints.wait_user_id)
    await c.message.edit_text("💳 Кому начислить очки? Введи <code>tg_id</code> пользователя.")


@dp.message(AdminGrantPoints.wait_user_id)
async def admin_grant_user(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return await m.answer("Нет доступа.")
    try:
        uid = int((m.text or "").strip())
    except Exception:
        return await m.answer("Нужен целый <code>tg_id</code>. Попробуй ещё.")
    await ensure_user(uid)
    await state.update_data(grant_uid=uid)
    await state.set_state(AdminGrantPoints.wait_amount)
    await m.answer(f"Сколько очков начислить пользователю <code>{uid}</code>? "
                   "Можно отрицательное число, чтобы списать.")

@dp.message(AdminGrantPoints.wait_amount)
async def admin_grant_amount(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return await m.answer("Нет доступа.")
    try:
        amount = int((m.text or "").strip())
    except Exception:
        return await m.answer("Нужно целое число (например: 50 или -20). Попробуй ещё.")
    data = await state.get_data()
    uid = int(data.get("grant_uid"))
    await ensure_user(uid)
    await add_points(uid, amount)
    new_pts = await get_points(uid)
    await state.clear()
    try:
        await bot.send_message(uid, f"💳 Тебе {'начислено' if amount>=0 else 'списано'} {abs(amount)} очков. Баланс: {new_pts}.")
    except Exception:
        pass
    await m.answer(f"✅ Готово. Пользователь <code>{uid}</code>: изменение {amount} очков. Текущий баланс: {new_pts}.",
                   reply_markup=admin_main_kb())

def admin_reply_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🛠 Админ"))
    return kb.as_markup(resize_keyboard=True)

async def menu_for(user_id: int) -> ReplyKeyboardMarkup:
    role = await get_role(user_id)
    if role == "admin" or user_id in ADMIN_IDS:
        return admin_reply_menu()
    return main_menu()

@dp.message(F.text == "🛠 Админ")
async def open_admin_from_button(m: Message, state: FSMContext):
    return await admin_panel(m, state)

@dp.message(AdminAddItem.wait_name)
async def admin_shop_add_name(m: Message, state: FSMContext):
    await state.update_data(name=(m.text or "").strip())
    await state.set_state(AdminAddItem.wait_price)
    await m.answer("🧩 Шаг 2/4: Цена (целое число)\n\nНапример: <code>50</code>")

@dp.message(AdminAddItem.wait_price)
async def admin_shop_add_price(m: Message, state: FSMContext):
    try:
        price = int((m.text or "").strip())
        if price < 0: raise ValueError
    except Exception:
        return await m.answer("Некорректное число. Попробуй ещё раз.")
    await state.update_data(price=price)
    await state.set_state(AdminAddItem.wait_type)
    await m.answer("🧩 Шаг 3/4: Тип — напиши <code>status</code> или <code>privilege</code>")

@dp.message(AdminAddItem.wait_type)
async def admin_shop_add_type(m: Message, state: FSMContext):
    t = (m.text or "").strip().lower()
    if t not in {"status", "privilege"}:
        return await m.answer("Тип должен быть <code>status</code> или <code>privilege</code>.")
    await state.update_data(type=t)
    await state.set_state(AdminAddItem.wait_payload)
    await m.answer("🧩 Шаг 4/4: Payload — текст статуса/описание привилегии")

@dp.message(AdminAddItem.wait_payload)
async def admin_shop_add_payload(m: Message, state: FSMContext):
    d = await state.get_data()
    await add_item(d["name"], d["price"], d["type"], (m.text or "").strip())
    await state.clear()
    await m.answer("✅ Товар добавлен.", reply_markup=admin_shop_kb())

@dp.callback_query(F.data == "admin:shop:del")
async def admin_shop_del(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminShopDel.wait_id)
    await c.message.edit_text("Отправь ID товара, который удалить (см. «📦 Список»).")

@dp.message(AdminShopDel.wait_id)
async def admin_shop_del_id(m: Message, state: FSMContext):
    try:
        await del_item(int((m.text or "").strip()))
        await m.answer("🗑 Удалено.", reply_markup=admin_shop_kb())
    except Exception:
        await m.answer("Не получилось удалить. Проверь ID.", reply_markup=admin_shop_kb())
    await state.clear()

@dp.callback_query(F.data == "admin:settings")
async def admin_settings(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    await safe_edit_message(c.message, text="⚙️ Настройки", reply_markup=admin_settings_kb())

@dp.message(AdminGrantPoints.wait_user_id, F.text.in_({"❌ Отмена", "🛠 Админ", "/admin"}))
@dp.message(AdminGrantPoints.wait_amount,  F.text.in_({"❌ Отмена", "🛠 Админ", "/admin"}))
async def admin_grant_cancel(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return await m.answer("Нет доступа.")
    await state.clear()
    await m.answer("Отменено. Возврат в панель администратора.", reply_markup=admin_main_kb())

@dp.callback_query(F.data.startswith("admin:set:"))
async def admin_settings_select(c: CallbackQuery, state: FSMContext):
    key = c.data.split(":", 2)[2]
    if key == "support_toggle":
        await set_setting("support_enabled", "0" if g_support_enabled() else "1")
        await safe_edit_message(c.message, text="⚙️ Настройки обновлены.", reply_markup=admin_settings_kb())
        return
    # ждём значение для числового ключа
    await state.set_state(AdminSettings.wait_value)
    await state.update_data(key=key)
    nice = {
        "inactivity_seconds": "⏱️ Неактивность (сек)",
        "block_rounds": "🔁 Блок-раундов",
        "daily_bonus_points": "🎁 Daily бонус",
        "ref_bonus_points": "🎯 Referral бонус",
    }.get(key, key)
    await c.message.edit_text(f"Введи новое значение для: <b>{nice}</b>\n(целое число)")

@dp.message(AdminSettings.wait_value)
async def admin_settings_set(m: Message, state: FSMContext):
    d = await state.get_data()
    key = d.get("key")
    try:
        val = int((m.text or "").strip())
        if val < 0: raise ValueError
    except Exception:
        return await m.answer("Нужно целое неотрицательное число. Попробуй ещё.")
    await set_setting(key, str(val))
    await state.clear()
    # маленький UX: если меняли inactivity — мягко продлим дедлайны активных чатов
    if key == "inactivity_seconds":
        now = _nowm()
        for mid in list(DEADLINE.keys()):
            DEADLINE[mid] = now + g_inactivity()
    await m.answer("✅ Сохранено.", reply_markup=admin_settings_kb())

async def list_admin_ids() -> list[int]:
    async with db() as conn:
        cur = await conn.execute("SELECT tg_id FROM users WHERE role='admin' ORDER BY tg_id ASC")
        return [int(x[0]) for x in await cur.fetchall()]

@dp.callback_query(F.data == "admin:admins")
async def admin_admins(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    ids = await list_admin_ids()
    txt = "👥 Админы:\n" + ("\n".join([f"• <code>{i}</code>" for i in ids]) or "пока пусто")
    await safe_edit_message(c.message, text=txt, reply_markup=admin_admins_kb())

@dp.callback_query(F.data == "admin:admins:add")
async def admin_admins_add(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAdmins.mode)
    await state.update_data(mode="add")
    await state.set_state(AdminAdmins.wait_user_id)
    await c.message.edit_text("Введи <code>tg_id</code> пользователя, которого сделать админом.")

@dp.callback_query(F.data == "admin:admins:del")
async def admin_admins_del(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAdmins.mode)
    await state.update_data(mode="del")
    await state.set_state(AdminAdmins.wait_user_id)
    await c.message.edit_text("Введи <code>tg_id</code> пользователя, которого лишить прав админа.")

@dp.message(AdminAdmins.wait_user_id)
async def admin_admins_apply(m: Message, state: FSMContext):
    d = await state.get_data()
    mode = d.get("mode")
    try:
        uid = int((m.text or "").strip())
    except Exception:
        return await m.answer("Нужен целый id пользователя.")
    if uid == m.from_user.id:
        return await m.answer("Нельзя менять свои права этим способом.")
    await ensure_user(uid)
    async with db() as conn:
        if mode == "add":
            await conn.execute("UPDATE users SET role='admin' WHERE tg_id=?", (uid,))
            await conn.commit()
            await m.answer(f"✅ Пользователь {uid} теперь админ.", reply_markup=admin_admins_kb())
        else:
            await conn.execute("UPDATE users SET role='user' WHERE tg_id=?", (uid,))
            await conn.commit()
            await m.answer(f"✅ Пользователь {uid} разжалован.", reply_markup=admin_admins_kb())
    await state.clear()

@dp.callback_query(F.data == "admin:support")
async def admin_support_menu(c: CallbackQuery, state: FSMContext):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return
    await c.message.edit_text("Диалоги саппорта (открытые):")
    async with db() as conn:
        cur = await conn.execute("""
            SELECT from_user, MAX(ts) AS last_ts
            FROM support_msgs
            WHERE status='open'
            GROUP BY from_user
            ORDER BY last_ts DESC
            LIMIT 10
        """)
        users = await cur.fetchall()
    if not users:
        return await bot.send_message(c.from_user.id, "Пусто. Новых обращений нет.", reply_markup=admin_main_kb())
    for (uid, _) in users:
        kb = InlineKeyboardBuilder()
        kb.button(text="✍️ Ответить", callback_data=f"admin:support:reply:{uid}")
        kb.button(text="✅ Закрыть", callback_data=f"sup_close:{uid}")  # переиспользуем твой
        kb.adjust(2)
        await bot.send_message(c.from_user.id, f"<b>#{uid}</b> — открыть диалог и ответить:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("admin:support:reply:"))
async def admin_support_reply_start(c: CallbackQuery, state: FSMContext):
    uid = int(c.data.split(":")[-1])
    await state.set_state(AdminSupportReply.wait_text)
    await state.update_data(uid=uid)
    await c.answer()
    await bot.send_message(c.from_user.id, f"Напиши ответ для пользователя <code>{uid}</code> (одним сообщением).")

@dp.message(AdminSupportReply.wait_text)
async def admin_support_reply_send(m: Message, state: FSMContext):
    d = await state.get_data()
    uid = d.get("uid")
    await bot.send_message(uid, f"🛠 Ответ админа:\n{m.text}")
    # фиксируем как «закрыто, если хочешь»: можно не закрывать автоматически
    await m.answer("✅ Ответ отправлен.", reply_markup=admin_main_kb())
    await state.clear()

@dp.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(c: CallbackQuery, state: FSMContext):
    await state.set_state(AdminBroadcast.wait_text)
    await c.message.edit_text("Введи текст рассылки (уйдёт всем пользователям).")

@dp.message(AdminBroadcast.wait_text)
async def admin_broadcast_run(m: Message, state: FSMContext):
    text = m.text or ""
    await state.clear()
    # собираем id пользователей
    uids = []
    async with db() as conn:
        cur = await conn.execute("SELECT tg_id FROM users")
        uids = [int(x[0]) for x in await cur.fetchall()]
    ok = 0
    for uid in uids:
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)  # мягкий троттлинг
    await m.answer(f"📣 Разослано: {ok}/{len(uids)}", reply_markup=admin_main_kb())

@dp.callback_query(F.data == "admin:stats")
async def admin_stats(c: CallbackQuery):
    async with db() as conn:
        ucnt = (await (await conn.execute("SELECT COUNT(*) FROM users")).fetchone())[0]
        qcnt = (await (await conn.execute("SELECT COUNT(*) FROM queue")).fetchone())[0]
        mact = (await (await conn.execute("SELECT COUNT(*) FROM matches WHERE active=1")).fetchone())[0]
        mtotal = (await (await conn.execute("SELECT COUNT(*) FROM matches")).fetchone())[0]
        sup_open = (await (await conn.execute("SELECT COUNT(*) FROM support_msgs WHERE status='open'")).fetchone())[0]
        ref_cnt = (await (await conn.execute("SELECT COUNT(*) FROM referrals")).fetchone())[0]
    txt = (
        "<b>📊 Статистика</b>\n\n"
        f"👤 Пользователей: <b>{ucnt}</b>\n"
        f"🧍‍♀️🧍‍♂️ В очереди: <b>{qcnt}</b>\n"
        f"💬 Активных чатов: <b>{mact}</b>\n"
        f"💬 Всего чатов: <b>{mtotal}</b>\n"
        f"🆘 Открытых тикетов: <b>{sup_open}</b>\n"
        f"🎯 Рефералов всего: <b>{ref_cnt}</b>\n"
        f"\n⚙️ Неактивность: {g_inactivity()} c | Блок-раундов: {g_block_rounds()}\n"
        f"🎁 Daily: {g_daily_bonus()} | 🎯 Referral: {g_ref_bonus()}\n"
        f"🆘 Support: {'ON' if g_support_enabled() else 'OFF'}"
    )
    await safe_edit_message(c.message, text=txt, reply_markup=admin_main_kb())

@dp.callback_query(F.data == "adm_list")
async def adm_list(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.")
    items = await list_items()
    txt = "📦 Товары:\n" + ("\n".join([f"{i[0]}. {i[1]} — {i[2]}💰 [{i[3]}] {i[4] or ''}" for i in items]) or "пусто")
    await c.message.edit_text(txt)

@dp.callback_query(F.data == "adm_add")
async def adm_add(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.")
    await c.message.edit_text(
        "Формат:\n<code>/add_item Название | Цена | status|privilege | payload</code>\n"
        "Пример:\n<code>/add_item Самый Скромный | 50 | status | Самый Скромный</code>"
    )

@dp.message(Command("grant"))
async def admin_grant_cmd(m: Message):
    if await deny_actions_during_chat(m):
        return
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        return await m.answer("Нет доступа.")
    # Формат: /grant <tg_id> <amount> [reason...]
    parts = (m.text or "").strip().split(maxsplit=3)
    if len(parts) < 3:
        return await m.answer("Формат: <code>/grant &lt;tg_id&gt; &lt;amount&gt; [reason]</code>\n"
                              "Например: <code>/grant 123456789 50 За активность</code>")
    try:
        _, uid_s, amt_s, *maybe_reason = parts
        uid = int(uid_s)
        amount = int(amt_s)
        reason = maybe_reason[0] if maybe_reason else ""
    except Exception:
        return await m.answer("Проверь формат. Пример: <code>/grant 123456789 50 За активность</code>")
    await ensure_user(uid)
    await add_points(uid, amount)
    new_pts = await get_points(uid)
    try:
        note = f"\nПричина: {reason}" if reason else ""
        await bot.send_message(uid, f"💳 Тебе {'начислено' if amount>=0 else 'списано'} {abs(amount)} очков.{note}\nБаланс: {new_pts}.")
    except Exception:
        pass
    await m.answer(f"✅ Пользователю <code>{uid}</code> {'начислено' if amount>=0 else 'списано'} {abs(amount)} очков."
                   f"\nТекущий баланс: {new_pts}.")

@dp.message(Command("add_item"))
async def adm_add_cmd(m: Message):
    if await deny_actions_during_chat(m):
        return
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        return await m.answer("Нет доступа.")
    try:
        _, rest = m.text.split(" ", 1)
        name, price, type_, payload = [x.strip() for x in rest.split("|", 3)]
        assert type_ in ("status", "privilege")
        price = int(price)
    except Exception:
        return await m.answer("Неверный формат. Пример:\n/add_item Название | 50 | status | Текст статуса")
    await add_item(name, price, type_, payload)
    await m.answer("✅ Товар добавлен.")

@dp.callback_query(F.data == "adm_del")
async def adm_del(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.")
    await c.message.edit_text("Отправь команду: <code>/del_item ID</code>")

@dp.message(Command("del_item"))
async def adm_del_cmd(m: Message):
    if await deny_actions_during_chat(m):
        return
    if await get_role(m.from_user.id) != "admin" and m.from_user.id not in ADMIN_IDS:
        return await m.answer("Нет доступа.")
    try:
        _cmd, sid = m.text.strip().split(maxsplit=1)
        await del_item(int(sid))
        await m.answer("🗑 Удалено.")
    except Exception:
        await m.answer("Формат: /del_item 3")

# ============================================================
#                          MODES & MENUS
# ============================================================

@dp.message(F.text == "🧭 Режимы")
async def modes_entry(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) == "admin" or m.from_user.id in ADMIN_IDS:
        await m.answer("Этот раздел недоступен админу. Открой панель: /admin", reply_markup=admin_reply_menu())
        return
    if await deny_actions_during_chat(m):
        return
    if not await gate_subscription(m):
        return
    await state.clear()
    await m.answer(
        "Выбери режим работы бота:\n\n"
        "<b>📇 Просмотр анкет</b> — лента анкет (в разработке)\n\n"
        "<b>🕵️ Анонимный чат</b> — случайные собеседники с возможностью взаимного раскрытия",
        reply_markup=modes_kb()
    )

@dp.message(F.text == "📇 Просмотр анкет")
async def mode_cards(m: Message):
    if await deny_actions_during_chat(m):
        return
    await m.answer("Раздел «Просмотр анкет» — <b>в разработке</b>.", reply_markup=modes_kb())

@dp.message(F.text == "🕵️ Анонимный чат")
async def mode_anon_chat(m: Message):
    if await get_role(m.from_user.id) == "admin" or m.from_user.id in ADMIN_IDS:
        await m.answer("Этот раздел недоступен админу. Открой панель: /admin", reply_markup=admin_reply_menu())
        return
    if await deny_actions_during_chat(m):
        return
    if not await gate_subscription(m):
        return
    await m.answer(
        "Режим «Анонимный чат». Здесь можно искать случайного собеседника.\n"
        "Используй кнопки ниже.",
        reply_markup=anon_chat_menu_kb()
    )

@dp.message(F.text == "💰 Баланс")
async def show_balance(m: Message):
    if await deny_actions_during_chat(m):
        return
    await ensure_user(m.from_user.id)
    pts = await get_points(m.from_user.id)
    st = await get_status(m.from_user.id)
    st_txt = f"\nТвой статус: «{st}»" if st else ""
    await m.answer(f"💰 Твой баланс: <b>{pts}</b> очков.{st_txt}", reply_markup=(await menu_for(m.from_user.id)))

# ========== GLOBAL «Отмена» ==========
@dp.message(F.text == "❌ Отмена")
async def global_cancel(m: Message, state: FSMContext):
    # ⛔ Во время анонимного чата эта кнопка не должна ничего отменять.
    # Отпускаем апдейт дальше — его подхватит relay_chat и перешлёт как обычный текст.
    if await is_chat_active(m.from_user.id):
        raise SkipHandler

    cur_state = await state.get_state()
    data = await state.get_data()
    refill_mode = bool(data.get("refill_mode"))

    # 1) Если на форме анкеты
    if cur_state in {
        RevealForm.name.state, RevealForm.faculty.state, RevealForm.age.state,
        RevealForm.about.state, RevealForm.photos.state,
    }:
        await state.clear()
        if refill_mode:
            await m.answer("Перезаполнение отменено. Старая анкета сохранена.", reply_markup=(await menu_for(m.from_user.id)))
        else:
            await set_user_fields(
                m.from_user.id, reveal_ready=0,
                first_name=None, last_name=None, faculty=None,
                age=None, about=None, photo1=None, photo2=None, photo3=None
            )
            await m.answer("Анкета отменена.", reply_markup=(await menu_for(m.from_user.id)))
        return

    # 2) Если в режиме поддержки — выходим
    if cur_state == SupportState.waiting.state:
        await state.clear()
        await m.answer("Вы вышли из поддержки.", reply_markup=(await menu_for(m.from_user.id)))
        return

    # 3) Если стоит в очереди — отменяем поиск
    if await in_queue(m.from_user.id):
        await dequeue(m.from_user.id)
        await m.answer("Поиск отменён.", reply_markup=(await menu_for(m.from_user.id)))
        return

    # 4) Иначе просто меню
    await m.answer("Главное меню.", reply_markup=(await menu_for(m.from_user.id)))

# ================== Профиль/анкета (вход) ==================
@dp.message(F.text == "👤 Анкета")
async def show_or_edit_reveal(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) == "admin" or m.from_user.id in ADMIN_IDS:
        return await m.answer("Раздел «Анкета» недоступен для администраторов. Открой /admin.",
                              reply_markup=admin_main_kb())
    if await deny_actions_during_chat(m):
        return
    if not await gate_subscription(m):
        return
    await ensure_user(m.from_user.id)

    if await in_queue(m.from_user.id):
        await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        return

    u = await get_user(m.from_user.id)
    if not u or not u[1] or not u[2]:
        await m.answer("Сначала выбери свой пол.", reply_markup=gender_self_kb())
        await state.update_data(start_form_after_prefs=True, is_refill=False)
        await state.set_state(GState.pick_gender)
        if not (m.from_user.username or ""):
            await m.answer("ℹ️ Для анкеты нужен @username. Его можно создать в настройках Telegram.")
        return

    ready = bool(u[3]) if u else False
    if not ready:
        await m.answer("Анкета не заполнена. Можно заполнить сейчас.", reply_markup=reveal_entry_menu())
        return

    txt = format_profile_text(u)
    photos = [p for p in (u[10], u[11], u[12]) if p]
    if photos:
        for p in photos[:-1]:
            await m.answer_photo(p)
        await m.answer_photo(photos[-1], caption=txt)
    else:
        await m.answer(txt)
    await m.answer("Что дальше?", reply_markup=reveal_entry_menu())

@dp.message(F.text == "✏️ Заполнить / Перезаполнить")
async def fill_or_refill_btn(m: Message, state: FSMContext):
    if await deny_actions_during_chat(m):
        return
    if not await gate_subscription(m):
        return

    await ensure_user(m.from_user.id)

    # если в поиске — только отмена
    if await in_queue(m.from_user.id):
        await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        return

    u = await get_user(m.from_user.id)

    # если не выбраны пол/кого ищешь — сначала спросим их
    if not u or not u[1] or not u[2]:
        await m.answer("Сначала выберем твой пол.", reply_markup=gender_self_kb())
        await state.update_data(start_form_after_prefs=True, is_refill=False, refill_mode=False)
        await state.set_state(GState.pick_gender)
        if not (m.from_user.username or ""):
            await m.answer("ℹ️ Для анкеты нужен @username. Его можно создать позже в настройках Telegram.")
        return

    # если анкета уже была — запускаем режим перезаполнения
    ready = bool(u[3])
    await state.update_data(refill_mode=ready, is_refill=ready)

    # для анкеты нужен username
    if not await _require_username(m):
        await state.clear()
        return

    await start_reveal_form(m, state, is_refill=ready)


@dp.message(F.text == "⬅️ В главное меню")
async def back_to_main_menu(m: Message, state: FSMContext):
    if await deny_actions_during_chat(m):
        return
    await state.clear()
    await m.answer("Главное меню.", reply_markup=(await menu_for(m.from_user.id)))

# ================== Выбор пола / кого ищем ==================
@dp.message(GState.pick_gender)
async def pick_gender_msg(m: Message, state: FSMContext):
    await ensure_user(m.from_user.id)
    text = (m.text or "").strip().casefold()
    if text not in {"я девушка", "я парень"}:
        await m.answer("Выбери одну из кнопок: «Я девушка» или «Я парень».", reply_markup=gender_self_kb())
        return
    gender = "Девушка" if text == "я девушка" else "Парень"

    data = await state.get_data()
    if data.get("refill_mode"):
        await state.update_data(new_gender=gender)
    else:
        await set_user_fields(m.from_user.id, gender=gender)

    await m.answer("Кто тебе интересен?", reply_markup=seeking_kb())
    await state.set_state(GState.pick_seeking)

@dp.message(GState.pick_seeking)
async def pick_seeking_msg(m: Message, state: FSMContext):
    await ensure_user(m.from_user.id)
    text = (m.text or "").strip()
    if text not in {"Девушки", "Парни", "Не важно"}:
        await m.answer("Выбери: «Девушки», «Парни» или «Не важно».", reply_markup=seeking_kb())
        return

    data = await state.get_data()
    refill_mode = data.get("refill_mode")
    if refill_mode:
        await state.update_data(new_seeking=text)
        if not await _require_username(m):
            await state.clear()
            return
        await start_reveal_form(m, state, is_refill=True)
        return

    await set_user_fields(m.from_user.id, seeking=text)
    after_prefs = data.get("start_form_after_prefs", False)
    is_refill = data.get("is_refill", False)
    await state.update_data(start_form_after_prefs=False)
    await state.clear()

    if after_prefs:
        if not (m.from_user.username or ""):
            await m.answer("Параметры сохранены. Для анкеты нужен @username. Его можно настроить в Telegram позже.",
                           reply_markup=(await menu_for(m.from_user.id)))
            return
        await start_reveal_form(m, state, is_refill=is_refill)
        return

    await m.answer("Параметры сохранены.", reply_markup=main_menu())

# ================== Reveal Form ==================
async def start_reveal_form(m: Message, state: FSMContext, is_refill: bool):
    await m.answer(
        "Анкета для взаимного раскрытия. Её увидят только при взаимном !reveal.\n"
        "Анкету нельзя оставить неполной — можно заполнить целиком или нажать «❌ Отмена».",
        reply_markup=cancel_kb()
    )
    await m.answer("Как тебя зовут?", reply_markup=cancel_kb())
    await state.update_data(is_refill=is_refill)
    await state.set_state(RevealForm.name)

@dp.message(RevealForm.name)
async def rf_name(m: Message, state: FSMContext):
    parts = (m.text or "").strip().split()
    first = parts[0] if parts else ""
    last = " ".join(parts[1:]) if len(parts) > 1 else ""
    data = await state.get_data()
    if data.get("refill_mode"):
        await state.update_data(new_first=first, new_last=last)
    else:
        await set_user_fields(m.from_user.id, first_name=first, last_name=last)
    await m.answer("С какого ты института?", reply_markup=faculties_kb())
    await state.set_state(RevealForm.faculty)

@dp.callback_query(RevealForm.faculty, F.data.startswith("fac:"))
async def rf_fac(c: CallbackQuery, state: FSMContext):
    idx = int(c.data.split(":")[1])
    fac = FACULTIES[idx]
    data = await state.get_data()
    if data.get("refill_mode"):
        await state.update_data(new_faculty=fac)
    else:
        await set_user_fields(c.from_user.id, faculty=fac)
    await c.message.edit_text(f"Факультет: <b>{fac}</b>")
    await c.message.answer("Сколько тебе лет?", reply_markup=cancel_kb())
    await state.set_state(RevealForm.age)
    await c.answer()

@dp.message(RevealForm.age)
async def rf_age(m: Message, state: FSMContext):
    try:
        age = int((m.text or "").strip())
        if not (17 <= age <= 99):
            raise ValueError
    except Exception:
        await m.answer("Возраст числом 17–99, попробуй ещё раз.", reply_markup=cancel_kb())
        return
    data = await state.get_data()
    if data.get("refill_mode"):
        await state.update_data(new_age=age)
    else:
        await set_user_fields(m.from_user.id, age=age)

    u = await get_user_or_create(m.from_user.id)
    refill = bool(data.get("is_refill"))
    has_prev_about = bool(u[8])
    await m.answer("Расскажи о себе (до 300 символов) или нажми «Пропустить».",
                   reply_markup=about_kb(refill=refill, has_prev=has_prev_about))
    await state.set_state(RevealForm.about)

@dp.message(RevealForm.about, F.text.casefold() == "пропустить")
async def rf_about_skip(m: Message, state: FSMContext):
    data = await state.get_data()
    uname = m.from_user.username or ""
    if data.get("refill_mode"):
        await state.update_data(new_about=None, new_username=(f"@{uname}" if uname else None))
    else:
        await set_user_fields(m.from_user.id, about=None)
        await set_user_fields(m.from_user.id, username=(f"@{uname}" if uname else None))

    u = await get_user_or_create(m.from_user.id)
    refill = bool(data.get("is_refill"))
    has_prev_photos = bool(u[10] or u[11] or u[12])
    await m.answer("Пришли до 3 фото (как фото).",
                   reply_markup=photos_empty_kb(refill=refill, has_prev=has_prev_photos))
    await state.set_state(RevealForm.photos)

@dp.message(RevealForm.about, F.text == "Оставить текущее")
async def rf_about_keep(m: Message, state: FSMContext):
    u = await get_user(m.from_user.id)
    if not u[8]:
        await m.answer("Описание пустое — оставлять нечего. Напиши текст или нажми «Пропустить».",
                       reply_markup=about_kb(refill=False, has_prev=False))
        return
    data = await state.get_data()
    # ничего не меняем — оставляем старое
    refill = bool(data.get("is_refill"))
    has_prev_photos = bool(u[10] or u[11] or u[12])
    await m.answer("Пришли до 3 фото (как фото).",
                   reply_markup=photos_empty_kb(refill=refill, has_prev=has_prev_photos))
    await state.set_state(RevealForm.photos)

@dp.message(RevealForm.about)
async def rf_about(m: Message, state: FSMContext):
    text_raw = (m.text or "").strip()
    if text_raw.casefold() == "пропустить":
        return await rf_about_skip(m, state)
    if text_raw and len(text_raw) > 300:
        await m.answer("Сделай описание короче (≤300 символов).")
        return

    data = await state.get_data()
    uname = m.from_user.username or ""
    if data.get("refill_mode"):
        await state.update_data(new_about=(text_raw or None), new_username=(f"@{uname}" if uname else None))
    else:
        await set_user_fields(m.from_user.id, about=(text_raw or None))
        await set_user_fields(m.from_user.id, username=(f"@{uname}" if uname else None))

    u = await get_user(m.from_user.id)
    refill = bool(data.get("is_refill"))
    has_prev_photos = bool(u[10] or u[11] or u[12])
    await m.answer("Пришли до 3 фото (как фото).",
                   reply_markup=photos_empty_kb(refill=refill, has_prev=has_prev_photos))
    await state.set_state(RevealForm.photos)

@dp.message(RevealForm.photos, F.text == "Оставить текущее")
async def rf_photos_keep(m: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("refill_mode"):
        await _commit_staged_profile(m.from_user.id, data, keep_old_photos=True)
        await state.clear()
        await m.answer("Анкета сохранена (фото оставили прежние). Теперь можно жать «🔎 Найти собеседника».",
                       reply_markup=main_menu())
        return
    await set_user_fields(m.from_user.id, reveal_ready=1)
    await state.clear()
    await m.answer("Анкета сохранена (оставили прежние фото). Теперь можно жать «🔎 Найти собеседника».",
                   reply_markup=(await menu_for(m.from_user.id)))

@dp.message(RevealForm.photos, F.photo)
async def rf_photos(m: Message, state: FSMContext):
    data = await state.get_data()
    file_id = m.photo[-1].file_id
    if data.get("refill_mode"):
        photos = list(data.get("new_photos") or [])
        if len(photos) < 3:
            photos.append(file_id)
            await state.update_data(new_photos=photos)
            idx = len(photos)
            if idx < 3:
                await m.answer(f"Фото {idx} сохранено. Ещё?", reply_markup=photos_progress_kb())
            else:
                await m.answer("Фото 3 сохранено. Нажми «Готово».", reply_markup=photos_progress_kb())
        else:
            await m.answer("Уже есть 3 фото. Нажми «Готово».", reply_markup=photos_progress_kb())
        return

    u = await get_user_or_create(m.from_user.id)
    current = [u[10], u[11], u[12]]
    if current[0] is None:
        await set_user_fields(m.from_user.id, photo1=file_id)
        await m.answer("Фото 1 сохранено. Ещё?", reply_markup=photos_progress_kb())
    elif current[1] is None:
        await set_user_fields(m.from_user.id, photo2=file_id)
        await m.answer("Фото 2 сохранено. Ещё?", reply_markup=photos_progress_kb())
    elif current[2] is None:
        await set_user_fields(m.from_user.id, photo3=file_id)
        await m.answer("Фото 3 сохранено. Нажми «Готово».", reply_markup=photos_progress_kb())
    else:
        await m.answer("Уже есть 3 фото. Нажми «Готово».", reply_markup=photos_progress_kb())

@dp.message(RevealForm.photos, F.text == "Сбросить фото")
async def rf_photos_reset(m: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("refill_mode"):
        await state.update_data(new_photos=[])
        await m.answer("Все новые фото в черновике удалены. Пришли новое фото (до 3).",
                       reply_markup=photos_empty_kb(refill=True, has_prev=True))
        return
    await set_user_fields(m.from_user.id, photo1=None, photo2=None, photo3=None)
    await m.answer("Все фото удалены. Пришли новое фото (до 3).",
        reply_markup=photos_empty_kb(refill=False, has_prev=False))

@dp.message(RevealForm.photos, F.text == "Готово")
async def rf_photos_done(m: Message, state: FSMContext):
    if not await _require_username(m):
        return
    data = await state.get_data()
    if data.get("refill_mode"):
        u = await get_user(m.from_user.id)
        old_have = bool(u[10] or u[11] or u[12])
        new_photos = data.get("new_photos") or []
        if not new_photos and not old_have:
            await m.answer("Нужно минимум 1 фото. Пришли фото и снова нажми «Готово».",
                           reply_markup=photos_empty_kb(refill=True, has_prev=False))
            return
        await _commit_staged_profile(m.from_user.id, data, keep_old_photos=(len(new_photos) == 0))
        await state.clear()
        await m.answer("Анкета сохранена. Теперь можно жать «🔎 Найти собеседника».", reply_markup=(await menu_for(m.from_user.id)))
        return

    u = await get_user(m.from_user.id)
    photos = [u[10], u[11], u[12]]
    if not any(photos):
        await m.answer("Нужно минимум 1 фото. Пришли фото и снова нажми «Готово».",
                       reply_markup=photos_empty_kb(refill=False, has_prev=False))
        return
    await set_user_fields(m.from_user.id, reveal_ready=1)
    await state.clear()
    await m.answer("Анкета сохранена. Теперь можно жать «🔎 Найти собеседника».", reply_markup=(await menu_for(m.from_user.id)))

async def _commit_staged_profile(tg_id: int, staged: dict, keep_old_photos: bool = False):
    fields = {}
    if 'new_gender' in staged:  fields['gender'] = staged['new_gender']
    if 'new_seeking' in staged: fields['seeking'] = staged['new_seeking']
    if 'new_first' in staged:   fields['first_name'] = staged['new_first']
    if 'new_last' in staged:    fields['last_name'] = staged['new_last']
    if 'new_faculty' in staged: fields['faculty'] = staged['new_faculty']
    if 'new_age' in staged:     fields['age'] = staged['new_age']
    if 'new_about' in staged:   fields['about'] = staged['new_about']
    if 'new_username' in staged:fields['username'] = staged['new_username']

    if not keep_old_photos:
        photos = staged.get('new_photos') or []
        if photos:
            p1 = photos[0] if len(photos) > 0 else None
            p2 = photos[1] if len(photos) > 1 else None
            p3 = photos[2] if len(photos) > 2 else None
            fields['photo1'] = p1
            fields['photo2'] = p2
            fields['photo3'] = p3

    fields['reveal_ready'] = 1
    await set_user_fields(tg_id, **fields)

# ============================================================
#                         FIND / MATCHING
# ============================================================

@dp.message(F.text == "🔎 Найти собеседника")
@dp.message(Command("find"))
async def find(m: Message, state: FSMContext):
    if not await gate_subscription(m):
        return
    await ensure_user(m.from_user.id)
    if await get_role(m.from_user.id) == "admin":
        await m.answer("Админ-аккаунт не участвует в поиске. Используй /admin для панели.", reply_markup=(await menu_for(m.from_user.id)))
        return

    if not await has_required_prefs(m.from_user.id):
        await m.answer("Сначала укажи свой пол и кого ищешь.", reply_markup=gender_self_kb())
        await state.update_data(start_form_after_prefs=True, is_refill=False)
        await state.set_state(GState.pick_gender)
        return

    peer = await active_peer(m.from_user.id)
    if peer:
        await m.answer("Ты сейчас в активном чате. Используй !next или !stop.")
        return

    if await in_queue(m.from_user.id):
        await m.answer("Уже ищу собеседника. Нажми «❌ Отмена» чтобы прервать.", reply_markup=cancel_kb())
        return

    u = await get_user(m.from_user.id)
    await enqueue(m.from_user.id, gender=u[1], seeking=u[2])
    await m.answer("Ищу собеседника… Пока идёт поиск, доступна только «❌ Отмена».", reply_markup=cancel_kb())
    await try_match_now(m.from_user.id)

# ============================================================
#                           SUPPORT
# ============================================================

@dp.message(F.text == "🆘 Поддержка")
async def support_entry(m: Message, state: FSMContext):
    if await get_role(m.from_user.id) == "admin" or m.from_user.id in ADMIN_IDS:
        await m.answer("Для админов есть «🧰 Поддержка» внутри /admin.", reply_markup=admin_reply_menu())
        return
    if await deny_actions_during_chat(m):
        return
    await state.clear()
    await state.set_state(SupportState.waiting)
    await m.answer(
        "Напиши сообщение с вопросом/проблемой — я перешлю админам.\n"
        "Чтобы выйти — нажми «❌ Отмена».",
        reply_markup=cancel_kb()
    )

@dp.message(SupportState.waiting)
async def support_collect(m: Message, state: FSMContext):
    async with db() as conn:
        cur = await conn.execute(
            "INSERT INTO support_msgs(from_user, text) VALUES(?,?)",
            (m.from_user.id, m.text or "")
        )
        _row_id = cur.lastrowid
        await conn.commit()

    for admin_id in (ADMIN_IDS or []):
        sent = await bot.send_message(
            admin_id,
            f"🆘 Запрос от {m.from_user.id} (@{m.from_user.username or '—'}):\n\n{m.text}"
        )
        SUPPORT_RELAY[sent.message_id] = m.from_user.id

    await m.answer("✉️ Сообщение отправлено админам. Ответ придёт сюда.")

@dp.message(Command("done"))
async def support_done(m: Message):
    async with db() as conn:
        await conn.execute(
            "UPDATE support_msgs SET status='closed' WHERE from_user=? AND status='open'",
            (m.from_user.id,)
        )
        await conn.commit()
    await m.answer("✅ Обращение закрыто. Если что — пиши снова: «🆘 Поддержка».")

@dp.message(F.reply_to_message, F.from_user.id.func(lambda uid: uid in ADMIN_IDS))
async def admin_reply_router(m: Message):
    uid = SUPPORT_RELAY.get(m.reply_to_message.message_id)
    if not uid:
        return
    await bot.send_message(uid, f"🛠 Ответ админа:\n{m.text}")
    await m.answer("✅ Отправлено пользователю.")

@dp.callback_query(F.data == "adm_support")
async def adm_support(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.", show_alert=True)

    await c.message.edit_text("Диалоги саппорта (последние):")
    async with db() as conn:
        cur = await conn.execute("""
            SELECT from_user, MAX(ts) AS last_ts
            FROM support_msgs
            WHERE status='open'
            GROUP BY from_user
            ORDER BY last_ts DESC
            LIMIT 10
        """)
        users = await cur.fetchall()

    if not users:
        return await bot.send_message(c.from_user.id, "Пусто. Новых обращений нет.")

    for (uid, _) in users:
        head = await bot.send_message(
            c.from_user.id, f"<b>Диалог с {uid}</b>. Ответьте реплаем на любое из сообщений ниже."
        )
        SUPPORT_RELAY[head.message_id] = uid

        async with db() as conn:
            cur = await conn.execute("""
                SELECT text, ts, id FROM support_msgs
                WHERE from_user=? AND status='open'
                ORDER BY ts DESC LIMIT 5
            """, (uid,))
            msgs = list(reversed(await cur.fetchall()))

        for text, ts, _sid in msgs:
            sent = await bot.send_message(c.from_user.id, f"🆘 {uid}: {text}")
            SUPPORT_RELAY[sent.message_id] = uid

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Закрыть тикет", callback_data=f"sup_close:{uid}")
        kb.adjust(1)
        await bot.send_message(c.from_user.id, "Можешь ответить реплаем. Или закрыть тикет:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sup_close:"))
async def sup_close(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS and await get_role(c.from_user.id) != "admin":
        return await c.answer("Нет доступа.", show_alert=True)
    uid = int(c.data.split(":")[1])
    async with db() as conn:
        await conn.execute("UPDATE support_msgs SET status='closed' WHERE from_user=?", (uid,))
        await conn.commit()

    await c.answer("Закрыто.")
    try:
        await c.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    try:
        await bot.send_message(uid, "🔧 Админ закрыл обращение. Если проблема осталась — открой новый запрос через «🆘 Поддержка».")
    except Exception:
        pass
    await bot.send_message(c.from_user.id, "Тикет закрыт.")

@dp.callback_query(F.data.startswith("sup_open:"))
async def sup_open(c: CallbackQuery):
    if await get_role(c.from_user.id) != "admin" and c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа.")
    uid = int(c.data.split(":")[1])

    async with db() as conn:
        cur = await conn.execute("""
            SELECT id, text, ts FROM support_msgs
            WHERE from_user=? AND status='open'
            ORDER BY id ASC LIMIT 30
        """, (uid,))
        msgs = await cur.fetchall()

    if not msgs:
        return await c.answer("У пользователя нет открытых сообщений.", show_alert=True)

    await c.message.edit_text(f"Диалог с {uid}. Ответьте реплаем на любое из сообщений ниже.")
    for _mid, text, _ts in msgs:
        fwd = await bot.send_message(c.from_user.id, f"🆘 {uid}:\n{text}")
        SUPPORT_RELAY[fwd.message_id] = uid

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Закрыть тикет", callback_data=f"sup_close:{uid}")
    await bot.send_message(c.from_user.id, "Можешь ответить реплаем. Или закрыть тикет:", reply_markup=kb.as_markup())

# ============================================================
#                       RELAY / CHAT HANDLERS
# ============================================================

@dp.message()
async def relay_chat(m: Message, state: FSMContext):
    # Обрабатываем только если у пользователя действительно есть активный чат (в RAM или в БД)
    if not await is_chat_active(m.from_user.id):
        # Не наш случай — пусть идут дальше по цепочке обработчиков
        raise SkipHandler

    # Восстановим RAM-состояние из БД (на случай рестарта) и получим peer/match_id
    materialized = await _materialize_session_if_needed(m.from_user.id)
    if not materialized:
        # На всякий случай отпускаем событие дальше
        raise SkipHandler
    peer, mid = materialized

    # Сброс таймера молчания и фиксация активности
    now_m = _nowm()
    DEADLINE[mid] = now_m + g_inactivity()
    LAST_SHOWN.pop(mid, None)

    now = _now()
    LAST_SEEN[m.from_user.id] = now
    LAST_SEEN[peer] = now

    await _stop_countdown(mid, m.from_user.id, peer, delete_msgs=True)
    WARNED.pop(mid, None)
    t = COUNTDOWN_TASKS.pop(mid, None)
    if t and not t.done():
        t.cancel()
    COUNTDOWN_MSGS.pop(mid, None)

    # Команды внутри чата
    if m.text:
        ttxt = m.text.strip().lower()
        if ttxt == "!stop":
            a = m.from_user.id
            b = peer
            await end_current_chat(a)
            _cleanup_match(mid, a, b)
            await send_post_chat_feedback(a, b, mid)
            await send_post_chat_feedback(b, a, mid)
            await m.answer("Чат завершён. Нажми «🔎 Найти собеседника», чтобы начать новый.", reply_markup=(await menu_for(m.from_user.id)))
            await bot.send_message(b, "Собеседник завершил чат.", reply_markup=(await menu_for(b)))
            return

        if ttxt == "!next":
            a = m.from_user.id
            b = peer
            if not await has_required_prefs(a):
                await end_current_chat(a)
                _cleanup_match(mid, a, b)
                await send_post_chat_feedback(a, b, mid)
                await send_post_chat_feedback(b, a, mid)
                await m.answer("Чтобы продолжить поиск, укажи свой пол и кого ищешь.", reply_markup=gender_self_kb())
                await bot.send_message(b, "Собеседник завершил чат.", reply_markup=(await menu_for(b)))
                return
            await record_separation(a, b)
            await end_current_chat(a)
            _cleanup_match(mid, a, b)
            me = await get_user(a)
            await enqueue(a, me[1], me[2])
            await m.answer("Ищу следующего собеседника…", reply_markup=cancel_kb())
            await bot.send_message(b, "Собеседник ушёл к следующему. Ты можешь нажать «🔎 Найти собеседника».", reply_markup=(await menu_for(b)))
            await try_match_now(a)
            return

        if ttxt == "!reveal":
            await handle_reveal(m.from_user.id, peer)
            return

    # Пересылка контента собеседнику
    # Пересылка контента собеседнику — с маскировкой и защитой
    if m.text:
        await send_text_anonym(peer, m.text)

    elif m.photo:
        await bot.send_photo(
            peer, m.photo[-1].file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.animation:
        await bot.send_animation(
            peer, m.animation.file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.video:
        await bot.send_video(
            peer, m.video.file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.audio:
        await bot.send_audio(
            peer, m.audio.file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.voice:
        await bot.send_voice(
            peer, m.voice.file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.video_note:
        await bot.send_video_note(peer, m.video_note.file_id, protect_content=True)

    elif m.document:
        await bot.send_document(
            peer, m.document.file_id,
            caption=clean_cap(m.caption),
            protect_content=True
        )

    elif m.contact or m.location or m.venue or m.poll or m.dice or m.game:
        await m.answer("Этот тип вложений отключён в анонимном чате.")

# ================== Раскрытие ==================

async def handle_reveal(me_id: int, peer_id: int):
    me = await get_user(me_id)
    peer = await get_user(peer_id)
    if not (me and peer and me[3] == 1 and peer[3] == 1):
        await bot.send_message(me_id, "Раскрытие невозможно: у одного из вас не заполнена анкета.")
        return

    async with db() as conn:
        cur = await conn.execute(
            "SELECT id,a_id,b_id,a_reveal,b_reveal FROM matches WHERE active=1 AND (a_id=? OR b_id=?) ORDER BY id DESC LIMIT 1",
            (me_id, me_id)
        )
        row = await cur.fetchone()
        if not row:
            await bot.send_message(me_id, "Нет активного чата.")
            return
        mid, a, b, ar, br = row
        is_a = (me_id == a)

        if (is_a and ar == 1) or ((not is_a) and br == 1):
            await bot.send_message(me_id, "Запрос на раскрытие уже отправлен. Ждём собеседника.")
            return

        if is_a:
            await conn.execute("UPDATE matches SET a_reveal=1 WHERE id=?", (mid,))
        else:
            await conn.execute("UPDATE matches SET b_reveal=1 WHERE id=?", (mid,))
        await conn.commit()

        cur = await conn.execute("SELECT a_reveal,b_reveal FROM matches WHERE id=?", (mid,))
        ar, br = await cur.fetchone()

    if ar == 1 and br == 1:
        await send_reveal_card(a, peer_id)
        await send_reveal_card(b, me_id)
        await bot.send_message(a, "Взаимное раскрытие выполнено.")
        await bot.send_message(b, "Взаимное раскрытие выполнено.")
    else:
        await bot.send_message(me_id, "Запрос на раскрытие отправлен. Ждём согласия собеседника.")

async def send_reveal_card(to_id: int, whose_id: int):
    """
    Показывает анкету ровно в том виде, как её видит владелец:
    - заголовок/тело от format_profile_text(u)
    - фото: все, у последнего — caption
    """
    u = await get_user(whose_id)
    if not u:
        await bot.send_message(to_id, "Профиль не найден.")
        return

    txt = format_profile_text(u)
    photos = [p for p in (u[10], u[11], u[12]) if p]

    if photos:
        for p in photos[:-1]:
            await bot.send_photo(to_id, p, protect_content=True)
        await bot.send_photo(
            to_id, photos[-1], caption=txt,
            protect_content=True
        )
    else:
        await bot.send_message(
            to_id, txt,
            parse_mode=None,
            disable_web_page_preview=True,
            protect_content=True
        )

# Обработка !команд, если RAM нет, но в БД активный матч есть
@dp.message(F.text.regexp(r"^!(stop|next|reveal)\b"))
async def bang_commands_when_db_active(m: Message, state: FSMContext):
    if m.from_user.id in ACTIVE:
        return  # разрулит relay_chat

    mat = await _materialize_session_if_needed(m.from_user.id)
    if not mat:
        await m.answer("Нет активного чата.")
        return

    peer, mid = mat
    txt = (m.text or "").strip().lower()

    if txt.startswith("!stop"):
        a = m.from_user.id
        b = peer
        await end_current_chat(a)
        _cleanup_match(mid, a, b)
        await send_post_chat_feedback(a, b, mid)
        await send_post_chat_feedback(b, a, mid)
        await m.answer("Чат завершён. Нажми «🔎 Найти собеседника», чтобы начать новый.", reply_markup=(await menu_for(m.from_user.id)))
        await bot.send_message(b, "Собеседник завершил чат.", reply_markup=(await menu_for(b)))
        return

    if txt.startswith("!next"):
        a = m.from_user.id
        b = peer
        if not await has_required_prefs(a):
            await end_current_chat(a)
            _cleanup_match(mid, a, b)
            await send_post_chat_feedback(a, b, mid)
            await send_post_chat_feedback(b, a, mid)
            await m.answer("Чтобы продолжить поиск, укажи свой пол и кого ищешь.", reply_markup=gender_self_kb())
            await bot.send_message(b, "Собеседник завершил чат.", reply_markup=(await menu_for(b)))
            return
        await record_separation(a, b)
        await end_current_chat(a)
        _cleanup_match(mid, a, b)
        me = await get_user(a)
        await enqueue(a, me[1], me[2])
        await m.answer("Ищу следующего собеседника…", reply_markup=cancel_kb())
        await bot.send_message(b, "Собеседник ушёл к следующему. Ты можешь нажать «🔎 Найти собеседника».", reply_markup=(await menu_for(b)))
        await try_match_now(a)
        return

    if txt.startswith("!reveal"):
        await handle_reveal(m.from_user.id, peer)
        return

# === ФИНАЛЬНЫЙ ФОЛБЭК ДЛЯ "НЕИЗВЕСТНЫХ" СООБЩЕНИЙ ===
@dp.message()
async def unknown_router(m: Message, state: FSMContext):
    # 1) Команды не трогаем — их ловят целевые хэндлеры
    if m.text and m.text.startswith("/"):
        return

    # 2) Если пользователь в чате/очереди/форме — не мешаем
    if await active_peer(m.from_user.id):
        return
    if await in_queue(m.from_user.id):
        await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        return
    if await state.get_state():
        return

    # 3) Иначе — меню
    await m.answer("Неизвестное действие. Возвращаю в главное меню.", reply_markup=(await menu_for(m.from_user.id)))

# ================== Entry ==================
async def main():
    await init_db()
    await load_settings_cache()
    # деактивируем очень старые активные чаты (например, старше суток)
    async with db() as conn:
        await conn.execute("UPDATE matches SET active=0 WHERE active=1 AND started_at < strftime('%s','now') - 86400")
        await conn.commit()
    print("DB path:", DB_PATH)
    # снять read-only если вдруг выставлен
    try:
        import stat
        if os.path.exists(DB_PATH):
            os.chmod(DB_PATH, stat.S_IWRITE | stat.S_IREAD)
    except Exception:
        pass

    print("Bot started")
    global RESOLVED_CHANNEL_ID
    try:
        chat = await bot.get_chat(CHANNEL_USERNAME)
        RESOLVED_CHANNEL_ID = chat.id  # например: -1001234567890
        print("Channel resolved id:", RESOLVED_CHANNEL_ID)
    except Exception as e:
        print("Could not resolve channel id:", repr(e))
        RESOLVED_CHANNEL_ID = None  # оставим None – дальше обработаем
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
