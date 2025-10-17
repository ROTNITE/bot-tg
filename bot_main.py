# bot_main.py
import asyncio
import os
import time
from typing import Optional, Dict, Tuple

from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery

from dotenv import load_dotenv

# клавиатуры (все берем из отдельного файла)
from keyboards import (
    main_menu, modes_kb,
    gender_self_kb, seeking_kb, faculties_kb,
    about_kb, photos_empty_kb, photos_progress_kb,
    cancel_kb, anon_chat_menu_kb, subscription_kb,
    reveal_entry_menu
)

# схема БД и коннектор — из отдельного файла
from db_schema import db, init_db, DB_PATH

# доп. фичи (магазин/саппорт/рефералка) и расширенное меню
from features_extra import setup_extra_features, extra_main_menu

INACTIVITY_SECONDS = 180  # таймаут молчания

# ================== CONFIG ==================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в .env")

# Админы. Можно несколько через запятую или из .env
ADMIN_IDS = set(int(x) for x in (os.getenv("ADMIN_IDS", "") or "").split(",") if x.strip())
SUPPORT_ENABLED = True
DAILY_BONUS_POINTS = 10
CHANNEL_USERNAME = "@nektomephi"           # юзернейм канала
CHANNEL_LINK = "https://t.me/nektomephi"   # ссылка-приглашение
RESOLVED_CHANNEL_ID: Optional[int] = None  # будет заполнен в main()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

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

# ================== FSM ==================
class GState(StatesGroup):
    pick_gender = State()
    pick_seeking = State()

class RevealForm(StatesGroup):
    name = State()
    faculty = State()
    age = State()
    about = State()
    photos = State()

# ================== Keyboards (GLOBAL) ==================
@dp.callback_query(F.data == "sub_check")
async def sub_check(c: CallbackQuery):
    if await is_subscribed(c.from_user.id):
        try:
            await c.message.edit_text("✅ Спасибо за подписку!")
        except Exception:
            pass
        await c.answer("Подписка подтверждена!")
        await bot.send_message(
            c.from_user.id,
            INTRO_TEXT,
            disable_web_page_preview=True,
            reply_markup=(await menu_for(<uid>))
        )
    else:
        await c.answer(
            "Похоже, ты ещё не подписался. Нажми «Подписаться», а потом снова «Проверить».",
            show_alert=False
        )

async def gate_subscription(message: Message) -> bool:
    if await is_subscribed(message.from_user.id):
        return True
    text = (
        "🔔 Перед использованием бота нужно подписаться на наш канал.\n"
        "Это помогает держать всех в курсе обновлений и правил."
    )
    if RESOLVED_CHANNEL_ID is None:
        text += "\n\n⚠️ Техническая заметка: если подписка не определяется после вступления, добавьте бота администратором канала."
    await message.answer(text, reply_markup=subscription_kb(CHANNEL_LINK))
    return False

def chat_hint() -> str:
    return ("Команды в чате:\n"
            "<code>!next</code> — следующий собеседник\n"
            "<code>!stop</code> — закончить\n"
            "<code>!reveal</code> — взаимное раскрытие (если анкеты есть у обоих)\n")

# ================== Inactivity monitor (RAM) ==================
# user_id -> (peer_id, match_id)
ACTIVE: Dict[int, Tuple[int, int]] = {}
# user_id -> last_seen_unix
LAST_SEEN: Dict[int, float] = {}
# match_id -> asyncio.Task
WATCH: Dict[int, asyncio.Task] = {}
# match_id -> уже отправляли ли предупреждение за 60 сек (True/False)
WARNED: Dict[int, bool] = {}

def _now() -> float:
    return time.time()

async def _watch_inactivity(mid: int, a: int, b: int):
    try:
        while True:
            await asyncio.sleep(5)
            # если матч уже не активен — выходим
            if a not in ACTIVE or b not in ACTIVE:
                return
            if ACTIVE.get(a, (None, None))[1] != mid or ACTIVE.get(b, (None, None))[1] != mid:
                return

            na = LAST_SEEN.get(a, _now())
            nb = LAST_SEEN.get(b, _now())
            # --- предупреждение за 60 секунд до авто-завершения ---
            idle = max(_now() - na, _now() - nb)
            remaining = int(INACTIVITY_SECONDS - idle)
            if 0 < remaining <= 60 and not WARNED.get(mid):
                WARNED[mid] = True
                warn_text = (
                    f"⌛️ Тишина… Чат автоматически завершится через {remaining} сек.\n"
                    f"Напиши любое сообщение, чтобы продолжить разговор."
                )
                await bot.send_message(a, warn_text)
                await bot.send_message(b, warn_text)
            # --- завершение по таймауту ---
            if _now() - na >= INACTIVITY_SECONDS or _now() - nb >= INACTIVITY_SECONDS:
                # Завершаем чат у ОБОИХ
                await end_current_chat(a)
                await end_current_chat(b)
                _cleanup_match(mid, a, b)

                # Сообщаем ОБОИМ и сразу отдаём главное меню
                await bot.send_message(a, "Чат завершён из-за неактивности.", reply_markup=main_menu())
                await bot.send_message(b, "Чат завершён из-за неактивности.", reply_markup=main_menu())
                return
    except asyncio.CancelledError:
        return

def _cleanup_match(mid: int, a: int, b: int):
    ACTIVE.pop(a, None)
    ACTIVE.pop(b, None)
    LAST_SEEN.pop(a, None)
    LAST_SEEN.pop(b, None)
    t = WATCH.pop(mid, None)
    if t and not t.done():
        t.cancel()
    WARNED.pop(mid, None)

# ================== Helpers ==================
async def is_subscribed(user_id: int) -> bool:
    target = RESOLVED_CHANNEL_ID or CHANNEL_USERNAME  # используем id, если смогли получить
    try:
        cm = await bot.get_chat_member(target, user_id)
        # статус может быть Enum или строкой
        status = str(getattr(cm, "status", "")).lower()
        # Для каналов подписчик — "member"; админ/создатель тоже ок
        if status in ("member", "administrator", "creator"):
            return True
        # На всякий случай у некоторых типов есть is_member
        if hasattr(cm, "is_member") and bool(getattr(cm, "is_member")):
            return True
        return False
    except Exception as e:
        # если бот НЕ админ канала, Telegram вернёт 400/403; считаем "не подписан"
        print("is_subscribed error:", repr(e))
        return False

async def ensure_user(tg_id: int):
    async with db() as conn:
        await conn.execute("INSERT OR IGNORE INTO users(tg_id) VALUES(?)", (tg_id,))
        # если tg_id в ADMIN_IDS — назначаем роль admin
        if tg_id in ADMIN_IDS:
            await conn.execute("UPDATE users SET role='admin' WHERE tg_id=?", (tg_id,))
        await conn.commit()

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
        cur = await conn.execute("""SELECT tg_id,gender,seeking,reveal_ready,first_name,last_name,
                                    faculty,age,about,username,photo1,photo2,photo3
                                    FROM users WHERE tg_id=?""", (tg_id,))
        return await cur.fetchone()

async def get_user_or_create(tg_id: int):
    u = await get_user(tg_id)
    if not u:
        await ensure_user(tg_id)
        u = await get_user(tg_id)
    return u

async def start_reveal_form(m: Message, state: FSMContext, is_refill: bool):
    await m.answer(
        "Анкета для взаимного раскрытия. Её увидят только при взаимном !reveal.\n"
        "Анкету нельзя оставить неполной — можно заполнить целиком или нажать «❌ Отмена».",
        reply_markup=cancel_kb()
    )
    await m.answer("Как тебя зовут?", reply_markup=cancel_kb())
    await state.update_data(is_refill=is_refill)
    await state.set_state(RevealForm.name)

async def get_role(tg_id:int)->str:
    async with db() as conn:
        cur = await conn.execute("SELECT role FROM users WHERE tg_id=?", (tg_id,))
        row = await cur.fetchone()
        return row[0] if row else "user"

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

async def get_status(tg_id:int)->Optional[str]:
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

@dp.message(Command("prefs"))
async def prefs(m: Message):
    await ensure_user(m.from_user.id)
    u = await get_user(m.from_user.id)
    await m.answer(f"gender={u[1]!r}\nseeking={u[2]!r}\nreveal_ready={u[3]}")

def format_profile_text(u: tuple) -> str:
    """
    u = (tg_id, gender, seeking, reveal_ready, first_name, last_name,
         faculty, age, about, username, photo1, photo2, photo3)
    """
    first = (u[4] or "").strip()
    last = (u[5] or "").strip()
    # В заголовке показываем имя (или фамилию, если имя пустое)
    name = first or (last.split()[0] if last else "Без имени")

    age = u[7]
    age_str = str(age) if isinstance(age, int) else "—"

    faculty = (u[6] or "—").strip()

    about = (u[8] or "").strip()
    first_line = ""
    rest = ""
    if about:
        lines = [ln.strip() for ln in about.splitlines() if ln.strip()]
        if lines:
            first_line = lines[0]
            if len(lines) > 1:
                rest = "\n".join(lines[1:])

    # Заголовок как в примере: Имя, 18, 📍 ИИКС — Первая строка описания
    header = f"{name}, {age_str}, 📍 {faculty}"
    if first_line:
        header += f" — {first_line}"

    # Остальной текст описания (если есть)
    body = f"\n{rest}" if rest else ""

    # Ник в конце (если есть)
    username = (u[9] or "").strip()
    tail = f"\n\n{username}" if username else ""

    return header + body + tail

async def active_peer(tg_id: int) -> Optional[int]:
    # сначала проверим RAM-мапу — так быстрее
    if tg_id in ACTIVE:
        return ACTIVE[tg_id][0]
    # fallback по БД
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
    # завершаем активные матчи в БД
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
        await conn.execute(
            "INSERT INTO recent_partners(u_id,partner_id,block_left) VALUES(?,?,2) "
            "ON CONFLICT(u_id,partner_id) DO UPDATE SET block_left=0",
            (a, b)
        )
        await conn.execute(
            "INSERT INTO recent_partners(u_id,partner_id,block_left) VALUES(?,?,2) "
            "ON CONFLICT(u_id,partner_id) DO UPDATE SET block_left=0",
            (b, a)
        )
        await conn.commit()

async def decay_blocks(u_id: int):
    async with db() as conn:
        await conn.execute(
            "UPDATE recent_partners SET block_left=block_left-1 "
            "WHERE u_id=? AND block_left>0", (u_id,)
        )
        await conn.execute(
            "DELETE FROM recent_partners WHERE u_id=? AND block_left<=0", (u_id,)
        )
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

# ========== GLOBAL «Отмена» ==========
@dp.message(F.text == "❌ Отмена")
async def global_cancel(m: Message, state: FSMContext):
    # 1) Если идёт анкета — чистим только анкету, а пол/поиск остаются
    cur_state = await state.get_state()
    if cur_state in {
        RevealForm.name.state, RevealForm.faculty.state,
        RevealForm.age.state, RevealForm.about.state,
        RevealForm.photos.state
    }:
        await set_user_fields(
            m.from_user.id, reveal_ready=0,
            first_name=None, last_name=None, faculty=None,
            age=None, about=None,
            photo1=None, photo2=None, photo3=None
        )
        await state.clear()
        await m.answer("Анкета отменена.", reply_markup=main_menu())
        return

    # 2) Если стоит в очереди — отменяем поиск
    if await in_queue(m.from_user.id):
        await dequeue(m.from_user.id)
        await m.answer("Поиск отменён.", reply_markup=main_menu())
        return

    await m.answer("Главное меню.", reply_markup=main_menu())

# ====== РЕЖИМЫ ======
@dp.message(F.text == "🧭 Режимы")
async def modes_entry(m: Message, state: FSMContext):
    if not await gate_subscription(m):
        return
    await state.clear()
    await m.answer(
        "Выбери режим работы бота:\n\n"
        "• <b>📇 Просмотр анкет</b> — лента анкет (в разработке)\n"
        "• <b>🕵️ Анонимный чат</b> — случайные собеседники с возможностью взаимного раскрытия",
        reply_markup=modes_kb()
    )

@dp.message(F.text == "📇 Просмотр анкет")
async def mode_cards(m: Message):
    await m.answer("Раздел «Просмотр анкет» — <b>в разработке</b>.", reply_markup=modes_kb())

@dp.message(F.text == "🕵️ Анонимный чат")
async def mode_anon_chat(m: Message):
    if not await gate_subscription(m):
        return
    await m.answer(
        "Режим «Анонимный чат». Здесь можно искать случайного собеседника.\n"
        "Используй кнопки ниже.",
        reply_markup=anon_chat_menu_kb()
    )

# ========== MATCHING ==========
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
            LEFT JOIN recent_partners rp ON rp.u_id=? AND rp.partner_id=q.tg_id AND rp.block_left>0
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

    # Запомним связь для мониторинга
    ACTIVE[a] = (b, mid)
    ACTIVE[b] = (a, mid)
    LAST_SEEN[a] = _now()
    LAST_SEEN[b] = _now()
    WATCH[mid] = asyncio.create_task(_watch_inactivity(mid, a, b))

    from aiogram.types import ReplyKeyboardRemove
    await bot.send_message(a, "Подключил собеседника. Вы анонимны.\n" + chat_hint(), reply_markup=ReplyKeyboardRemove())
    await bot.send_message(b, "Подключил собеседника. Вы анонимны.\n" + chat_hint(), reply_markup=ReplyKeyboardRemove())

async def try_match_now(tg_id: int):
    mate = await find_partner(tg_id)
    if mate:
        await start_match(tg_id, mate)

# ================== /start ==================
@dp.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    if not await gate_subscription(m):
        return
    await ensure_user(m.from_user.id)
    u = await get_user(m.from_user.id)

    # новичок или не указаны базовые предпочтения
    if not u or not u[1] or not u[2]:
        # отправляем приветственное описание + сразу просим выбрать пол
        await m.answer(INTRO_TEXT, disable_web_page_preview=True)
        await m.answer(
            "Сначала выберем твой пол и кого ищешь. Затем, при желании, можно заполнить анкету для деанонимизации.",
            reply_markup=gender_self_kb()
        )
        await state.update_data(start_form_after_prefs=True, is_refill=False)
        await state.set_state(GState.pick_gender)
        if not (m.from_user.username or ""):
            await m.answer("ℹ️ Для деанонимизации (анкеты) нужен @username в Telegram. Его можно создать позже.")
        return

    # если уже всё настроено — просто показываем меню
    await m.answer("Главное меню.", reply_markup=main_menu())

# ========== Выбор пола / кого ищем ==========
@dp.message(GState.pick_gender)
async def pick_gender_msg(m: Message, state: FSMContext):
    await ensure_user(m.from_user.id)
    # нормализуем ввод без учёта регистра
    text = (m.text or "").strip().casefold()
    if text not in {"я девушка", "я парень"}:
        await m.answer("Выбери одну из кнопок: «Я девушка» или «Я парень».",
                       reply_markup=gender_self_kb())
        return
    gender = "Девушка" if text == "я девушка" else "Парень"
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
    await set_user_fields(m.from_user.id, seeking=text)

    data = await state.get_data()
    after_prefs = data.get("start_form_after_prefs", False)
    is_refill = data.get("is_refill", False)
    await state.update_data(start_form_after_prefs=False)
    await state.clear()

    if after_prefs:
        if not (m.from_user.username or ""):
            await m.answer("Параметры сохранены. Для анкеты нужен @username. Его можно настроить в Telegram позже.",
                           reply_markup=main_menu())
            return
        await m.answer(
            "Анкета для взаимного раскрытия. Её увидят только при взаимном !reveal.\n"
            "Анкету нельзя оставить неполной — можно заполнить целиком или нажать «❌ Отмена».",
            reply_markup=cancel_kb()
        )
        await m.answer("Как тебя зовут?", reply_markup=cancel_kb())
        await state.update_data(is_refill=is_refill)
        await state.set_state(RevealForm.name)
        return

    await m.answer("Параметры сохранены.", reply_markup=main_menu())

# ================== Reveal Form ==================
@dp.message(RevealForm.name)
async def rf_name(m: Message, state: FSMContext):
    parts = m.text.strip().split()
    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else ""
    await set_user_fields(m.from_user.id, first_name=first, last_name=last)
    await m.answer("С какого ты института?", reply_markup=faculties_kb())
    await state.set_state(RevealForm.faculty)

@dp.callback_query(RevealForm.faculty, F.data.startswith("fac:"))
async def rf_fac(c: CallbackQuery, state: FSMContext):
    idx = int(c.data.split(":")[1])
    fac = FACULTIES[idx]
    await set_user_fields(c.from_user.id, faculty=fac)
    await c.message.edit_text(f"Факультет: <b>{fac}</b>")
    await c.message.answer("Сколько тебе лет?", reply_markup=cancel_kb())
    await state.set_state(RevealForm.age)
    await c.answer()

@dp.message(RevealForm.age)
async def rf_age(m: Message, state: FSMContext):
    try:
        age = int(m.text.strip())
        if not (17 <= age <= 99):
            raise ValueError
    except Exception:
        await m.answer("Возраст числом 17–99, попробуй ещё раз.", reply_markup=cancel_kb())
        return

    await set_user_fields(m.from_user.id, age=age)

    u = await get_user_or_create(m.from_user.id)
    data = await state.get_data()
    refill = bool(data.get("is_refill"))
    has_prev_about = bool(u[8])
    await m.answer(
        "Расскажи о себе (до 300 символов) или нажми «Пропустить».",
        reply_markup=about_kb(refill=refill, has_prev=has_prev_about)
    )
    await state.set_state(RevealForm.about)

# --- спец-обработчик «Пропустить» ---
@dp.message(RevealForm.about, F.text.casefold() == "пропустить")
async def rf_about_skip(m: Message, state: FSMContext):
    await set_user_fields(m.from_user.id, about=None)
    uname = m.from_user.username or ""
    await set_user_fields(m.from_user.id, username=(f"@{uname}" if uname else None))

    u = await get_user_or_create(m.from_user.id)
    data = await state.get_data()
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
    await set_user_fields(m.from_user.id, about=(text_raw or None))
    uname = m.from_user.username or ""
    await set_user_fields(m.from_user.id, username=(f"@{uname}" if uname else None))

    u = await get_user(m.from_user.id)
    data = await state.get_data()
    refill = bool(data.get("is_refill"))
    has_prev_photos = bool(u[10] or u[11] or u[12])
    await m.answer("Пришли до 3 фото (как фото).",
                   reply_markup=photos_empty_kb(refill=refill, has_prev=has_prev_photos))
    await state.set_state(RevealForm.photos)

# ====== ФОТО ======
@dp.message(RevealForm.photos, F.text == "Оставить текущее")
async def rf_photos_keep(m: Message, state: FSMContext):
    await set_user_fields(m.from_user.id, reveal_ready=1)
    await state.clear()
    await m.answer("Анкета сохранена (оставили прежние фото). Теперь можно жать «🔎 Найти собеседника».",
                   reply_markup=main_menu())

@dp.message(RevealForm.photos, F.photo)
async def rf_photos(m: Message, state: FSMContext):
    u = await get_user_or_create(m.from_user.id)
    current = [u[10], u[11], u[12]]
    file_id = m.photo[-1].file_id
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
    await set_user_fields(m.from_user.id, photo1=None, photo2=None, photo3=None)
    await m.answer("Все фото удалены. Пришли новое фото (до 3).",
                   reply_markup=photos_empty_kb(refill=False, has_prev=False))

@dp.message(RevealForm.photos, F.text == "Готово")
async def rf_photos_done(m: Message, state: FSMContext):
    u = await get_user(m.from_user.id)
    photos = [u[10], u[11], u[12]]
    if not any(photos):
        await m.answer("Нужно минимум 1 фото. Пришли фото и снова нажми «Готово».",
                       reply_markup=photos_empty_kb(refill=False, has_prev=False))
        return
    await set_user_fields(m.from_user.id, reveal_ready=1)
    await state.clear()
    await m.answer("Анкета сохранена. Теперь можно жать «🔎 Найти собеседника».", reply_markup=main_menu())

# ================== Меню анкеты ==================
@dp.message(F.text == "⬅️ В главное меню")
async def reveal_back_msg(m: Message, state: FSMContext):
    if await in_queue(m.from_user.id):
        await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        return
    await state.clear()
    await m.answer("Главное меню.", reply_markup=main_menu())

@dp.message(F.text == "✏️ Заполнить / Перезаполнить")
async def reveal_begin_msg(m: Message, state: FSMContext):
    if await in_queue(m.from_user.id):
        await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        return

    u = await get_user_or_create(m.from_user.id)
    ready = bool(u and u[3])
    have_prefs = bool(u and u[1] and u[2])  # gender, seeking

    if have_prefs:
        # Пол и интерес уже заданы — сразу к анкете
        await start_reveal_form(m, state, is_refill=ready)
        return

    # Пол/интерес не заданы — сначала спросим их, а потом автоматически запустим анкету
    await state.update_data(start_form_after_prefs=True, is_refill=ready)
    await m.answer("Кто ты?", reply_markup=gender_self_kb())
    await state.set_state(GState.pick_gender)

# ================== Профиль/анкета (вход) ==================
@dp.message(F.text == "👤 Анкета")
async def show_or_edit_reveal(m: Message, state: FSMContext):
    if not await gate_subscription(m):
        return
    await ensure_user(m.from_user.id)
    peer = await active_peer(m.from_user.id)
    if peer:
        await m.answer("Ты сейчас в активном чате. Используй !next или !stop.")
        return
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

# ================== Поиск / Матчинг ==================
@dp.message(F.text == "🔎 Найти собеседника")
@dp.message(Command("find"))
async def find(m: Message, state: FSMContext):
    if not await gate_subscription(m):
        return
    await ensure_user(m.from_user.id)
    # если нет обязательных полей — запрещаем поиск и переводим в опрос
    if not await has_required_prefs(m.from_user.id):
        await m.answer(
            "Сначала укажи свой пол и кого ищешь.",
            reply_markup=gender_self_kb()
        )
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

    u = await get_user(m.from_user.id)  # к этому моменту гарантированно есть gender/seeking
    await enqueue(m.from_user.id, gender=u[1], seeking=u[2])
    await m.answer("Ищу собеседника… Пока идёт поиск, доступна только «❌ Отмена».", reply_markup=cancel_kb())
    await try_match_now(m.from_user.id)

# ================== Реле сообщений ==================
@dp.message()
async def relay(m: Message, state: FSMContext):
    peer = await active_peer(m.from_user.id)

    # Если пользователь НЕ в активном чате
    if not peer:
        # Если сейчас идёт заполнение анкеты — не мешаем спец-хендлерам анкеты
        cur_state = await state.get_state()
        if cur_state:
            return

        # Если стоит в очереди — подсказываем только "Отмена"
        if await in_queue(m.from_user.id):
            await m.answer("Идёт поиск. Доступна только «❌ Отмена».", reply_markup=cancel_kb())
        else:
            # Любое другое «неизвестное» сообщение — отправляем в главное меню
            await m.answer("Главное меню.", reply_markup=main_menu())
        return

    # --- Ниже логика РЕАЛЬНОГО чата ---

    # отметим активность отправителя
    LAST_SEEN[m.from_user.id] = _now()

    # если было предупреждение — сбросим флаг (активность продлевает чат)
    if m.from_user.id in ACTIVE:
        mid = ACTIVE[m.from_user.id][1]
        if WARNED.pop(mid, None):
            pass

    if m.text:
        t = m.text.strip().lower()
        if t == "!stop":
            if m.from_user.id in ACTIVE:
                mid = ACTIVE[m.from_user.id][1]
                a = m.from_user.id
                b = ACTIVE[m.from_user.id][0]
                await end_current_chat(a)
                _cleanup_match(mid, a, b)
                await m.answer("Чат завершён. Нажми «🔎 Найти собеседника», чтобы начать новый.", reply_markup=main_menu())
                await bot.send_message(b, "Собеседник завершил чат.", reply_markup=main_menu())
            return

        if t == "!next":
            if m.from_user.id in ACTIVE:
                mid = ACTIVE[m.from_user.id][1]
                a = m.from_user.id
                b = ACTIVE[m.from_user.id][0]

                # запрет, если нет пола/поиска
                if not await has_required_prefs(a):
                    await end_current_chat(a)
                    _cleanup_match(mid, a, b)
                    await m.answer(
                        "Чтобы продолжить поиск, укажи свой пол и кого ищешь.",
                        reply_markup=gender_self_kb()
                    )
                    await bot.send_message(b, "Собеседник завершил чат.", reply_markup=main_menu())
                    return

                await record_separation(a, b)
                await end_current_chat(a)
                _cleanup_match(mid, a, b)
                me = await get_user(a)
                await enqueue(a, me[1], me[2])
                await m.answer("Ищу следующего собеседника…", reply_markup=cancel_kb())
                await bot.send_message(b, "Собеседник ушёл к следующему. Ты можешь нажать «🔎 Найти собеседника».",
                                       reply_markup=main_menu())
                await try_match_now(a)
            return

        if t == "!reveal":
            await handle_reveal(m.from_user.id, peer)
            return

    # Пересылка всех типов
    if m.text:
        await bot.send_message(peer, m.text)
    elif m.photo:
        await bot.send_photo(peer, m.photo[-1].file_id, caption=m.caption or "")
    elif m.sticker:
        await bot.send_sticker(peer, m.sticker.file_id)
    elif m.animation:
        await bot.send_animation(peer, m.animation.file_id, caption=m.caption or "")
    elif m.video:
        await bot.send_video(peer, m.video.file_id, caption=m.caption or "")
    elif m.audio:
        await bot.send_audio(peer, m.audio.file_id, caption=m.caption or "")
    elif m.voice:
        await bot.send_voice(peer, m.voice.file_id, caption=m.caption or "")
    elif m.video_note:
        await bot.send_video_note(peer, m.video_note.file_id)
    elif m.document:
        await bot.send_document(peer, m.document.file_id, caption=m.caption or "")

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
    u = await get_user(whose_id)
    txt = (
        "<b>Раскрытие личности</b>\n\n"
        f"Имя: {u[4] or '-'} {u[5] or ''}\n"
        f"Институт: {u[6] or '-'}\n"
        f"Возраст: {u[7] or '-'}\n"
        f"О себе: {u[8] or '-'}\n"
        f"Telegram: {u[9] or '-'}"
    )
    photos = [p for p in (u[10], u[11], u[12]) if p]
    if photos:
        for p in photos[:-1]:
            await bot.send_photo(to_id, p)
        await bot.send_photo(to_id, photos[-1], caption=txt)
    else:
        await bot.send_message(to_id, txt)

# ================== Entry ==================
async def main():
    await init_db()
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

    await setup_extra_features(dp, bot)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())