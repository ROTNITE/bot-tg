# keyboards.py
from aiogram.types import (
    ReplyKeyboardMarkup, InlineKeyboardMarkup,
    KeyboardButton, InlineKeyboardButton
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

# ====== СПИСОК ФАКУЛЬТЕТОВ ======
FACULTIES = [
    "ИИКС", "ФБИУКС", "ИМО", "ИФТИС",
    "ИНТЭЛ", "ИФТЭБ", "ИФИБ", "ЛАПЛАЗ",
    "ИЯФИТ"
]

# ====== ГЛАВНОЕ МЕНЮ ======
def main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🧭 Режимы"))
    kb.add(KeyboardButton(text="👤 Анкета"))
    return kb.as_markup(resize_keyboard=True)

# ====== РАСШИРЕННОЕ МЕНЮ (если активен магазин/саппорт) ======
def extra_main_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🧭 Режимы"))
    kb.add(KeyboardButton(text="👤 Анкета"))
    kb.add(KeyboardButton(text="💰 Магазин"))
    kb.add(KeyboardButton(text="🆘 Support"))
    kb.add(KeyboardButton(text="👥 Рефералка"))
    return kb.as_markup(resize_keyboard=True)

# ====== МЕНЮ РЕЖИМОВ ======
def modes_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="📇 Просмотр анкет"))
    kb.add(KeyboardButton(text="🕵️ Анонимный чат"))
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    return kb.as_markup(resize_keyboard=True)

# ====== МЕНЮ ДЛЯ АНКЕТЫ ======
def reveal_entry_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    kb.add(KeyboardButton(text="✏️ Заполнить / Перезаполнить"))
    return kb.as_markup(resize_keyboard=True)

# ====== ПОЛ / КОГО ИЩЕШЬ ======
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

# ====== ФАКУЛЬТЕТЫ (Inline) ======
def faculties_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for i, f in enumerate(FACULTIES):
        b.button(text=f, callback_data=f"fac:{i}")
    b.adjust(2)
    return b.as_markup()

# ====== МЕНЮ АНКЕТЫ — О СЕБЕ ======
def about_kb(refill: bool = False, has_prev: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="Пропустить"))
    if refill and has_prev:
        kb.add(KeyboardButton(text="Оставить текущее"))
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

# ====== МЕНЮ АНКЕТЫ — ФОТО ======
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

# ====== ПРОСТЫЕ МЕНЮ ======
def cancel_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb.as_markup(resize_keyboard=True)

def anon_chat_menu_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="🔎 Найти собеседника"))
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    return kb.as_markup(resize_keyboard=True)

def shop_back_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="⬅️ В главное меню"))
    kb.add(KeyboardButton(text="💰 Магазин"))
    return kb.as_markup(resize_keyboard=True)

# ====== КЛАВИАТУРА ДЛЯ ПРОВЕРКИ ПОДПИСКИ ======
def subscription_kb(channel_link: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Подписаться", url=channel_link)
    kb.button(text="✅ Проверить подписку", callback_data="sub_check")
    kb.adjust(1)
    return kb.as_markup()
