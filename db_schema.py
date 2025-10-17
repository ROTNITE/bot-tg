# db_schema.py
from __future__ import annotations

import os
import aiosqlite

# --- путь к БД: C:\Users\<user>\AppData\Local\mephi_dating\bot.db ---
APPDATA_DIR = os.path.join(os.path.expanduser("~"), "AppData", "Local", "mephi_dating")
os.makedirs(APPDATA_DIR, exist_ok=True)
DB_PATH = os.path.join(APPDATA_DIR, "bot.db")

# Базовая схема (без «опасных» ALTER'ов — их применим отдельно и бережно)
CREATE_SQL_BASE = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS recent_partners(
  u_id INTEGER NOT NULL,
  partner_id INTEGER NOT NULL,
  block_left INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(u_id, partner_id)
);

CREATE TABLE IF NOT EXISTS users(
  tg_id INTEGER PRIMARY KEY,
  gender TEXT,              -- "Парень" / "Девушка"
  seeking TEXT,             -- "Парни" / "Девушки" / "Не важно"
  reveal_ready INTEGER DEFAULT 0, -- 1 если анкета сохранена
  first_name TEXT,
  last_name TEXT,
  faculty TEXT,
  age INTEGER,
  about TEXT,
  username TEXT,            -- telegram @username
  photo1 TEXT,
  photo2 TEXT,
  photo3 TEXT,
  created_at INTEGER DEFAULT (strftime('%s','now')),
  updated_at INTEGER DEFAULT (strftime('%s','now'))
);

-- магазин
CREATE TABLE IF NOT EXISTS shop_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  price INTEGER NOT NULL,
  type TEXT NOT NULL,           -- 'status' | 'privilege'
  payload TEXT,                 -- для status: текст статуса; для privilege: ключ флага
  is_active INTEGER DEFAULT 1
);

-- покупки
CREATE TABLE IF NOT EXISTS purchases(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  item_id INTEGER NOT NULL,
  ts INTEGER DEFAULT (strftime('%s','now'))
);

-- саппорт (простая очередь сообщений)
CREATE TABLE IF NOT EXISTS support_msgs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_user INTEGER NOT NULL,
  to_admin INTEGER,             -- кому переслано (если назначен)
  orig_msg_id INTEGER,          -- id сообщения у бота, чтобы уметь реплаить
  text TEXT,
  ts INTEGER DEFAULT (strftime('%s','now')),
  status TEXT DEFAULT 'open'    -- open/closed
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

CREATE INDEX IF NOT EXISTS idx_matches_active ON matches(active);
"""

# Аккуратные миграции: добавим недостающие колонки только если их ещё нет
ALTERS = [
    ("users", "role",         "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'"),
    ("users", "points",       "ALTER TABLE users ADD COLUMN points INTEGER DEFAULT 0"),
    ("users", "status_title", "ALTER TABLE users ADD COLUMN status_title TEXT"),
]


def db() -> aiosqlite.Connection:
    """Вернёт соединение с БД. Использовать как: `async with db() as conn: ...`"""
    return aiosqlite.connect(DB_PATH)


async def _column_exists(conn: aiosqlite.Connection, table: str, col: str) -> bool:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    return any(r[1] == col for r in rows)


async def _apply_safe_alters(conn: aiosqlite.Connection) -> None:
    for table, col, stmt in ALTERS:
        try:
            if not await _column_exists(conn, table, col):
                await conn.execute(stmt)
        except Exception as e:
            # Не валим бот: просто логируем и идём дальше
            print(f"[DB] ALTER failed ({table}.{col}): {repr(e)}")


async def init_db() -> None:
    """Создать базовые таблицы и применить безболезненные миграции."""
    # убедимся, что папка существует (если импортировали модуль отдельно)
    os.makedirs(APPDATA_DIR, exist_ok=True)

    async with db() as conn:
        await conn.executescript(CREATE_SQL_BASE)
        await _apply_safe_alters(conn)
        await conn.commit()


__all__ = [
    "APPDATA_DIR",
    "DB_PATH",
    "db",
    "init_db",
    "CREATE_SQL_BASE",
]
