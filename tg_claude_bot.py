#!/usr/bin/env python3
"""
Telegram <-> Claude Code bridge.

Один процесс `claude -p` на чат. Процесс не завершается после ответа,
а читает stdin, поэтому инициализация (3-10 с) выполняется один раз
при запуске процесса, а не на каждое сообщение.

session_id пишется в SQLite, поэтому после перезапуска скрипта диалог
возобновляется через --resume (если пауза не слишком велика).

Зависимости:
    pip install -r requirements.txt

Запуск:
    cp .env.example .env   # и вписать TELEGRAM_BOT_TOKEN
    python tg_claude_bot.py

Авторизация Claude берётся из локального логина (`claude` уже авторизован).
На сервере вместо этого нужен CLAUDE_CODE_OAUTH_TOKEN в окружении.
"""

import asyncio
import json
import logging
import mimetypes
import os
import pathlib
import re
import shlex
import signal
import sqlite3
import sys
import time
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

# --------------------------------------------------------------------------
# Конфигурация
# --------------------------------------------------------------------------

# .env читается рядом со скриптом, а не в текущем каталоге: запуск из другого
# места (systemd, cron) не должен менять набор настроек. Уже установленные
# переменные окружения приоритетнее файла — override=False по умолчанию,
# поэтому `TELEGRAM_BOT_TOKEN=... python tg_claude_bot.py` перекрывает .env.
#
# utf-8-sig, а не utf-8: Блокнот и `Set-Content -Encoding utf8` пишут BOM,
# он приклеивается к имени первого ключа, и TELEGRAM_BOT_TOKEN не читается
# при визуально правильном файле. Файлы без BOM utf-8-sig читает так же.
load_dotenv(pathlib.Path(__file__).resolve().with_name(".env"), encoding="utf-8-sig")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
CLAUDE_WORKDIR = os.environ.get("CLAUDE_WORKDIR", os.getcwd())
CLAUDE_EXTRA_ARGS = shlex.split(os.environ.get("CLAUDE_EXTRA_ARGS", ""))

# stream-json — построчный протокол: одно событие claude на одну строку stdout.
# asyncio.StreamReader требует числовой лимit буфера (не умеет "без лимита") и
# без него режет строку на 64 КиБ, роняя ход ("Separator is found, but chunk
# is longer than limit"), если инструмент вернул много текста (большой файл,
# длинный вывод команды). sys.maxsize — практически то же самое, что без
# лимита: реального события такого размера не бывает раньше, чем кончится ОЗУ.
STDOUT_LINE_LIMIT = int(os.environ.get("STDOUT_LINE_LIMIT", str(sys.maxsize)))

# Белый список по user_id: пускаем конкретных людей в любом чате, а не чаты
# целиком. Пусто = пускаем всех (поведение по умолчанию не меняется).
# Мусорные значения не глотаем молча — о них предупреждаем при старте.
_BAD_ALLOWED: List[str] = []


def _parse_user_ids(raw: str) -> set:
    out = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            _BAD_ALLOWED.append(part)
    return out


ALLOWED_USERS = _parse_user_ids(os.environ.get("ALLOWED_USERS", ""))

# Разрешённые группы (chat_id, обычно отрицательные). В такой группе писать боту
# может любой участник, НО слэш-команды и системный промпт остаются только за
# ALLOWED_USERS. Вне этого списка действует прежнее правило: только свои юзеры.
ALLOWED_GROUPS = _parse_user_ids(os.environ.get("ALLOWED_GROUPS", ""))

# Повторно сообщать об отказе одному и тому же человеку в одном чате не чаще
# этого. С выключенным privacy mode группа иначе получит отказ на каждое
# сообщение каждого постороннего — бот сам станет спамером. 0 = отвечать всегда.
DENY_NOTICE_COOLDOWN_S = float(os.environ.get("DENY_NOTICE_COOLDOWN_S", "600"))

# База для относительных путей в /cd. Абсолютный путь и ~ идут мимо неё.
PROJECT_BASE_DIR = os.environ.get("PROJECT_BASE_DIR", "") or CLAUDE_WORKDIR

# Запереть /cd внутри PROJECT_BASE_DIR. По умолчанию выключено: фильтра по
# chat_id нет, поэтому каталог сможет назначить любой, кто нашёл бота.
PROJECT_STRICT = os.environ.get("PROJECT_STRICT", "0") not in ("0", "false", "no")

# Модель по умолчанию для новых чатов. Пусто = решает сам Claude Code
# (а он по умолчанию тянется к самой мощной, что для болтовни в мессенджере
# избыточно и быстро съедает квоту).
DEFAULT_MODEL = os.environ.get("CLAUDE_MODEL", "sonnet").strip()

# Алиасы для /model. Полное имя вида claude-sonnet-4-6 тоже принимается как есть.
MODEL_ALIASES = ("opus", "sonnet", "haiku")

STATE_DB = pathlib.Path(os.environ.get("STATE_DB", "state.db"))

# TODO: параллельная работа многих чатов не продумана.
# Сейчас каждый активный чат держит собственный процесс claude -p: своя память,
# свои MCP-соединения, свой контекст. Семафор ограничивает только одновременные
# генерации, но не число живых процессов. На 3-5 чатах это нормально, на 20+
# машина начнёт задыхаться, а расход квоты станет непредсказуемым.
# Что надо решить, если дойдёт до масштаба:
#   - потолок на число одновременно живых процессов + вытеснение по LRU
#     (самый давно неактивный чат гасится, session_id остаётся в базе,
#     следующее сообщение поднимет его через --resume);
#   - приоритеты: личка важнее групп, или наоборот;
#   - учёт нагрузки на подписку в разрезе чатов, чтобы один болтливый чат
#     не подрезал остальные и интерактивную работу в Claude Code;
#   - бэкпрешер: если очередь ходов длиннее N, честно писать в чат
#     "занят, отвечу позже" вместо молчания.
MAX_CONCURRENT_TURNS = int(os.environ.get("MAX_CONCURRENT_TURNS", "2"))

# Ждём столько после последнего сообщения, прежде чем отдать всё Клоду
# одним ходом. Спасает от «три сообщения подряд = три хода».
DEBOUNCE_S = float(os.environ.get("DEBOUNCE_S", "1.2"))

# Тишина дольше этого — гасим процесс. Свежая сессия дешевле раздутой.
IDLE_TIMEOUT_S = float(os.environ.get("IDLE_TIMEOUT_S", "900"))

# Потолок на один ход. Дольше — считаем, что процесс завис.
TURN_TIMEOUT_S = float(os.environ.get("TURN_TIMEOUT_S", "600"))

# Фоновая задача (Bash run_in_background) по завершении сама переинвокает агента
# — он выдаёт ещё один ход с результатом уже ПОСЛЕ того, как исходный ход закрылся
# событием result. Чтобы этот автономный ход не потерялся, после хода с фоновой
# задачей держим процесс живым и дочитываем stdout ещё столько секунд.
BG_KEEPALIVE_S = float(os.environ.get("BG_KEEPALIVE_S", "1800"))
# После доставки автономного ответа, не породившего новых фоновых задач, ждём
# ещё столько на «соседние» завершения — и гасим, не держа процесс всё окно.
BG_TAIL_GRACE_S = float(os.environ.get("BG_TAIL_GRACE_S", "120"))
# Рубильник хвостового ридера. ВЫКЛ по умолчанию: он дочитывает stdout после
# ответа, но при неудачном тайминге недочитанный автономный сегмент остаётся в
# пайпе и уезжает в следующий ход — бот «отвечает на прошлое сообщение». Пока
# не вычищен этот рассинхрон, держим выключенным (поведение как до правки).
BG_TAIL_READER = os.environ.get("BG_TAIL_READER", "0") not in ("0", "false", "no")

# После перезапуска скрипта возобновляем сессию только если пауза меньше этого.
# Дольше — начинаем чисто: старый контекст всё равно раздут, а история диалога
# при необходимости читается прямо из чата.
RESUME_MAX_AGE_S = float(os.environ.get("RESUME_MAX_AGE_S", "7200"))

# Окно контекста модели — для /context и автопредупреждения.
CONTEXT_WINDOW = int(os.environ.get("CONTEXT_WINDOW", "200000"))
CONTEXT_WARN_RATIO = float(os.environ.get("CONTEXT_WARN_RATIO", "0.7"))

# Потоковая выдача: ответ дописывается в сообщение по мере генерации.
# Требует флага --include-partial-messages; при старте проверяется, что
# установленная версия claude его поддерживает.
STREAMING = os.environ.get("STREAMING", "1") not in ("0", "false", "no")

# Не чаще одной правки в EDIT_INTERVAL_S на чат: Telegram отдаёт 429
# примерно при одной правке в секунду.
EDIT_INTERVAL_S = float(os.environ.get("EDIT_INTERVAL_S", "1.1"))

# Порог, после которого текущее сообщение закрывается и начинается новое.
SPLIT_AT = 3800

# Три падения подряд — перестаём поднимать процесс на этот срок.
CRASH_LIMIT = 3
CRASH_COOLDOWN_S = 600.0

SYSTEM_APPEND = os.environ.get(
    "CLAUDE_SYSTEM_APPEND",
    "You're replying in Telegram, which renders a limited legacy-Markdown "
    "subset: *bold*, _italic_, `inline code`, and ``` code blocks ``` work. "
    "Headers (#), tables and nested formatting don't render — avoid those, "
    "use a plain '-' for list items instead. Keep it compact. In group "
    "chats, messages arrive prefixed with [Name]: — that's how you tell "
    "people apart. Incoming photos/videos/documents/voice are saved to disk "
    "and mentioned as a file path in the chat, not inlined. To send the "
    "user a file (image, document, etc.) yourself, write the marker exactly "
    "as [[send: path]] with DOUBLE square brackets, on its own line (path "
    "relative to the working directory or absolute). The bridge intercepts "
    "that marker, uploads the file as a real attachment, and strips the "
    "marker from the text — so the file is actually delivered, not just "
    "named. Don't describe the path in prose expecting it to be sent; only "
    "the [[send: ...]] marker triggers an upload.",
)

TG_API = "https://api.telegram.org/bot{token}/{method}"
TG_MSG_LIMIT = 4000

# Малозаметная метка перед каждым сообщением от самого бриджа (ошибки,
# статусы, помощь), чтобы в чате было видно, что это не ответ Клода.
SYS_TAG = "_SYSTEM_"


def sys_text(body: str) -> str:
    return f"{SYS_TAG} · {body}"

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bridge")


# --------------------------------------------------------------------------
# Персистентность session_id
# --------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sessions ("
        "chat_id INTEGER PRIMARY KEY, session_id TEXT, ts REAL)"
    )
    # Миграция для баз, созданных до появления выбора модели.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sessions)")}
    if "model" not in cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN model TEXT")
    if "system_prompt" not in cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN system_prompt TEXT")
    if "workdir" not in cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN workdir TEXT")
    if "paused" not in cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN paused INTEGER NOT NULL DEFAULT 0")
    # Отложенные задачи: в нужный момент промпт впрыскивается в актор чата,
    # как обычное сообщение — тот же ход, стриминг и --resume.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scheduled ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "chat_id INTEGER NOT NULL, "
        "run_at REAL NOT NULL, "
        "prompt TEXT NOT NULL, "
        "repeat_secs REAL NOT NULL DEFAULT 0, "
        "created_at REAL NOT NULL, "
        "created_by INTEGER)"
    )
    return conn


def save_system_prompt(chat_id: int, prompt: Optional[str]):
    """Живёт дольше сессий: подставляется в каждый новый процесс."""
    try:
        with _db() as c:
            c.execute(
                "INSERT INTO sessions (chat_id, system_prompt, ts) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET system_prompt=excluded.system_prompt",
                (chat_id, prompt, time.time()),
            )
    except Exception as e:
        log.error("save_system_prompt: %s", e)


def load_system_prompt(chat_id: int) -> Optional[str]:
    try:
        with _db() as c:
            row = c.execute(
                "SELECT system_prompt FROM sessions WHERE chat_id=?", (chat_id,)
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        log.error("load_system_prompt: %s", e)
        return None


def save_model(chat_id: int, model: str):
    """Выбор модели переживает сброс сессии и перезапуск скрипта."""
    try:
        with _db() as c:
            c.execute(
                "INSERT INTO sessions (chat_id, model, ts) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET model=excluded.model",
                (chat_id, model, time.time()),
            )
    except Exception as e:
        log.error("save_model: %s", e)


def load_model(chat_id: int) -> Optional[str]:
    try:
        with _db() as c:
            row = c.execute(
                "SELECT model FROM sessions WHERE chat_id=?", (chat_id,)
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        log.error("load_model: %s", e)
        return None


def save_workdir(chat_id: int, workdir: str):
    """Каталог переживает /clear, /compact и перезапуск скрипта."""
    try:
        with _db() as c:
            c.execute(
                "INSERT INTO sessions (chat_id, workdir, ts) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET workdir=excluded.workdir",
                (chat_id, workdir, time.time()),
            )
    except Exception as e:
        log.error("save_workdir: %s", e)


def load_workdir(chat_id: int) -> Optional[str]:
    try:
        with _db() as c:
            row = c.execute(
                "SELECT workdir FROM sessions WHERE chat_id=?", (chat_id,)
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        log.error("load_workdir: %s", e)
        return None


def save_paused(chat_id: int, paused: bool):
    """Пауза чата переживает /clear, /compact и перезапуск скрипта."""
    try:
        with _db() as c:
            c.execute(
                "INSERT INTO sessions (chat_id, paused, ts) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET paused=excluded.paused",
                (chat_id, 1 if paused else 0, time.time()),
            )
    except Exception as e:
        log.error("save_paused: %s", e)


def load_paused(chat_id: int) -> bool:
    try:
        with _db() as c:
            row = c.execute(
                "SELECT paused FROM sessions WHERE chat_id=?", (chat_id,)
            ).fetchone()
        return bool(row[0]) if row and row[0] else False
    except Exception as e:
        log.error("load_paused: %s", e)
        return False


def resolve_workdir(raw: str) -> Tuple[Optional[str], str]:
    """
    Разбирает аргумент /cd. Возвращает (путь, причина отказа).
    Относительный путь считается от PROJECT_BASE_DIR, ~ разворачивается.
    """
    p = raw.strip().strip('"').strip("'")
    if not p:
        return None, "Empty path."
    try:
        path = pathlib.Path(p).expanduser()
        if not path.is_absolute():
            path = pathlib.Path(PROJECT_BASE_DIR).expanduser() / path
        path = path.resolve()
    except Exception as e:
        return None, f"Couldn't parse path: {e}"

    if not path.exists():
        return None, f"Directory doesn't exist: {path}"
    if not path.is_dir():
        return None, f"Not a directory: {path}"

    if PROJECT_STRICT:
        base = pathlib.Path(PROJECT_BASE_DIR).expanduser().resolve()
        # is_relative_to появился в 3.9; сравнение по частям надёжнее строкового
        # префикса, который считает /srv/app-old вложенным в /srv/app.
        if base != path and base not in path.parents:
            return None, f"PROJECT_STRICT: only {base} and its subdirectories are allowed."

    return str(path), ""


def save_session(chat_id: int, session_id: str):
    try:
        with _db() as c:
            c.execute(
                "INSERT INTO sessions (chat_id, session_id, ts) VALUES (?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET "
                "session_id=excluded.session_id, ts=excluded.ts",
                (chat_id, session_id, time.time()),
            )
    except Exception as e:
        log.error("save_session: %s", e)


def load_session(chat_id: int) -> Optional[Tuple[str, float]]:
    try:
        with _db() as c:
            row = c.execute(
                "SELECT session_id, ts FROM sessions WHERE chat_id=?", (chat_id,)
            ).fetchone()
        return (row[0], row[1]) if row and row[0] else None
    except Exception as e:
        log.error("load_session: %s", e)
        return None


def clear_session(chat_id: int):
    """Забываем сессию, но не выбор модели — он живёт дольше контекста."""
    try:
        with _db() as c:
            c.execute(
                "UPDATE sessions SET session_id=NULL, ts=? WHERE chat_id=?",
                (time.time(), chat_id),
            )
    except Exception as e:
        log.error("clear_session: %s", e)


# --------------------------------------------------------------------------
# Отложенные задачи (крон с промптом)
# --------------------------------------------------------------------------

SCHEDULER_TICK_S = 15.0            # как часто проверяем очередь
MAX_TASKS_PER_CHAT = 50           # чтоб один чат не забил планировщик

_DUR_RE = re.compile(r"(\d+)\s*([smhdw])", re.I)
_UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(s: str) -> Optional[float]:
    """'30m', '2h', '1h30m', '1d' -> секунды. None, если не разобрать."""
    s = s.strip().lower()
    if not s:
        return None
    total = 0
    pos = 0
    for m in _DUR_RE.finditer(s):
        if m.start() != pos:      # мусор между числами — не наш формат
            return None
        total += int(m.group(1)) * _UNIT_S[m.group(2)]
        pos = m.end()
    if pos != len(s) or total <= 0:
        return None
    return float(total)


def next_time_at(hhmm: str) -> Optional[float]:
    """'18:00' -> ближайший локальный момент с этим временем (сегодня/завтра)."""
    try:
        hh, mm = hhmm.split(":")
        hh, mm = int(hh), int(mm)
    except Exception:
        return None
    if not (0 <= hh < 24 and 0 <= mm < 60):
        return None
    now = time.time()
    lt = time.localtime(now)
    # isdst=-1 — mktime сам разберётся с переводом часов
    t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hh, mm, 0, 0, 0, -1))
    if t <= now:
        t += 86400
    return t


def fmt_when(run_at: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(run_at))


def add_task(chat_id: int, run_at: float, prompt: str,
             repeat_secs: float, created_by: Optional[int]) -> Optional[int]:
    try:
        with _db() as c:
            n = c.execute(
                "SELECT COUNT(*) FROM scheduled WHERE chat_id=?", (chat_id,)
            ).fetchone()[0]
            if n >= MAX_TASKS_PER_CHAT:
                return None
            cur = c.execute(
                "INSERT INTO scheduled (chat_id, run_at, prompt, repeat_secs, created_at, created_by) "
                "VALUES (?,?,?,?,?,?)",
                (chat_id, run_at, prompt, repeat_secs, time.time(), created_by),
            )
            return cur.lastrowid
    except Exception as e:
        log.error("add_task: %s", e)
        return None


def list_tasks(chat_id: int) -> List[Tuple]:
    try:
        with _db() as c:
            return c.execute(
                "SELECT id, run_at, prompt, repeat_secs FROM scheduled "
                "WHERE chat_id=? ORDER BY run_at",
                (chat_id,),
            ).fetchall()
    except Exception as e:
        log.error("list_tasks: %s", e)
        return []


def cancel_task(chat_id: int, task_id: int) -> bool:
    """Удаляет задачу только в пределах своего чата (чужую не тронуть)."""
    try:
        with _db() as c:
            cur = c.execute(
                "DELETE FROM scheduled WHERE id=? AND chat_id=?", (task_id, chat_id)
            )
            return cur.rowcount > 0
    except Exception as e:
        log.error("cancel_task: %s", e)
        return False


def due_tasks(now: float) -> List[Tuple]:
    try:
        with _db() as c:
            return c.execute(
                "SELECT id, chat_id, run_at, prompt, repeat_secs FROM scheduled "
                "WHERE run_at<=? ORDER BY run_at",
                (now,),
            ).fetchall()
    except Exception as e:
        log.error("due_tasks: %s", e)
        return []


def _settle_task(task_id: int, run_at: float, repeat_secs: float, now: float):
    """После срабатывания: разовую удаляем, повторную двигаем в будущее.
    Двигаем сразу за now (а не на +repeat от старого run_at), чтобы после
    простоя моста не выстрелить пачкой пропущенных срабатываний."""
    try:
        with _db() as c:
            if repeat_secs > 0:
                nxt = run_at + repeat_secs
                while nxt <= now:
                    nxt += repeat_secs
                c.execute("UPDATE scheduled SET run_at=? WHERE id=?", (nxt, task_id))
            else:
                c.execute("DELETE FROM scheduled WHERE id=?", (task_id,))
    except Exception as e:
        log.error("_settle_task: %s", e)


# --------------------------------------------------------------------------
# Тонкий клиент Telegram (long polling — белый IP не нужен)
# --------------------------------------------------------------------------

_BOLD_DBL_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_BOLD_UNDERSCORE_RE = re.compile(r"__(.+?)__", re.DOTALL)


def _normalize_md(text: str) -> str:
    """Модель по системному промпту должна писать *bold* (легаси-Markdown
    Telegram), но иногда сваливается в привычный GFM **bold**/__bold__.
    Двойной маркер telegram трактует как пустую сущность и роняет парсинг
    всего сообщения целиком — тогда call_md откатывается в plain text, и
    юзер видит сырые звёздочки без форматирования вообще. Сводим двойные
    маркеры к одинарным до отправки, чтобы разметка выживала в любом случае."""
    text = _BOLD_DBL_RE.sub(r"*\1*", text)
    text = _BOLD_UNDERSCORE_RE.sub(r"*\1*", text)
    return text


class Telegram:
    def __init__(self, token: str, session: aiohttp.ClientSession):
        self.token = token
        self.session = session

    async def call(self, method: str, **params):
        url = TG_API.format(token=self.token, method=method)
        timeout = aiohttp.ClientTimeout(total=params.pop("_timeout", 30))
        async with self.session.post(url, json=params, timeout=timeout) as resp:
            data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"{method} failed: {data}")
        return data["result"]

    async def call_md(self, method: str, **params):
        """Как call(), но рендерит Markdown и откатывается в plain text,
        если разметка не закрыта (например, посреди стриминга дельт)."""
        if "text" in params:
            params["text"] = _normalize_md(params["text"])
        try:
            return await self.call(method, parse_mode="Markdown", **params)
        except RuntimeError as e:
            if "can't parse entities" in str(e).lower():
                return await self.call(method, **params)
            raise

    async def send(self, chat_id: int, text: str, reply_to: Optional[int] = None):
        for chunk in _split(text, TG_MSG_LIMIT):
            try:
                await self.call_md(
                    "sendMessage",
                    chat_id=chat_id,
                    text=chunk,
                    reply_to_message_id=reply_to,
                    allow_sending_without_reply=True,
                )
            except Exception as e:
                log.error("sendMessage: %s", e)
            reply_to = None

    async def typing(self, chat_id: int):
        try:
            await self.call("sendChatAction", chat_id=chat_id, action="typing")
        except Exception:
            pass

    async def download_file(self, file_id: str) -> bytes:
        """Скачивает файл по file_id (фото, видео, документ — что угодно)."""
        info = await self.call("getFile", file_id=file_id)
        file_path = info["file_path"]
        url = f"https://api.telegram.org/file/bot{self.token}/{file_path}"
        async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            resp.raise_for_status()
            return await resp.read()

    @staticmethod
    async def _await_ready_file(
        path: pathlib.Path, *,
        appear: float = 4.0, quiet: float = 1.2, timeout: float = 60.0, poll: float = 0.4,
    ) -> bool:
        """Ждёт, пока путь станет готовым к отправке файлом.

        True  — файл появился и запись завершилась (размер/mtime замерли).
        False — за `appear` секунд обычного файла так и не возникло (или это
                папка): почти наверняка метку лишь упомянули в тексте, а не
                вызвали командой — слать нечего (см. SkipSend).

        Ждём именно по размеру, а не только ловим Errno 13 на чтении: писатель
        (рендер, ffmpeg) может и не держать эксклюзивный лок — тогда read_bytes
        вернул бы обрезанный файл, и уехала бы битая картинка без ошибки.
        """
        # Фаза 1 — дождаться появления обычного файла.
        start = time.monotonic()
        while not path.is_file():
            if path.is_dir() or time.monotonic() - start >= appear:
                return False
            await asyncio.sleep(poll)

        # Фаза 2 — дождаться, пока размер и mtime замрут на `quiet` секунд.
        deadline = time.monotonic() + timeout
        last_sig = None
        stable_since = None
        while True:
            try:
                st = path.stat()
                sig = (st.st_size, st.st_mtime_ns)
                nonzero = st.st_size > 0
            except (FileNotFoundError, OSError):
                sig, nonzero = None, False

            now = time.monotonic()
            if sig is not None and sig == last_sig and nonzero:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= quiet:
                    return True
            else:
                stable_since = None
                last_sig = sig

            if now >= deadline:
                return True               # не устаканился, но файл есть — пусть решает чтение
            await asyncio.sleep(poll)

    @staticmethod
    async def _read_file_bytes(path: pathlib.Path) -> bytes:
        """Читает файл, дождавшись конца записи и пережив эксклюзивный лок.
        Если реального файла нет — поднимает SkipSend (тихий пропуск)."""
        if not await Telegram._await_ready_file(path):
            raise SkipSend(str(path))
        # Подстраховка от эксклюзивного лока в момент чтения: даём дописаться и
        # пробуем снова, с нарастающей паузой.
        last: Optional[Exception] = None
        for i in range(9):
            try:
                return path.read_bytes()
            except (PermissionError, OSError) as e:
                last = e
                await asyncio.sleep(min(0.5 * (i + 1), 3.0))
        raise RuntimeError(f"still locked/unreadable after retries: {last}")

    async def send_file(self, chat_id: int, path: pathlib.Path):
        """Отправляет локальный файл: картинки — как sendPhoto, остальное — sendDocument."""
        payload = await self._read_file_bytes(path)
        is_image = path.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp")
        method, field = ("sendPhoto", "photo") if is_image else ("sendDocument", "document")
        url = TG_API.format(token=self.token, method=method)
        data = aiohttp.FormData()
        data.add_field("chat_id", str(chat_id))
        data.add_field(field, payload, filename=path.name)
        timeout = aiohttp.ClientTimeout(total=120)
        async with self.session.post(url, data=data, timeout=timeout) as resp:
            result = await resp.json()
        if not result.get("ok"):
            raise RuntimeError(f"{method} failed: {result}")


def _split(text: str, limit: int) -> List[str]:
    text = (text or "").strip() or "(empty response)"
    if len(text) <= limit:
        return [text]
    parts, buf = [], ""
    for line in text.splitlines(keepends=True):
        while len(line) > limit:
            parts.append(line[:limit])
            line = line[limit:]
        if len(buf) + len(line) > limit:
            parts.append(buf)
            buf = line
        else:
            buf += line
    if buf:
        parts.append(buf)
    return parts


def _fmt_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


# TODO: ненадёжный текстовый протокол отправки файлов.
# Отправка держится на том, что модель дословно напечатает метку
# [[send: путь]], а мост выловит её регуляркой. Это принципиально
# вероятностно: модель роняет скобку, заворачивает метку в кодблок или
# перефразирует — и файл молча не уходит, а сырая строка утекает в текст
# (ровно этот баг и ловили). Терпимая регулярка ниже (1-2 скобки) — стоп-гэп,
# а не решение: она же возвращает риск ложных срабатываний, ради ухода от
# которого двойные скобки изначально и вводили. Надёжно чинится только уходом
# от текстового протокола к структурированному вызову (отдельный инструмент
# «отправить файл», который модель вызывает явно, без парсинга прозы).
# Конкретную реализацию пока не фиксируем.
#
# Основная форма — [[send: путь]]. Одинарные скобки [send: путь] тоже
# принимаем как стоп-гэп: модель нередко роняет вторую скобку.
_SEND_RE = re.compile(r"\[\[?\s*send:\s*(.+?)\s*\]\]?")
# Ещё не закрытая метка в хвосте растущего сообщения (её дописывают прямо
# сейчас) — чтобы в стриминге не мигало полусырое "[send: C:\...".
_SEND_RE_TAIL = re.compile(r"\[\[?\s*send:[^\]\n]*$")


def _extract_send_files(answer: str, workdir: str) -> Tuple[str, List[pathlib.Path]]:
    """Достаёт из ответа Клода метки [[send: путь]]. Возвращает (текст без меток, пути)."""
    paths = []
    for m in _SEND_RE.finditer(answer):
        p = pathlib.Path(m.group(1))
        if not p.is_absolute():
            p = pathlib.Path(workdir) / p
        paths.append(p)
    return _SEND_RE.sub("", answer).strip(), paths


class StaleSession(RuntimeError):
    """--resume не смог подняться: сессия протухла или не найдена."""


class Interrupted(RuntimeError):
    """Пользователь оборвал текущий ход командой /stop."""


class SkipSend(RuntimeError):
    """Метка [[send:]] указала не на реальный файл (не появился за grace-окно
    или это папка). Почти наверняка маркер лишь упомянут в тексте, а не вызван
    как команда — отправлять нечего, ошибку в чат не шлём."""


# --------------------------------------------------------------------------
# Актор: один процесс claude -p на чат
# --------------------------------------------------------------------------

class StreamingMessage:
    """
    Одно растущее сообщение в Telegram.

    Дельты копятся в буфере, отрисовка — не чаще EDIT_INTERVAL_S.
    При переполнении лимита сообщение закрывается и начинается новое,
    поэтому done_len хранит, сколько символов уже ушло в закрытые.
    """

    def __init__(self, tg: "Telegram", chat_id: int, workdir: str = "."):
        self.tg = tg
        self.chat_id = chat_id
        self.workdir = workdir
        self.message_id: Optional[int] = None
        self.buf = ""        # текст текущего сообщения (сырой, с метками)
        self.shown = ""      # что уже отрисовано в текущем сообщении
        self.status = ""     # чем занят Клод, пока нет текста
        self.done_len = 0    # символов ответа в закрытых сообщениях
        self.started = False # была ли хоть одна дельта
        self.sent_paths: set = set()  # что уже ушло как файл — от повторной отправки
        self._task: Optional[asyncio.Task] = None
        self._closed = False
        self._interval = EDIT_INTERVAL_S

    # -- отправка медиа прямо по ходу стриминга ----------------------------

    def _harvest_sends(self):
        """Дописанные метанки [[send: путь]] превращаем в отправку файла и
        вырезаем из буфера — сразу, не дожидаясь конца хода, чтобы метка не
        оставалась строкой в размышлениях."""
        if "send:" not in self.buf:
            return
        self.buf = _SEND_RE.sub(self._on_marker, self.buf)

    def _on_marker(self, m: "re.Match") -> str:
        self._fire_send((m.group(1) or "").strip())
        return ""

    def _fire_send(self, raw: str):
        if not raw:
            return
        p = pathlib.Path(raw)
        if not p.is_absolute():
            p = pathlib.Path(self.workdir) / p
        key = str(p)
        if key in self.sent_paths:   # дедуп: и внутри стрима, и против финального result
            return
        self.sent_paths.add(key)
        asyncio.create_task(self._deliver(p))

    async def _deliver(self, p: pathlib.Path):
        try:
            await self.tg.send_file(self.chat_id, p)
        except SkipSend:
            log.info("[%s] send-маркер без реального файла, пропускаю: %s", self.chat_id, p)
        except Exception as e:
            log.error("[%s] send_file %s: %s", self.chat_id, p, e)
            try:
                await self.tg.send(self.chat_id, sys_text(f"Couldn't send {p.name}: {e}"))
            except Exception:
                pass

    def _display_buf(self) -> str:
        """Буфер без незакрытой метки в хвосте — её не показываем, пока
        дописывается (закрытые метки уже вырезал _harvest_sends)."""
        m = _SEND_RE_TAIL.search(self.buf)
        return self.buf[:m.start()] if m else self.buf

    def start(self):
        self._task = asyncio.create_task(self._loop())

    def append(self, delta: str):
        if delta:
            self.buf += delta
            self.started = True

    def set_status(self, status: str):
        self.status = status

    async def _loop(self):
        try:
            while not self._closed:
                await asyncio.sleep(self._interval)
                try:
                    await self._render()
                except Exception as e:
                    log.debug("[%s] render: %s", self.chat_id, e)
        except asyncio.CancelledError:
            return

    async def _render(self, final: bool = False):
        # Сначала вырезаем дописанные метки [[send:]] и отправляем файлы —
        # в тексте они мелькать не должны.
        self._harvest_sends()

        # Статус (какой тул сейчас крутится) дописываем под текстом, а не только
        # пока текста ещё нет — иначе после первой же дельты дальнейшие вызовы
        # инструментов происходят молча и выглядят как зависание.
        body = self._display_buf()
        target = f"{body}\n\n{self.status}" if self.status else body
        if not target:
            return

        # Текущее сообщение переросло лимит — закрываем его и начинаем новое.
        while len(self.buf) > SPLIT_AT:
            cut = self.buf.rfind("\n", 0, SPLIT_AT)
            if cut < SPLIT_AT // 2:
                cut = SPLIT_AT
            head, self.buf = self.buf[:cut], self.buf[cut:].lstrip("\n")
            await self._push(head, close=True)
            self.done_len += len(head)
            self.message_id = None
            self.shown = ""
            body = self._display_buf()
            target = f"{body}\n\n{self.status}" if self.status else body

        if target != self.shown:
            await self._push(target)

    async def _push(self, text: str, close: bool = False):
        text = text.strip()
        if not text:
            return
        try:
            if self.message_id is None:
                res = await self.tg.call_md(
                    "sendMessage", chat_id=self.chat_id, text=text
                )
                self.message_id = res["message_id"]
            else:
                await self.tg.call_md(
                    "editMessageText",
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=text,
                )
            self.shown = "" if close else text
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg:
                # Притормаживаем: лимит правок на чат.
                self._interval = min(self._interval * 1.5, 5.0)
                log.debug("[%s] rate limit, интервал -> %.1f", self.chat_id, self._interval)
            elif "not modified" in msg:
                self.shown = text
            else:
                log.debug("[%s] push: %s", self.chat_id, msg)

    async def finish(self, final_text: Optional[str]):
        """Дорисовывает окончательный текст из события result."""
        self._closed = True
        if self._task:
            self._task.cancel()
        self.status = ""  # финальный текст не должен тащить хвост "⚙️ тул…"

        # result авторитетнее накопленных дельт.
        if final_text and final_text.strip():
            self.buf = final_text[self.done_len:] if self.done_len else final_text

        try:
            await self._render(final=True)
        except Exception as e:
            log.error("[%s] finish: %s", self.chat_id, e)

    async def abort(self):
        self._closed = True
        if self._task:
            self._task.cancel()


class ChatActor:
    def __init__(self, chat_id: int, tg: Telegram, sem: asyncio.Semaphore):
        self.chat_id = chat_id
        self.tg = tg
        self.sem = sem

        self.proc: Optional[asyncio.subprocess.Process] = None
        self.session_id: Optional[str] = None
        self.resumed = False
        self.model: str = load_model(chat_id) or DEFAULT_MODEL
        self.system_prompt: Optional[str] = load_system_prompt(chat_id)
        self.workdir: str = load_workdir(chat_id) or CLAUDE_WORKDIR
        # Пауза: бот не реагирует на обычные сообщения этого чата, пока /resume.
        self.paused: bool = load_paused(chat_id)

        # Режим «следующее сообщение — это системный промпт».
        self.awaiting_system: float = 0.0

        # Конспект от /compact — подмешивается к следующему сообщению.
        self.pending_seed: Optional[str] = None

        self.mailbox: List[str] = []
        self.lock = asyncio.Lock()          # держится, пока идёт ход
        self.last_activity = time.monotonic()

        # метрики последнего хода
        self.context_tokens = 0
        self._last_msg_usage: dict = {}     # usage последнего "assistant"-события хода
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False

        self._debounce: Optional[asyncio.Task] = None
        self._drain: Optional[asyncio.Task] = None
        self._force_fresh = False
        self._interrupt = False          # /stop попросил оборвать текущий ход
        self._crashes = 0
        self._blocked_until = 0.0

        # «Хвостовой» ридер: дочитывает автономные ходы (переинвок фоновой задачи)
        # после того, как обычный ход закрылся. _bg_deadline — до какого момента
        # держим процесс живым в ожидании фонового ответа.
        self._tail: Optional[asyncio.Task] = None
        self._tail_state = "idle"        # "idle" ждёт первую строку сегмента / "reading" внутри сегмента
        self._preempt = False            # новый ход просит хвост уступить stdout
        self._bg_deadline = 0.0          # monotonic; пока now < него — не гасим и держим хвост

    # -- вход ---------------------------------------------------------------

    async def submit(self, text: str):
        """Кладём в мейлбокс и сдвигаем дебаунс. Ход запустится сам."""
        self.mailbox.append(text)
        self.last_activity = time.monotonic()
        if self._debounce and not self._debounce.done():
            self._debounce.cancel()
        self._debounce = asyncio.create_task(self._after_debounce())

    async def interrupt(self) -> bool:
        """Обрывает текущий ход. Возвращает False, если обрывать нечего.

        Ставим флаг и гасим процесс: readline в _read_turn разблокируется,
        увидит флаг и поднимет Interrupted — ход завершится, уже показанный
        кусок ответа останется, очередь чистится. Сессия восстановится через
        --resume на следующем сообщении.
        """
        # Снимаем ещё не запущенный (в дебаунсе) ввод.
        self.mailbox.clear()
        if self._debounce and not self._debounce.done():
            self._debounce.cancel()
        # /stop прекращает и ожидание фонового ответа.
        self._bg_deadline = 0.0
        tail_running = bool(self._tail and not self._tail.done())
        if not self.lock.locked() and not tail_running:
            return False
        self._interrupt = True
        await self.kill()          # заодно гасит хвостовой ридер
        return True

    async def _after_debounce(self):
        try:
            await asyncio.sleep(DEBOUNCE_S)
        except asyncio.CancelledError:
            return
        # Если ход уже идёт — он сам подберёт мейлбокс, когда закончит.
        if self.lock.locked():
            return
        if self._drain is None or self._drain.done():
            self._drain = asyncio.create_task(self._drain_loop())

    async def _drain_loop(self):
        async with self.lock:
            # Хвостовой ридер держит stdout — заберём его себе, пока идёт ход.
            await self._preempt_tail()
            while self.mailbox:
                merged = "\n".join(self.mailbox).strip()
                self.mailbox.clear()
                if not merged:
                    continue
                # Конспект от /compact уходит вместе с первым сообщением
                # новой сессии — отдельный ход на него не тратится.
                if self.pending_seed:
                    merged = (
                        "Конспект предыдущей части разговора "
                        "(контекст был сжат, продолжай с учётом этого):\n"
                        f"{self.pending_seed}\n\n---\n\n{merged}"
                    )
                    self.pending_seed = None
                try:
                    await self._run_turn(merged)
                except Exception as e:
                    log.exception("[%s] ход упал", self.chat_id)
                    await self.kill()
                    await self.tg.send(
                        self.chat_id,
                        sys_text(f"⚠️ Session crashed: {e}\nNext message will start a fresh one."),
                    )
                    break
        # Ход оставил висящую фоновую задачу — слушаем её автономный ответ.
        self._maybe_start_tail()

    # -- хвостовой ридer: автономные ходы после result ----------------------

    def _maybe_start_tail(self):
        """Поднимает хвостовой ридер, если после хода осталась висеть фоновая
        задача (до _bg_deadline) и процесс жив."""
        if not BG_TAIL_READER:
            return
        if self._tail and not self._tail.done():
            return
        if not self.alive():
            return
        if time.monotonic() >= self._bg_deadline:
            return
        self._tail = asyncio.create_task(self._tail_reader())

    async def _preempt_tail(self):
        """Забирает stdout у хвостового ридера перед обычным ходом: два читателя
        одного пайпа недопустимы. Если ридер ждёт первую строку (idle) — рвём
        сразу (данные не теряются, буфер stdout сохраняется); если он в середине
        автономного сегмента — даём его дочитать, но ограниченно."""
        t = self._tail
        if not t or t.done():
            self._tail = None
            return
        self._preempt = True
        try:
            if self._tail_state == "idle":
                t.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=45)
            except asyncio.TimeoutError:
                t.cancel()                       # застрял в длинном сегменте — рвём
                try:
                    await t
                except BaseException:
                    pass
            except BaseException:
                pass
        finally:
            self._tail = None
            self._preempt = False
            self._tail_state = "idle"

    async def _tail_reader(self):
        """Пока процесс жив и есть незакрытая фоновая задача — читаем stdout и
        отдаём каждый автономный ход (переинвок по завершению фона) в чат новым
        сообщением. Между сегментами висим на readline, ничего не тратя."""
        log.info("[%s] tail-reader: жду автономный ответ (~%.0fs)",
                 self.chat_id, max(0.0, self._bg_deadline - time.monotonic()))
        delivered = 0
        try:
            while self.alive() and not self._preempt and time.monotonic() < self._bg_deadline:
                stream = StreamingMessage(self.tg, self.chat_id, self.workdir) if STREAMING else None
                if stream:
                    stream.start()
                self._tail_state = "idle"
                before = self._bg_deadline
                try:
                    answer = await self._read_turn(
                        stream,
                        on_activity=lambda: setattr(self, "_tail_state", "reading"),
                        crash_on_eof=False,
                    )
                except asyncio.CancelledError:
                    if stream:
                        await stream.abort()
                    raise
                except (Interrupted, StaleSession, RuntimeError):
                    # процесс закрыл stdout / умер — просто выходим, не крашим.
                    if stream:
                        await stream.abort()
                    return
                # Полный сегмент получили — отдаём его целиком (даже если тем
                # временем пришёл _preempt: терять готовый ответ нельзя).
                await self._deliver_segment(answer, stream)
                delivered += 1
                self.last_activity = time.monotonic()
                log.info("[%s] tail-reader: автономный ответ доставлен (#%d)", self.chat_id, delivered)
                # Ход не породил новых фоновых задач (_bg_deadline не сдвинулся) —
                # значит это, вероятно, финал: ждём ещё чуть-чуть на соседние
                # завершения и сворачиваемся, не удерживая процесс всё окно.
                if self._bg_deadline == before:
                    self._bg_deadline = min(self._bg_deadline, time.monotonic() + BG_TAIL_GRACE_S)
        finally:
            self._tail_state = "idle"

    # -- процесс ------------------------------------------------------------

    def alive(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def _spawn(self):
        cmd = [
            CLAUDE_BIN,
            "-p",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
        ]

        if STREAMING:
            cmd += ["--include-partial-messages"]
        if self.model:
            cmd += ["--model", self.model]

        self.resumed = False
        if not self._force_fresh:
            row = load_session(self.chat_id)
            if row and (time.time() - row[1]) < RESUME_MAX_AGE_S:
                cmd += ["--resume", row[0]]
                self.resumed = True
                log.info("[%s] resume %s", self.chat_id, row[0])
        self._force_fresh = False

        appends = [p for p in (SYSTEM_APPEND, self.system_prompt) if p]
        if appends:
            cmd += ["--append-system-prompt", "\n\n".join(appends)]
        cmd += CLAUDE_EXTRA_ARGS

        log.info("[%s] spawn: %s", self.chat_id, " ".join(shlex.quote(c) for c in cmd))
        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self.workdir,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=STDOUT_LINE_LIMIT,
        )
        if not self.resumed:
            self.session_id = None
            self.context_tokens = 0
            self._last_msg_usage = {}
            self.turns = 0
            self._warned_context = False
        asyncio.create_task(self._drain_stderr(self.proc))

    async def _drain_stderr(self, proc):
        if not proc or not proc.stderr:
            return
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            log.debug("[%s] stderr: %s", self.chat_id, line.decode(errors="replace").rstrip())

    async def kill(self):
        # Гасим хвостовой ридер — но не сам себя, если kill вызван из него
        # (его собственный _read_turn на EOF зовёт kill).
        t = self._tail
        if t and t is not asyncio.current_task() and not t.done():
            self._tail = None
            t.cancel()
        proc, self.proc = self.proc, None
        if proc and proc.returncode is None:
            try:
                if proc.stdin and not proc.stdin.is_closing():
                    proc.stdin.close()
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    async def reset(self):
        """Полный сброс контекста: процесс убит, привязка к сессии стёрта."""
        await self.kill()
        clear_session(self.chat_id)
        self.session_id = None
        self.context_tokens = 0
        self._last_msg_usage = {}
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False
        self.mailbox.clear()
        self._crashes = 0
        self._blocked_until = 0.0
        self._bg_deadline = 0.0

    async def set_system_prompt(self, prompt: Optional[str]) -> str:
        """
        Промпт переживает /clear, /compact и перезапуск скрипта:
        он не в контексте сессии, а во флаге запуска процесса.
        Поэтому применяется только при следующем спавне — гасим процесс.
        """
        self.system_prompt = (prompt or "").strip() or None
        save_system_prompt(self.chat_id, self.system_prompt)
        self.awaiting_system = 0.0

        was_live = self.alive()
        await self.kill()  # session_id сохраняем: контекст восстановится через --resume

        tail = " Process will restart on the next message, context is preserved." if was_live else ""
        if self.system_prompt:
            return f"System prompt set ({len(self.system_prompt)} chars).{tail}"
        return f"System prompt cleared.{tail}"

    async def set_model(self, model: str) -> str:
        """Смена модели требует рестарта процесса — контекст теряется."""
        old, self.model = self.model, model
        save_model(self.chat_id, model)
        had_context = self.alive() or bool(self.session_id)
        await self.reset()
        self.model = model  # reset не трогает модель, но перестрахуемся
        note = "\nContext reset — model only changes when the process (re)starts." if had_context else ""
        return f"Model: {old or 'default'} → {model}{note}"

    async def set_workdir(self, raw: str) -> str:
        """
        Смена каталога всегда рвёт сессию: --resume ищет её в каталоге,
        из которого она была создана, и в новом месте просто не найдёт.
        """
        path, err = resolve_workdir(raw)
        if not path:
            return err

        if path == self.workdir:
            return f"Already in that directory: {path}"

        old = self.workdir
        had_context = self.alive() or bool(self.session_id)
        await self.reset()
        self.workdir = path
        save_workdir(self.chat_id, path)

        note = "\nContext reset — sessions are tied to the working directory." if had_context else ""
        return f"Directory: {old} → {path}{note}"

    async def compact(self) -> str:
        """
        Аналог /compact из CLI, которого в headless нет.
        Просим сессию сжать саму себя, ответ в чат не показываем,
        процесс убиваем, конспект подмешиваем к следующему сообщению.
        """
        if not self.alive() and not self.session_id:
            return "Nothing to compact — no active session."

        before = self.context_tokens
        prompt = (
            "Сожми весь наш разговор в компактный конспект для переноса в новую сессию. "
            "Включи: принятые решения, установленные факты, текущую задачу, открытые вопросы, "
            "важные имена/пути/значения. Опусти рассуждения и то, что уже неактуально. "
            "Выведи только конспект, без вступления и комментариев."
        )

        async with self.lock:          # не влезаем в середину чужого хода
            await self._preempt_tail()  # stdout нужен нам, а не хвостовому ридеру
            async with self.sem:
                summary = await self._turn_once(prompt)

        if not summary or summary == "(timeout)":
            return "Couldn't compact context — session didn't respond. Try /clear."

        await self.kill()
        clear_session(self.chat_id)
        self.session_id = None
        self.context_tokens = 0
        self._last_msg_usage = {}
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False
        self.pending_seed = summary.strip()

        was = f"{_fmt_tokens(before)} " if before else ""
        return (
            f"Context compacted. Was {was}→ summary is {len(self.pending_seed)} chars.\n"
            f"It'll be sent along with your next message."
        )

    # -- один ход -----------------------------------------------------------

    async def _run_turn(self, text: str):
        if time.monotonic() < self._blocked_until:
            await self.tg.send(
                self.chat_id,
                sys_text("⏸ Session disabled after repeated crashes. Check the logs and that `claude` is authenticated."),
            )
            return

        self._interrupt = False
        stream = StreamingMessage(self.tg, self.chat_id, self.workdir) if STREAMING else None

        try:
            async with self.sem:
                answer = await self._turn_once(text, stream)

                # Протухший --resume: чистим привязку и поднимаемся заново.
                if answer is None:
                    log.warning("[%s] resume не поднялся, стартую чисто", self.chat_id)
                    if stream:
                        await stream.abort()
                        stream = StreamingMessage(self.tg, self.chat_id, self.workdir)
                    clear_session(self.chat_id)
                    self._force_fresh = True
                    await self.kill()
                    answer = await self._turn_once(text, stream)
                    if answer is None:
                        raise RuntimeError("процесс claude не стартует")
        except Interrupted:
            # Оставляем уже показанный кусок ответа и уже отправленные файлы.
            if stream:
                await stream.finish(None)
            self._interrupt = False
            self.last_activity = time.monotonic()
            await self.tg.send(self.chat_id, sys_text("⏹ Stopped. Next message continues the session."))
            return

        self._crashes = 0
        self.last_activity = time.monotonic()

        await self._deliver_segment(answer, stream)
        await self._maybe_warn_context()

    async def _deliver_segment(self, answer: str, stream: Optional["StreamingMessage"]):
        """Отдаёт готовый сегмент (ответ хода) в чат: финализирует стрим-сообщение
        и досылает файлы из меток [[send:]]. Общий путь для обычного и для
        автономного (фонового) хода."""
        answer, files_to_send = _extract_send_files(answer, self.workdir)
        already = stream.sent_paths if stream else set()

        if stream and (stream.started or stream.message_id):
            await stream.finish(answer)
        else:
            if stream:
                await stream.abort()
            # Пустой текст при наличии файла не гоним — это был чистый [[send:]].
            if answer or not files_to_send:
                await self.tg.send(self.chat_id, answer)

        for path in files_to_send:
            if str(path) in already:      # уже ушёл по ходу стриминга
                continue
            try:
                await self.tg.send_file(self.chat_id, path)
            except SkipSend:
                log.info("[%s] send-маркер без реального файла, пропускаю: %s", self.chat_id, path)
            except Exception as e:
                log.error("[%s] send_file %s: %s", self.chat_id, path, e)
                await self.tg.send(self.chat_id, sys_text(f"Couldn't send {path.name}: {e}"))

    async def _turn_once(self, text: str, stream: Optional["StreamingMessage"] = None) -> Optional[str]:
        """Один ход. None = сессия не поднялась (кандидат на протухший resume)."""
        if not self.alive():
            await self._spawn()

        payload = json.dumps(
            {"type": "user", "message": {"role": "user", "content": text}},
            ensure_ascii=False,
        ) + "\n"

        assert self.proc and self.proc.stdin
        try:
            self.proc.stdin.write(payload.encode())
            await self.proc.stdin.drain()
        except Exception:
            if self.resumed:
                return None
            self._note_crash()
            raise RuntimeError("процесс claude недоступен")

        # Индикатор «печатает» держим и при стриминге тоже: текст растёт не
        # непрерывно (паузы на тул-коллах, thinking), а без него в такие
        # моменты не отличить «завис» от «работает».
        typing = asyncio.create_task(self._typing_loop())
        if stream is not None:
            stream.start()
        try:
            return await asyncio.wait_for(self._read_turn(stream), timeout=TURN_TIMEOUT_S)
        except StaleSession:
            return None
        except asyncio.TimeoutError:
            await self.kill()
            self._note_crash()
            await self.tg.send(
                self.chat_id, sys_text("⏱ Turn didn't finish in time. Session restarted.")
            )
            return "(timeout)"
        finally:
            if typing:
                typing.cancel()

    async def _typing_loop(self):
        try:
            while True:
                await self.tg.typing(self.chat_id)
                await asyncio.sleep(4)
        except asyncio.CancelledError:
            return

    async def _read_turn(
        self,
        stream: Optional["StreamingMessage"] = None,
        on_activity=None,
        crash_on_eof: bool = True,
    ) -> str:
        """Читает stream-json до события result. Возвращает текст ответа.

        on_activity: колбэк, вызывается один раз при первой строке сегмента
            (хвостовому ридеру — отметить, что он уже внутри сегмента).
        crash_on_eof: считать закрытие stdout крашем. Для хвостового ридера
            False — там EOF это штатное завершение процесса, не краш.
        """
        assert self.proc and self.proc.stdout
        collected: List[str] = []
        got_init = False
        seen_line = False

        while True:
            raw = await self.proc.stdout.readline()
            if not raw:
                # /stop уронил процесс намеренно — это не краш.
                if self._interrupt:
                    await self.kill()
                    raise Interrupted()
                await self.kill()
                # Умер, не дойдя до init, на возобновлённой сессии —
                # почти наверняка session_id протух.
                if self.resumed and not got_init:
                    raise StaleSession()
                if crash_on_eof:
                    self._note_crash()
                raise RuntimeError("процесс claude закрыл stdout")

            line = raw.decode(errors="replace").strip()
            if not line:
                continue
            if not seen_line:
                seen_line = True
                if on_activity:
                    try:
                        on_activity()
                    except Exception:
                        pass
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                log.debug("[%s] non-json: %s", self.chat_id, line[:200])
                continue

            etype = ev.get("type")

            # Частичные сообщения: дельты текста и старт блоков.
            if stream is not None and etype in ("stream_event", "content_block_delta", "content_block_start"):
                inner = ev.get("event") if etype == "stream_event" else ev
                self._feed_stream(stream, inner or {})
                continue

            if etype == "system" and ev.get("subtype") == "init":
                got_init = True
                sid = ev.get("session_id")
                if sid:
                    self.session_id = sid
                    # Пишем сразу, до первого ответа: краш посреди хода
                    # не должен терять привязку чата к сессии.
                    save_session(self.chat_id, sid)
                log.info("[%s] session %s", self.chat_id, self.session_id)

            elif etype == "assistant":
                message = ev.get("message", {})
                for block in message.get("content", []) or []:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "text":
                        collected.append(block.get("text", ""))
                    elif block.get("type") == "tool_use" and BG_TAIL_READER and self._is_background_tool(block):
                        # Модель запустила фоновую задачу: по её завершению агент
                        # сам переинвокнется отдельным ходом ПОСЛЕ result. Держим
                        # процесс живым и включаем хвостовой ридер на это окно.
                        self._bg_deadline = time.monotonic() + BG_KEEPALIVE_S
                        log.info("[%s] замечена фоновая задача, keepalive до +%.0fs", self.chat_id, BG_KEEPALIVE_S)
                usage = message.get("usage")
                if usage:
                    self._last_msg_usage = usage

            elif etype == "result":
                self._absorb_usage(ev)
                if ev.get("is_error"):
                    raise RuntimeError(ev.get("result") or "claude вернул ошибку")
                final = ev.get("result")
                if isinstance(final, str) and final.strip():
                    return final
                return "\n".join(collected)

    @staticmethod
    def _is_background_tool(block: dict) -> bool:
        """tool_use-блок — это Bash, запущенный в фоне (run_in_background)."""
        if block.get("name") != "Bash":
            return False
        inp = block.get("input")
        return isinstance(inp, dict) and bool(inp.get("run_in_background"))

    @staticmethod
    def _feed_stream(stream: "StreamingMessage", ev: dict):
        """Вытаскивает текстовые дельты и имена инструментов из частичных событий."""
        t = ev.get("type")
        if t == "content_block_delta":
            delta = ev.get("delta") or {}
            if delta.get("type") == "text_delta":
                stream.append(delta.get("text") or "")
        elif t == "content_block_start":
            block = ev.get("content_block") or {}
            if block.get("type") == "tool_use":
                name = block.get("name") or "инструмент"
                stream.set_status(f"⚙️ {name}…")
            elif block.get("type") == "text":
                stream.set_status("")  # снова пошёл текст — статус тула убираем

    def _absorb_usage(self, ev: dict):
        """Достаёт размер контекста из последнего usage хода.

        В событии result поле usage суммирует input/cache_read/cache_creation
        по всем внутренним шагам хода (каждый вызов инструмента — отдельный
        шаг с растущим cache_read), а не отражает фактический размер
        контекста на конец хода. Поэтому берём usage последнего сообщения
        assistant (self._last_msg_usage, см. _read_turn) — это одиночный
        API-вызов без накопления, ровно тот контекст, что был отправлен
        модели перед финальным ответом. result.usage — запасной вариант,
        если по какой-то причине assistant-событие не пришло.
        """
        usage = self._last_msg_usage or ev.get("usage") or ev.get("message", {}).get("usage") or {}
        try:
            total = (
                int(usage.get("input_tokens", 0) or 0)
                + int(usage.get("cache_read_input_tokens", 0) or 0)
                + int(usage.get("cache_creation_input_tokens", 0) or 0)
                + int(usage.get("output_tokens", 0) or 0)
            )
            if total:
                self.context_tokens = total
        except Exception:
            pass
        # num_turns считает шаги внутри одного запроса (вызовы инструментов),
        # а не ходы диалога: без инструментов он всегда 1, поэтому раньше
        # счётчик сессии стоял на единице. Ходы считаем сами, а num_turns
        # оставляем как отдельную метрику — по ней видно работу инструментов.
        self.turns += 1
        self.last_turn_steps = int(ev.get("num_turns") or 1)
        log.debug("[%s] usage=%s ctx=%s", self.chat_id, usage, self.context_tokens)

    async def _maybe_warn_context(self):
        if not self.context_tokens or self._warned_context:
            return
        if self.context_tokens >= CONTEXT_WINDOW * CONTEXT_WARN_RATIO:
            self._warned_context = True
            pct = 100 * self.context_tokens / CONTEXT_WINDOW
            await self.tg.send(
                self.chat_id,
                sys_text(
                    f"ℹ️ Context is {pct:.0f}% full "
                    f"({_fmt_tokens(self.context_tokens)} / {_fmt_tokens(CONTEXT_WINDOW)}). "
                    f"There's no auto-compaction here — the turn will fail on overflow. /clear starts over."
                ),
            )

    def context_report(self) -> str:
        state = "paused" if self.paused else ("generating" if self.lock.locked() else ("running" if self.alive() else "stopped"))
        if not self.context_tokens:
            body = "Context: no data yet (no turns in this session)"
        else:
            pct = 100 * self.context_tokens / CONTEXT_WINDOW
            body = (
                f"Context: {_fmt_tokens(self.context_tokens)} / "
                f"{_fmt_tokens(CONTEXT_WINDOW)} ({pct:.0f}%)"
            )
        lines = [
            f"Model: {self.model or 'default'}",
            f"Directory: {self.workdir}",
            body,
            f"Turns in session: {self.turns}"
            + (f" (steps in last turn: {self.last_turn_steps})" if self.last_turn_steps > 1 else ""),
            f"Process: {state}{' (resumed)' if self.resumed else ''}",
            f"session_id: {self.session_id or '—'}",
            f"Queued: {len(self.mailbox)}",
        ]
        if self.system_prompt:
            lines.append(f"System prompt: set, {len(self.system_prompt)} chars.")
        if self.pending_seed:
            lines.append(f"Waiting on a /compact summary: {len(self.pending_seed)} chars.")
        return "\n".join(lines)

    def _note_crash(self):
        self._crashes += 1
        if self._crashes >= CRASH_LIMIT:
            self._blocked_until = time.monotonic() + CRASH_COOLDOWN_S
            log.error("[%s] крашлуп, пауза %.0f c", self.chat_id, CRASH_COOLDOWN_S)


# --------------------------------------------------------------------------
# Супервизор
# --------------------------------------------------------------------------

class Supervisor:
    def __init__(self, tg: Telegram):
        self.tg = tg
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_TURNS)
        self.actors: Dict[int, ChatActor] = {}

    def actor(self, chat_id: int) -> ChatActor:
        if chat_id not in self.actors:
            self.actors[chat_id] = ChatActor(chat_id, self.tg, self.sem)
        return self.actors[chat_id]

    async def reaper(self):
        """Гасит процессы, простоявшие дольше IDLE_TIMEOUT_S."""
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            for a in list(self.actors.values()):
                if (a.alive() and not a.lock.locked()
                        and now - a.last_activity > IDLE_TIMEOUT_S
                        and now >= a._bg_deadline):     # ждём автономный ответ фона — не гасим
                    log.info("[%s] простой %.0f c — гашу процесс", a.chat_id, now - a.last_activity)
                    await a.kill()

    async def scheduler(self):
        """Крон с промптом: в срок впрыскивает задачу в актор чата как обычное
        сообщение — тот же ход, стриминг ответа и продолжение сессии."""
        while True:
            await asyncio.sleep(SCHEDULER_TICK_S)
            now = time.time()
            for task_id, chat_id, run_at, prompt, repeat_secs in due_tasks(now):
                # Сначала фиксируем (удаляем/двигаем), потом отправляем — так
                # перезапуск моста в момент выстрела не продублирует задачу.
                _settle_task(task_id, run_at, repeat_secs, now)
                try:
                    actor = self.actor(chat_id)
                    fire = (
                        f"⏰ Запланированная задача #{task_id} — время выполнить её сейчас:\n"
                        f"{prompt}"
                    )
                    await self.tg.send(
                        chat_id, sys_text(f"⏰ Запускаю задачу #{task_id}…")
                    )
                    await actor.submit(fire)
                    log.info("[%s] задача #%s сработала", chat_id, task_id)
                except Exception as e:
                    log.error("[%s] задача #%s не запустилась: %s", chat_id, task_id, e)

    async def shutdown(self):
        for a in list(self.actors.values()):
            await a.kill()


# --------------------------------------------------------------------------
# Приём апдейтов
# --------------------------------------------------------------------------

AWAIT_SYSTEM_S = 300.0

SCHED_USAGE = (
    "Schedule a task (the bot runs the prompt itself at that time):\n"
    "/in 45m <task> — once, after an interval (30m, 2h, 1h30m, 1d)\n"
    "/at 18:30 <task> — once, at the next HH:MM\n"
    "/every 2h <task> — repeat on an interval\n"
    "/tasks — list · /tasks del <id> — remove"
)

HELP = (
    "Commands:\n"
    "/stop — interrupt the current turn (keeps what's shown)\n"
    "/context — model, prompt, context usage, state\n"
    "/system — set a system prompt (as the next message)\n"
    "/compact — compact the conversation into a summary and start fresh\n"
    "/clear — reset context completely\n"
    "/pause — stop reacting to messages here · /resume — start again\n"
    "/model [opus|sonnet|haiku] — switch model (resets context)\n"
    "/cd [path] — working directory (resets context)\n"
    "/in · /at · /every · /tasks — schedule tasks (the bot runs them for you)\n"
    "/help — this message"
)


_deny_notified: Dict[Tuple[int, int], float] = {}


def _incoming_attachment(msg: dict) -> Optional[Tuple[str, str]]:
    """(file_id, предложенное имя файла) для вложения любого типа. None, если вложения нет."""
    if msg.get("photo"):
        f = msg["photo"][-1]          # последний элемент — самое большое разрешение
        return f["file_id"], f"{f['file_unique_id']}.jpg"
    for field, default_ext in (
        ("video", ".mp4"), ("video_note", ".mp4"), ("animation", ".mp4"),
        ("voice", ".ogg"), ("audio", ".mp3"), ("document", ""),
    ):
        f = msg.get(field)
        if not f:
            continue
        ext = mimetypes.guess_extension(f.get("mime_type") or "") or default_ext
        name = f.get("file_name") or f"{f['file_unique_id']}{ext}"
        return f["file_id"], name
    return None


def _should_notify_denial(chat_id: int, user_id: Optional[int]) -> bool:
    """Первый отказ проговариваем, повторы в пределах паузы — молча."""
    if DENY_NOTICE_COOLDOWN_S <= 0:
        return True
    key = (chat_id, user_id or 0)
    now = time.monotonic()
    if now - _deny_notified.get(key, 0.0) < DENY_NOTICE_COOLDOWN_S:
        return False
    if len(_deny_notified) > 1000:      # не растим словарь бесконечно
        _deny_notified.clear()
    _deny_notified[key] = now
    return True


async def handle(sup: Supervisor, tg: Telegram, msg: dict):
    chat_id = msg["chat"]["id"]
    attachment = _incoming_attachment(msg)
    text = (msg.get("text") or msg.get("caption") or "").strip()
    if not text and not attachment:
        return

    # Проверка до создания актора: посторонний не должен поднимать процесс.
    user_id = (msg.get("from") or {}).get("id")
    chat_type = msg.get("chat", {}).get("type")
    is_group = chat_type in ("group", "supergroup")

    # Свой юзер (в whitelist'е, либо whitelist пуст = открыто всем) может всё.
    user_ok = (not ALLOWED_USERS) or (user_id in ALLOWED_USERS)
    # В разрешённой группе писать боту может любой участник.
    group_ok = is_group and chat_id in ALLOWED_GROUPS

    if not (user_ok or group_ok):
        # Отвечаем реплаем на само сообщение: в группе иначе непонятно,
        # что именно проигнорировано. Повторы гасит пауза.
        notify = _should_notify_denial(chat_id, user_id)
        log.log(
            logging.WARNING if notify else logging.DEBUG,
            "отказ: user_id=%s chat_id=%s", user_id, chat_id,
        )
        if notify:
            await tg.send(
                chat_id,
                sys_text(f"Message ignored: no access.\nuser_id: {user_id}"),
                reply_to=msg.get("message_id"),
            )
        return

    # Управлять ботом (слэш-команды, системный промпт) — только whitelist.
    # Остальные участники разрешённой группы могут лишь писать боту.
    privileged = user_ok

    actor = sup.actor(chat_id)

    # Пауза: чат заглушён. Пропускаем только команды от своих (чтобы /resume и
    # прочие настройки работали) — обычные сообщения и вложения молча игнорируем.
    if actor.paused and not (privileged and text.startswith("/")):
        return

    if attachment:
        file_id, filename = attachment
        is_group = msg.get("chat", {}).get("type") != "private"
        who = (msg.get("from") or {}).get("first_name") or "user"

        dest_dir = pathlib.Path(actor.workdir) / "telegram-uploads"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{time.strftime('%Y%m%d-%H%M%S')}_{msg.get('message_id')}_{filename}"
        try:
            data = await tg.download_file(file_id)
            dest.write_bytes(data)
        except Exception as e:
            log.error("[%s] file download: %s", chat_id, e)
            await tg.send(chat_id, sys_text(f"Couldn't download the file: {e}"))
            return

        # Файл лежит в рабочей папке — дальше Клод читает/обрабатывает его
        # своими обычными инструментами (Read, Bash), как в обычном CLI.
        note = f"[Telegram attachment saved to {dest.relative_to(actor.workdir)}]"
        if text:
            note += f"\n{text}"
        if is_group:
            note = f"[{who}]: {note}"
        await actor.submit(note)
        return

    if text.startswith("/"):
        # Команды — только whitelist. Остальные в группе просто пишут боту.
        if not privileged:
            if _should_notify_denial(chat_id, user_id):
                await tg.send(
                    chat_id,
                    sys_text("Commands are for admins only — just message the bot normally."),
                    reply_to=msg.get("message_id"),
                )
            return
        cmd = text.split()[0].split("@")[0].lower()

        if cmd in ("/start", "/help"):
            uid = (msg.get("from") or {}).get("id")
            await tg.send(chat_id, sys_text(f"Ready. chat_id: {chat_id}, user_id: {uid}\n\n{HELP}"))
            return

        if cmd in ("/stop", "/cancel", "/interrupt"):
            stopped = await actor.interrupt()
            await tg.send(
                chat_id,
                sys_text("⏹ Interrupting the current turn." if stopped else "Nothing is running."),
            )
            return

        if cmd in ("/pause", "/mute", "/sleep"):
            actor.paused = True
            save_paused(chat_id, True)
            await tg.send(
                chat_id,
                sys_text("⏸ Paused. I'll ignore messages here until /resume. Commands still work."),
            )
            return

        if cmd in ("/resume", "/unmute", "/wake"):
            was = actor.paused
            actor.paused = False
            save_paused(chat_id, False)
            await tg.send(
                chat_id,
                sys_text("▶️ Resumed — listening again." if was else "Wasn't paused."),
            )
            return

        if cmd in ("/in", "/at", "/every"):
            parts = text.split(maxsplit=2)
            if len(parts) < 3:
                await tg.send(chat_id, sys_text(SCHED_USAGE))
                return
            spec, prompt = parts[1], parts[2].strip()
            repeat = 0.0
            if cmd == "/at":
                run_at = next_time_at(spec)
                if run_at is None:
                    await tg.send(chat_id, sys_text("Bad time. Use HH:MM, e.g. /at 18:30 <task>."))
                    return
            else:
                secs = parse_duration(spec)
                if secs is None:
                    await tg.send(chat_id, sys_text("Bad interval. Use 30m, 2h, 1h30m, 1d, e.g. /in 45m <task>."))
                    return
                run_at = time.time() + secs
                if cmd == "/every":
                    repeat = secs
            uid = (msg.get("from") or {}).get("id")
            tid = add_task(chat_id, run_at, prompt, repeat, uid)
            if tid is None:
                await tg.send(chat_id, sys_text(f"Too many scheduled tasks (limit {MAX_TASKS_PER_CHAT}). Remove some with /tasks del <id>."))
                return
            extra = f", every {spec}" if repeat else ""
            await tg.send(chat_id, sys_text(f"⏰ Task #{tid} scheduled for {fmt_when(run_at)}{extra}."))
            return

        if cmd in ("/tasks", "/jobs", "/sched"):
            parts = text.split()
            if len(parts) >= 3 and parts[1].lower() in ("del", "cancel", "rm", "remove"):
                try:
                    tid = int(parts[2])
                except ValueError:
                    await tg.send(chat_id, sys_text("Usage: /tasks del <id>"))
                    return
                ok = cancel_task(chat_id, tid)
                await tg.send(chat_id, sys_text(f"Task #{tid} removed." if ok else f"No task #{tid} in this chat."))
                return
            rows = list_tasks(chat_id)
            if not rows:
                await tg.send(chat_id, sys_text("No scheduled tasks.\n\n" + SCHED_USAGE))
                return
            lines = ["⏰ Scheduled tasks:"]
            for tid, run_at, prompt, repeat_secs in rows:
                rep = f" (every {int(repeat_secs)}s)" if repeat_secs else ""
                short = prompt if len(prompt) <= 60 else prompt[:57] + "…"
                lines.append(f"#{tid} · {fmt_when(run_at)}{rep} · {short}")
            lines.append("\nRemove: /tasks del <id>")
            await tg.send(chat_id, sys_text("\n".join(lines)))
            return

        if cmd in ("/clear", "/reset", "/new"):
            await actor.reset()
            await tg.send(chat_id, sys_text("Context reset. Next message will start a new session."))
            return

        if cmd in ("/context", "/ctx"):
            await tg.send(chat_id, sys_text(actor.context_report()))
            return

        if cmd == "/model":
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await tg.send(
                    chat_id,
                    sys_text(
                        f"Current: {actor.model or 'default'}\n"
                        f"Change: /model {' | '.join(MODEL_ALIASES)}\n"
                        f"A full name like claude-sonnet-4-6 also works.\n"
                        f"Switching restarts the process — context is lost."
                    ),
                )
                return
            await tg.send(chat_id, sys_text(await actor.set_model(parts[1].strip())))
            return

        if cmd in ("/cd", "/dir", "/project"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                base = pathlib.Path(PROJECT_BASE_DIR).expanduser()
                await tg.send(
                    chat_id,
                    sys_text(
                        f"Current: {actor.workdir}\n"
                        f"Change: /cd <path>\n"
                        f"Relative paths are resolved from {base}"
                        + (" (can't go outside it)" if PROJECT_STRICT else "")
                        + "\nSwitching resets context — sessions are tied to the directory."
                    ),
                )
                return
            await tg.send(chat_id, sys_text(await actor.set_workdir(parts[1])))
            return

        if cmd == "/compact":
            await tg.typing(chat_id)
            await tg.send(chat_id, sys_text(await actor.compact()))
            return

        if cmd in ("/system", "/sys"):
            arg = text.split(maxsplit=1)
            arg = arg[1].strip() if len(arg) > 1 else ""

            if arg.lower() in ("off", "clear", "reset", "-"):
                await tg.send(chat_id, sys_text(await actor.set_system_prompt(None)))
                return
            if arg.lower() == "show":
                await tg.send(
                    chat_id,
                    sys_text(
                        f"Current system prompt:\n\n{actor.system_prompt}"
                        if actor.system_prompt else "No system prompt set."
                    ),
                )
                return
            if arg:
                await tg.send(chat_id, sys_text(await actor.set_system_prompt(arg)))
                return

            # Без аргумента — ждём промпт следующим сообщением.
            actor.awaiting_system = time.monotonic()
            current = (
                f"\n\nCurrently set ({len(actor.system_prompt)} chars), the new one will replace it."
                if actor.system_prompt else ""
            )
            await tg.send(
                chat_id,
                sys_text(
                    "Send the system prompt as your next message.\n"
                    "It'll be applied to every new session in this chat "
                    "and survives /clear, /compact, and bot restarts.\n"
                    "/system off — clear it, /system show — view it." + current
                ),
            )
            return

        # Остальные слэш-команды Claude Code (/cost, /resume, /vim) в headless
        # не работают: их обрабатывает интерактивный REPL, которого здесь нет.
        # Отбиваем, чтобы они молча не улетали в модель как обычный текст.
        await tg.send(chat_id, sys_text(f"Unknown command.\n\n{HELP}"))
        return

    # Ждём системный промпт следующим сообщением (окно 5 минут). Ловим его только
    # от whitelist'а: в группе чужое сообщение не должно стать системным промптом.
    if actor.awaiting_system and privileged:
        if time.monotonic() - actor.awaiting_system < AWAIT_SYSTEM_S:
            await tg.send(chat_id, sys_text(await actor.set_system_prompt(text)))
            return
        actor.awaiting_system = 0.0
        await tg.send(chat_id, sys_text("⌛ Waited too long for the system prompt, cancelled. Treating this as a regular message."))

    # В группах Клод должен различать собеседников.
    if msg.get("chat", {}).get("type") != "private":
        who = (msg.get("from") or {}).get("first_name") or "user"
        text = f"[{who}]: {text}"

    await actor.submit(text)


async def poll(sup: Supervisor, tg: Telegram, stop: asyncio.Event):
    offset = 0
    seen: set = set()
    fails = 0

    def backoff() -> float:
        # 2, 4, 8, 16, 30, 30… — чтобы при недоступном Telegram не долбить раз в 3 с.
        return min(2.0 ** min(fails, 5), 30.0)

    while not stop.is_set():
        try:
            updates = await tg.call(
                "getUpdates",
                offset=offset,
                timeout=25,
                allowed_updates=["message"],
                _timeout=40,
            )
        except asyncio.TimeoutError:
            continue
        except (aiohttp.ClientError, OSError) as e:
            # Разрыв длинного соединения — рядовое событие long polling, а не
            # поломка: Telegram закрывает соединение сам. ERROR тут только
            # засоряет лог, поэтому шумим лишь когда обрывы идут подряд.
            fails += 1
            delay = backoff()
            log.log(
                logging.WARNING if fails >= 3 else logging.DEBUG,
                "getUpdates: обрыв связи (%s), подряд %d, пауза %.0f c",
                e, fails, delay,
            )
            await asyncio.sleep(delay)
            continue
        except Exception as e:
            fails += 1
            delay = backoff()
            log.error("getUpdates: %s (пауза %.0f c)", e, delay)
            await asyncio.sleep(delay)
            continue

        fails = 0

        for upd in updates:
            offset = max(offset, upd["update_id"] + 1)
            # Telegram ретраит при таймауте — дедуп до запуска Клода.
            if upd["update_id"] in seen:
                continue
            seen.add(upd["update_id"])
            if len(seen) > 5000:
                seen.clear()
            if "message" in upd:
                asyncio.create_task(handle(sup, tg, upd["message"]))


async def _check_streaming_support():
    """Если версия claude не знает --include-partial-messages, выключаем стриминг."""
    global STREAMING
    if not STREAMING:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            CLAUDE_BIN, "--help",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
        if b"include-partial-messages" not in out:
            STREAMING = False
            log.warning(
                "claude не поддерживает --include-partial-messages, "
                "стриминг выключен (ответы будут приходить целиком)"
            )
    except Exception as e:
        STREAMING = False
        log.warning("не удалось проверить поддержку стриминга (%s), выключаю", e)


async def main():
    if not BOT_TOKEN:
        sys.exit(
            "Не задан TELEGRAM_BOT_TOKEN: скопируйте .env.example в .env "
            "и впишите токен от @BotFather"
        )

    if _BAD_ALLOWED:
        # Молча выкинутый id — это либо запертый владелец, либо дыра в списке.
        log.warning(
            "ALLOWED_USERS: не разобраны и пропущены %s — нужны числовые id",
            ", ".join(repr(x) for x in _BAD_ALLOWED),
        )

    await _check_streaming_support()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows

    async with aiohttp.ClientSession() as session:
        tg = Telegram(BOT_TOKEN, session)
        me = await tg.call("getMe")
        log.info(
            "бот @%s готов | workdir=%s | db=%s | стриминг=%s | доступ: %s | группы: %s",
            me.get("username"), CLAUDE_WORKDIR, STATE_DB,
            "вкл" if STREAMING else "выкл",
            f"{len(ALLOWED_USERS)} user_id" if ALLOWED_USERS else "открыт всем",
            f"{len(ALLOWED_GROUPS)} разрешено" if ALLOWED_GROUPS else "нет",
        )

        sup = Supervisor(tg)
        tasks = [
            asyncio.create_task(poll(sup, tg, stop)),
            asyncio.create_task(sup.reaper()),
            asyncio.create_task(sup.scheduler()),
        ]
        await stop.wait()
        log.info("останавливаюсь…")
        for t in tasks:
            t.cancel()
        await sup.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
