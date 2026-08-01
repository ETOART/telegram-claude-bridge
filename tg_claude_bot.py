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
import os
import pathlib
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
TURN_TIMEOUT_S = float(os.environ.get("TURN_TIMEOUT_S", "180"))

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
    "Ты отвечаешь в Telegram. Пиши компактно, без markdown-заголовков и "
    "таблиц — они не рендерятся. Списки и код допустимы. В групповых чатах "
    "сообщения приходят с префиксом [Имя]: — это разные собеседники.",
)

TG_API = "https://api.telegram.org/bot{token}/{method}"
TG_MSG_LIMIT = 4000

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


def resolve_workdir(raw: str) -> Tuple[Optional[str], str]:
    """
    Разбирает аргумент /cd. Возвращает (путь, причина отказа).
    Относительный путь считается от PROJECT_BASE_DIR, ~ разворачивается.
    """
    p = raw.strip().strip('"').strip("'")
    if not p:
        return None, "Пустой путь."
    try:
        path = pathlib.Path(p).expanduser()
        if not path.is_absolute():
            path = pathlib.Path(PROJECT_BASE_DIR).expanduser() / path
        path = path.resolve()
    except Exception as e:
        return None, f"Не удалось разобрать путь: {e}"

    if not path.exists():
        return None, f"Каталог не существует: {path}"
    if not path.is_dir():
        return None, f"Это не каталог: {path}"

    if PROJECT_STRICT:
        base = pathlib.Path(PROJECT_BASE_DIR).expanduser().resolve()
        # is_relative_to появился в 3.9; сравнение по частям надёжнее строкового
        # префикса, который считает /srv/app-old вложенным в /srv/app.
        if base != path and base not in path.parents:
            return None, f"PROJECT_STRICT: разрешён только {base} и вложенные."

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
# Тонкий клиент Telegram (long polling — белый IP не нужен)
# --------------------------------------------------------------------------

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

    async def send(self, chat_id: int, text: str, reply_to: Optional[int] = None):
        for chunk in _split(text, TG_MSG_LIMIT):
            try:
                await self.call(
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


def _split(text: str, limit: int) -> List[str]:
    text = (text or "").strip() or "(пустой ответ)"
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


class StaleSession(RuntimeError):
    """--resume не смог подняться: сессия протухла или не найдена."""


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

    def __init__(self, tg: "Telegram", chat_id: int):
        self.tg = tg
        self.chat_id = chat_id
        self.message_id: Optional[int] = None
        self.buf = ""        # текст текущего сообщения
        self.shown = ""      # что уже отрисовано в текущем сообщении
        self.status = ""     # чем занят Клод, пока нет текста
        self.done_len = 0    # символов ответа в закрытых сообщениях
        self.started = False # была ли хоть одна дельта
        self._task: Optional[asyncio.Task] = None
        self._closed = False
        self._interval = EDIT_INTERVAL_S

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
        target = self.buf if self.buf else self.status
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
            target = self.buf

        if target != self.shown:
            await self._push(target)

    async def _push(self, text: str, close: bool = False):
        text = text.strip()
        if not text:
            return
        try:
            if self.message_id is None:
                res = await self.tg.call(
                    "sendMessage", chat_id=self.chat_id, text=text
                )
                self.message_id = res["message_id"]
            else:
                await self.tg.call(
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

        # Режим «следующее сообщение — это системный промпт».
        self.awaiting_system: float = 0.0

        # Конспект от /compact — подмешивается к следующему сообщению.
        self.pending_seed: Optional[str] = None

        self.mailbox: List[str] = []
        self.lock = asyncio.Lock()          # держится, пока идёт ход
        self.last_activity = time.monotonic()

        # метрики последнего хода
        self.context_tokens = 0
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False

        self._debounce: Optional[asyncio.Task] = None
        self._drain: Optional[asyncio.Task] = None
        self._force_fresh = False
        self._crashes = 0
        self._blocked_until = 0.0

    # -- вход ---------------------------------------------------------------

    async def submit(self, text: str):
        """Кладём в мейлбокс и сдвигаем дебаунс. Ход запустится сам."""
        self.mailbox.append(text)
        self.last_activity = time.monotonic()
        if self._debounce and not self._debounce.done():
            self._debounce.cancel()
        self._debounce = asyncio.create_task(self._after_debounce())

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
                        f"⚠️ Сбой сессии: {e}\nСледующее сообщение поднимет новую.",
                    )
                    break

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
        )
        if not self.resumed:
            self.session_id = None
            self.context_tokens = 0
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
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False
        self.mailbox.clear()
        self._crashes = 0
        self._blocked_until = 0.0

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

        tail = " Процесс перезапустится на следующем сообщении, контекст сохранён." if was_live else ""
        if self.system_prompt:
            return f"Системный промпт установлен ({len(self.system_prompt)} симв.).{tail}"
        return f"Системный промпт снят.{tail}"

    async def set_model(self, model: str) -> str:
        """Смена модели требует рестарта процесса — контекст теряется."""
        old, self.model = self.model, model
        save_model(self.chat_id, model)
        had_context = self.alive() or bool(self.session_id)
        await self.reset()
        self.model = model  # reset не трогает модель, но перестрахуемся
        note = "\nКонтекст сброшен — модель меняется только при старте процесса." if had_context else ""
        return f"Модель: {old or 'по умолчанию'} → {model}{note}"

    async def set_workdir(self, raw: str) -> str:
        """
        Смена каталога всегда рвёт сессию: --resume ищет её в каталоге,
        из которого она была создана, и в новом месте просто не найдёт.
        """
        path, err = resolve_workdir(raw)
        if not path:
            return err

        if path == self.workdir:
            return f"Каталог уже такой: {path}"

        old = self.workdir
        had_context = self.alive() or bool(self.session_id)
        await self.reset()
        self.workdir = path
        save_workdir(self.chat_id, path)

        note = "\nКонтекст сброшен — сессии привязаны к каталогу." if had_context else ""
        return f"Каталог: {old} → {path}{note}"

    async def compact(self) -> str:
        """
        Аналог /compact из CLI, которого в headless нет.
        Просим сессию сжать саму себя, ответ в чат не показываем,
        процесс убиваем, конспект подмешиваем к следующему сообщению.
        """
        if not self.alive() and not self.session_id:
            return "Сжимать нечего — активной сессии нет."

        before = self.context_tokens
        prompt = (
            "Сожми весь наш разговор в компактный конспект для переноса в новую сессию. "
            "Включи: принятые решения, установленные факты, текущую задачу, открытые вопросы, "
            "важные имена/пути/значения. Опусти рассуждения и то, что уже неактуально. "
            "Выведи только конспект, без вступления и комментариев."
        )

        async with self.lock:          # не влезаем в середину чужого хода
            async with self.sem:
                summary = await self._turn_once(prompt)

        if not summary or summary == "(таймаут)":
            return "Не удалось сжать контекст — сессия не ответила. Попробуй /clear."

        await self.kill()
        clear_session(self.chat_id)
        self.session_id = None
        self.context_tokens = 0
        self.turns = 0
        self.last_turn_steps = 0
        self._warned_context = False
        self.pending_seed = summary.strip()

        was = f"{_fmt_tokens(before)} " if before else ""
        return (
            f"Контекст сжат. Было {was}→ конспект на {len(self.pending_seed)} символов.\n"
            f"Он уйдёт вместе со следующим твоим сообщением."
        )

    # -- один ход -----------------------------------------------------------

    async def _run_turn(self, text: str):
        if time.monotonic() < self._blocked_until:
            await self.tg.send(
                self.chat_id,
                "⏸ Сессия отключена после серии сбоев. Проверь логи и что `claude` авторизован.",
            )
            return

        stream = StreamingMessage(self.tg, self.chat_id) if STREAMING else None

        async with self.sem:
            answer = await self._turn_once(text, stream)

            # Протухший --resume: чистим привязку и поднимаемся заново.
            if answer is None:
                log.warning("[%s] resume не поднялся, стартую чисто", self.chat_id)
                if stream:
                    await stream.abort()
                    stream = StreamingMessage(self.tg, self.chat_id)
                clear_session(self.chat_id)
                self._force_fresh = True
                await self.kill()
                answer = await self._turn_once(text, stream)
                if answer is None:
                    raise RuntimeError("процесс claude не стартует")

        self._crashes = 0
        self.last_activity = time.monotonic()

        if stream and (stream.started or stream.message_id):
            await stream.finish(answer)
        else:
            if stream:
                await stream.abort()
            await self.tg.send(self.chat_id, answer)

        await self._maybe_warn_context()

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

        # При стриминге индикатор «печатает» не нужен: виден растущий текст.
        typing = None if stream is not None else asyncio.create_task(self._typing_loop())
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
                self.chat_id, "⏱ Ход не завершился за отведённое время. Сессия перезапущена."
            )
            return "(таймаут)"
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

    async def _read_turn(self, stream: Optional["StreamingMessage"] = None) -> str:
        """Читает stream-json до события result. Возвращает текст ответа."""
        assert self.proc and self.proc.stdout
        collected: List[str] = []
        got_init = False

        while True:
            raw = await self.proc.stdout.readline()
            if not raw:
                await self.kill()
                # Умер, не дойдя до init, на возобновлённой сессии —
                # почти наверняка session_id протух.
                if self.resumed and not got_init:
                    raise StaleSession()
                self._note_crash()
                raise RuntimeError("процесс claude закрыл stdout")

            line = raw.decode(errors="replace").strip()
            if not line:
                continue
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
                for block in ev.get("message", {}).get("content", []) or []:
                    if isinstance(block, dict) and block.get("type") == "text":
                        collected.append(block.get("text", ""))

            elif etype == "result":
                self._absorb_usage(ev)
                if ev.get("is_error"):
                    raise RuntimeError(ev.get("result") or "claude вернул ошибку")
                final = ev.get("result")
                if isinstance(final, str) and final.strip():
                    return final
                return "\n".join(collected)

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

    def _absorb_usage(self, ev: dict):
        """Достаёт размер контекста из result. Поля версионно нестабильны."""
        usage = ev.get("usage") or ev.get("message", {}).get("usage") or {}
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
                f"ℹ️ Контекст занят на {pct:.0f}% "
                f"({_fmt_tokens(self.context_tokens)} / {_fmt_tokens(CONTEXT_WINDOW)}). "
                f"Автосжатия здесь нет — при переполнении ход упадёт. /clear начнёт заново.",
            )

    def context_report(self) -> str:
        state = "генерация" if self.lock.locked() else ("запущен" if self.alive() else "остановлен")
        if not self.context_tokens:
            body = "Контекст: нет данных (ещё не было ходов в этой сессии)"
        else:
            pct = 100 * self.context_tokens / CONTEXT_WINDOW
            body = (
                f"Контекст: {_fmt_tokens(self.context_tokens)} / "
                f"{_fmt_tokens(CONTEXT_WINDOW)} ({pct:.0f}%)"
            )
        lines = [
            f"Модель: {self.model or 'по умолчанию'}",
            f"Каталог: {self.workdir}",
            body,
            f"Ходов в сессии: {self.turns}"
            + (f" (в последнем шагов: {self.last_turn_steps})" if self.last_turn_steps > 1 else ""),
            f"Процесс: {state}{' (возобновлён)' if self.resumed else ''}",
            f"session_id: {self.session_id or '—'}",
            f"В очереди: {len(self.mailbox)}",
        ]
        if self.system_prompt:
            lines.append(f"Системный промпт: задан, {len(self.system_prompt)} симв.")
        if self.pending_seed:
            lines.append(f"Ждёт конспект от /compact: {len(self.pending_seed)} симв.")
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
                if a.alive() and not a.lock.locked() and now - a.last_activity > IDLE_TIMEOUT_S:
                    log.info("[%s] простой %.0f c — гашу процесс", a.chat_id, now - a.last_activity)
                    await a.kill()

    async def shutdown(self):
        for a in list(self.actors.values()):
            await a.kill()


# --------------------------------------------------------------------------
# Приём апдейтов
# --------------------------------------------------------------------------

AWAIT_SYSTEM_S = 300.0

HELP = (
    "Команды:\n"
    "/context — модель, промпт, заполнение контекста, состояние\n"
    "/system — задать системный промпт (следующим сообщением)\n"
    "/compact — сжать разговор в конспект и начать сессию заново\n"
    "/clear — сбросить контекст начисто\n"
    "/model [opus|sonnet|haiku] — сменить модель (сбрасывает контекст)\n"
    "/cd [путь] — рабочий каталог (сбрасывает контекст)\n"
    "/help — это сообщение"
)


async def handle(sup: Supervisor, tg: Telegram, msg: dict):
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or msg.get("caption") or "").strip()
    if not text:
        return

    # Проверка до создания актора: посторонний не должен поднимать процесс.
    if ALLOWED_USERS:
        user_id = (msg.get("from") or {}).get("id")
        if user_id not in ALLOWED_USERS:
            if msg.get("chat", {}).get("type") == "private":
                # Личка: отвечаем, чтобы человек мог назвать свой id владельцу.
                log.warning("отказ: user_id=%s chat_id=%s", user_id, chat_id)
                await tg.send(chat_id, f"Нет доступа.\nТвой user_id: {user_id}")
            else:
                # Группа: молчим. Иначе с выключенным privacy mode бот засыпет
                # чат отказами на каждое сообщение любого участника.
                log.debug("отказ в группе: user_id=%s chat_id=%s", user_id, chat_id)
            return

    actor = sup.actor(chat_id)

    if text.startswith("/"):
        cmd = text.split()[0].split("@")[0].lower()

        if cmd in ("/start", "/help"):
            uid = (msg.get("from") or {}).get("id")
            await tg.send(chat_id, f"Готов. chat_id: {chat_id}, user_id: {uid}\n\n{HELP}")
            return

        if cmd in ("/clear", "/reset", "/new"):
            await actor.reset()
            await tg.send(chat_id, "Контекст сброшен. Следующее сообщение начнёт новую сессию.")
            return

        if cmd in ("/context", "/ctx"):
            await tg.send(chat_id, actor.context_report())
            return

        if cmd == "/model":
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await tg.send(
                    chat_id,
                    f"Сейчас: {actor.model or 'по умолчанию'}\n"
                    f"Сменить: /model {' | '.join(MODEL_ALIASES)}\n"
                    f"Можно и полное имя вида claude-sonnet-4-6.\n"
                    f"Смена перезапускает процесс — контекст теряется.",
                )
                return
            await tg.send(chat_id, await actor.set_model(parts[1].strip()))
            return

        if cmd in ("/cd", "/dir", "/project"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                base = pathlib.Path(PROJECT_BASE_DIR).expanduser()
                await tg.send(
                    chat_id,
                    f"Сейчас: {actor.workdir}\n"
                    f"Сменить: /cd <путь>\n"
                    f"Относительный путь считается от {base}"
                    + (" (выход за неё запрещён)" if PROJECT_STRICT else "")
                    + "\nСмена сбрасывает контекст — сессии привязаны к каталогу.",
                )
                return
            await tg.send(chat_id, await actor.set_workdir(parts[1]))
            return

        if cmd == "/compact":
            await tg.typing(chat_id)
            await tg.send(chat_id, await actor.compact())
            return

        if cmd in ("/system", "/sys"):
            arg = text.split(maxsplit=1)
            arg = arg[1].strip() if len(arg) > 1 else ""

            if arg.lower() in ("off", "clear", "reset", "-"):
                await tg.send(chat_id, await actor.set_system_prompt(None))
                return
            if arg.lower() == "show":
                await tg.send(
                    chat_id,
                    f"Текущий системный промпт:\n\n{actor.system_prompt}"
                    if actor.system_prompt else "Системный промпт не задан.",
                )
                return
            if arg:
                await tg.send(chat_id, await actor.set_system_prompt(arg))
                return

            # Без аргумента — ждём промпт следующим сообщением.
            actor.awaiting_system = time.monotonic()
            current = (
                f"\n\nСейчас задан ({len(actor.system_prompt)} симв.), новый заменит его."
                if actor.system_prompt else ""
            )
            await tg.send(
                chat_id,
                "Пришли системный промпт следующим сообщением.\n"
                "Он будет подставляться в каждую новую сессию этого чата "
                "и переживёт /clear, /compact и перезапуск бота.\n"
                "/system off — снять, /system show — посмотреть." + current,
            )
            return

        # Остальные слэш-команды Claude Code (/cost, /resume, /vim) в headless
        # не работают: их обрабатывает интерактивный REPL, которого здесь нет.
        # Отбиваем, чтобы они молча не улетали в модель как обычный текст.
        await tg.send(chat_id, f"Неизвестная команда.\n\n{HELP}")
        return

    # Ждём системный промпт следующим сообщением (окно 5 минут).
    if actor.awaiting_system:
        if time.monotonic() - actor.awaiting_system < AWAIT_SYSTEM_S:
            await tg.send(chat_id, await actor.set_system_prompt(text))
            return
        actor.awaiting_system = 0.0
        await tg.send(chat_id, "⌛ Ждал системный промпт слишком долго, отменил. Обрабатываю как обычное сообщение.")

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
            "бот @%s готов | workdir=%s | db=%s | стриминг=%s | доступ: %s",
            me.get("username"), CLAUDE_WORKDIR, STATE_DB,
            "вкл" if STREAMING else "выкл",
            f"{len(ALLOWED_USERS)} user_id" if ALLOWED_USERS else "открыт всем",
        )

        sup = Supervisor(tg)
        tasks = [
            asyncio.create_task(poll(sup, tg, stop)),
            asyncio.create_task(sup.reaper()),
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
