# tg_claude_bot

A bridge between Telegram and a local Claude Code. A chat message is handed to a `claude -p` process, and the answer comes back to the same chat.

The process does not exit after answering: it stays up and reads stdin. Startup (launching Node, reading configs, connecting to MCP servers) takes 3–10 seconds and happens once, when the process starts. Later messages go into the already-running process, so response time is generation only.

`session_id` is stored in SQLite, so restarting the script or rebooting the machine does not break the conversation: the session is restored via `--resume`.

> Russian original: [README.ru.md](README.ru.md)

---

## Installation

```bash
pip install -r requirements.txt
```

Get a token from [@BotFather](https://t.me/BotFather) with `/newbot`, then put it into `.env`:

```bash
cp .env.example .env
```

```bash
python tg_claude_bot.py
```

`claude` must be installed and authenticated (verify by running `claude` in a terminal).

Settings live in `.env`, which is loaded from the script's own directory — the working directory you launch from does not matter. Real environment variables take precedence over the file, so `TELEGRAM_BOT_TOKEN=... python tg_claude_bot.py` and `docker run --env-file` still work as overrides. [`.env.example`](.env.example) lists every supported variable with its default.

There is no `chat_id` filter: the bot replies in any chat it is added to. Sessions consume your Claude Code subscription quota. To restrict access, add a `chat_id` check at the top of `handle()`.

---

## Architecture

```
Telegram (long polling)
   ↓
Supervisor — semaphore over MAX_CONCURRENT_TURNS concurrent generations
   ↓
ChatActor[chat_id] — one claude -p process per chat
   ↓
stdin:  {"type":"user","message":{...}}   JSONL, one line per message
stdout: system/init → assistant → result  JSONL events
   ↓
SQLite: chat_id → session_id | model | system_prompt | ts
```

**Long polling.** The script polls Telegram for updates via `getUpdates`. No inbound connections are needed: no public IP, port forwarding, TLS certificate or tunnel, and it works behind NAT.

**Debounce.** A message is not handed to the process immediately — the script waits `DEBOUNCE_S` (1.2 s) in case the user keeps typing. Several messages in a row are merged into a single request.

**Message queue.** Messages that arrive during generation accumulate in a list and are handed to the process after the `result` event. A second process is never started for the same chat.

**Idle shutdown.** If a chat has been silent longer than `IDLE_TIMEOUT_S` (15 min), the process is terminated. The `session_id` stays in the database; the next message starts a process with `--resume` if the gap is shorter than `RESUME_MAX_AGE_S` (2 h).

**Chat isolation.** The key is `chat_id`. Every chat and every group gets its own process, its own session and its own settings.

**Streaming output.** The process is started with `--include-partial-messages`, and text deltas arrive as `content_block_delta` events. The answer is sent as a single message and extended via `editMessageText` no more than once per `EDIT_INTERVAL_S` (1.1 s) — Telegram returns 429 at roughly one edit per second. On a 429 the interval is multiplied by 1.5, up to 5 seconds.

Until text starts arriving, the message shows the current tool (`⚙️ Read…`), taken from `content_block_start` events with a `tool_use` block. The `typing` indicator is not used in this mode.

When the message limit (3800 characters) is exceeded, the current message is closed and the rest is sent as a new one. On the `result` event the final text overwrites the accumulated deltas: `result` is authoritative.

Flag support is checked at startup by running `claude --help`. If the flag is missing, streaming is disabled automatically and answers arrive whole. Force it off with `STREAMING=0`.

---

## Commands

| Command | Action |
|---|---|
| `/context` (`/ctx`) | Model, system prompt, context size, turn count, process state, `session_id` |
| `/system` | Set the system prompt with the next message. `/system <text>` — set immediately, `/system show` — print, `/system off` — clear |
| `/compact` | Compress the conversation into a summary and start a new session |
| `/clear` (`/reset`, `/new`) | Terminate the process and drop the session binding. Model and system prompt are kept |
| `/model` | Without an argument — print the current one. `/model sonnet` — switch |
| `/cd` (`/dir`, `/project`) | Without an argument — print the current working directory. `/cd <path>` — switch |
| `/start`, `/help` | Print `chat_id` and the command list |

Any other line starting with `/` is rejected with a hint. Claude Code slash commands (`/cost`, `/resume`, `/vim` and the rest) do not work in `-p` mode — they are handled by the interactive REPL, which is not running here. Without the rejection they would be passed to the model as ordinary text.

### System prompt

`/system` without an argument arms a wait: the next message is saved as the system prompt. The window is 5 minutes, after which the mode is cancelled and the message is handled normally.

The prompt is stored in SQLite per chat and passed via `--append-system-prompt` on every process start. Consequences:

- it survives `/clear`, `/compact`, `/model` and script restarts;
- it takes no room in the conversation context window;
- it only takes effect on the next process start, so setting it terminates the current process. The `session_id` is not dropped, and context is restored via `--resume`.

`CLAUDE_SYSTEM_APPEND` is not replaced but joined with the user prompt by a blank line: base first, then user. To drop the base part, set `CLAUDE_SYSTEM_APPEND=""`.

### Model

The default is `sonnet`, set via `CLAUDE_MODEL`. With nothing specified, Claude Code picks the model itself.

The choice is stored per chat and survives `/clear`, `/compact` and script restarts. `opus`, `sonnet`, `haiku` and full names such as `claude-sonnet-4-6` are accepted.

`--model` is a process start flag and cannot be changed on a running process. Switching the model therefore terminates the process and drops the session binding: context is lost.

### Working directory

Each chat has its own working directory, stored in SQLite and surviving `/clear`, `/compact` and script restarts. Chats that never ran `/cd` use `CLAUDE_WORKDIR`.

Relative paths resolve against `PROJECT_BASE_DIR`, `~` expands to the home directory, absolute paths are taken as they are. The target must exist and be a directory.

Switching always drops the session: `--resume` looks for a session in the directory that created it and will not find it elsewhere.

Note that the directory decides what Claude Code sees. Its default system prompt carries the working directory and git status, so the agent introduces itself as working on whatever project it is pointed at.

`PROJECT_STRICT=1` confines `/cd` to `PROJECT_BASE_DIR` and its subdirectories. It is off by default. Since there is no `chat_id` filter, anyone who finds the bot can set the directory, and the agent has file and shell tools — turn it on to bound the reach.

### Context and `/compact`

In `-p` mode there is no automatic context compaction. Exceeding the context window fails the request. The script warns once per session when the window is `CONTEXT_WARN_RATIO` (70%) full.

`/compact` does four things:

1. Sends a service request into the current session: compress the conversation into a summary (decisions, facts, current task, open questions).
2. Receives the answer and does not post it to the chat.
3. Terminates the process and drops the session binding.
4. Stores the summary and prepends it to the user's next message.

Cost is one extra model request. No separate request is spent on delivering the summary: it rides along with the next message. `/context` shows that a summary is stored and pending.

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | — | Required. Token from BotFather |
| `CLAUDE_BIN` | `claude` | Path to the executable, if it is not on PATH |
| `CLAUDE_MODEL` | `sonnet` | Model for new chats |
| `CLAUDE_WORKDIR` | current directory | Default working directory for chats that have not used `/cd`. `--resume` is bound to the path: saved sessions are not found if the directory changes |
| `PROJECT_BASE_DIR` | `CLAUDE_WORKDIR` | Base for relative paths in `/cd` |
| `PROJECT_STRICT` | `0` | `1` confines `/cd` to `PROJECT_BASE_DIR` and its subdirectories |
| `CLAUDE_EXTRA_ARGS` | empty | Extra flags, e.g. `--mcp-config ./mcp.json` |
| `CLAUDE_SYSTEM_APPEND` | Telegram formatting instructions | Base part of the system prompt |
| `STATE_DB` | `state.db` | SQLite file with settings and chat-to-session bindings |
| `RESUME_MAX_AGE_S` | `7200` | Max session age eligible for `--resume`. Older — new session |
| `CONTEXT_WINDOW` | `200000` | Model context window size, for `/context` |
| `CONTEXT_WARN_RATIO` | `0.7` | Fill ratio at which the warning is emitted |
| `STREAMING` | `1` | Stream the answer. `0` — send it whole |
| `EDIT_INTERVAL_S` | `1.1` | Minimum interval between message edits |
| `MAX_CONCURRENT_TURNS` | `2` | Concurrent generations across all chats |
| `DEBOUNCE_S` | `1.2` | Pause before handing accumulated messages to the process |
| `IDLE_TIMEOUT_S` | `900` | Idle time before the process is terminated |
| `TURN_TIMEOUT_S` | `180` | Maximum duration of one request |
| `LOG_LEVEL` | `INFO` | `DEBUG` prints the `claude` process stderr and `usage` events |

---

## Working in groups

**Privacy mode.** By default BotFather limits a bot's access to group messages: only messages starting with `/` and messages mentioning the bot come through. Turn it off with `/setprivacy` in BotFather. Once off, every group message triggers a model request.

**Sender names.** In non-private chats the text is prefixed with `[Name]:`. This format is described in the base system prompt.

---

## Troubleshooting

**The process exits right after starting.** The likely cause is incompatible flags — the set changes between Claude Code versions:

```bash
claude --help | grep -i "input-format\|output-format\|append-system\|resume\|model"
```

The most likely culprit is `--append-system-prompt`. Disable it with an empty value:

```bash
export CLAUDE_SYSTEM_APPEND=""
```

**Inspecting the `claude` process stderr:**

```bash
LOG_LEVEL=DEBUG python tg_claude_bot.py
```

The same mode prints `result` events in full. Use them to check which fields `usage` is assembled from on your installed version.

**`/context` prints "no data".** The `usage` fields in the `result` event are laid out differently than the parser expects. Fix it in `_absorb_usage()`.

**Every message answers with "session failure".** Check `claude` authentication by running it in a terminal. After three consecutive failures the chat is disabled for 10 minutes.

**The answer jitters or truncates while streaming.** Raise `EDIT_INTERVAL_S` to 2–3 seconds. To disable entirely, use `STREAMING=0`.

**Windows.** stdin/stdout pipe buffering differs from Unix. If it misbehaves, run it from WSL2.

---

## Resource usage

There are no monetary charges: `claude -p` uses subscription authentication, not an API key. It consumes the Claude Code subscription quota shared with interactive sessions. `MAX_CONCURRENT_TURNS`, `IDLE_TIMEOUT_S` and `DEBOUNCE_S` are tuned to keep that consumption in check.

A running process with no model requests does no work and consumes no quota. Consumption comes from process starts and from growing context: every request resends the whole session history.

---

## Limitations

**A running machine is required.** The bot stops when the machine is shut down or sleeps. Continuous operation needs a server; authentication there goes through `CLAUDE_CODE_OAUTH_TOKEN` in the environment (obtained with `claude setup-token`). No code changes are needed.

**Text only.** Images, voice messages and documents are ignored.

**Many chats at once is not worked out.** See the TODO at the top of the file: every active chat holds its own process, with no eviction of inactive ones. It works fine with 3–5 chats; beyond that it needs a cap on concurrently running processes and overload handling.

---

## License

MIT — see [LICENSE](LICENSE).
