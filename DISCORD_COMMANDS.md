# DISCORD_COMMANDS.md

This workspace accepts natural-language control messages from Discord or Telegram.

Natural language is the primary interface.

The user should be able to say things like:

- `现在进度怎么样`
- `下一步做什么`
- `当前最大的 blocker 是什么`
- `继续做刚才那个`
- `帮我检查一下现在能不能提测`
- `把今天的改动总结一下`

Optional fallback:

- If the user wants terse operator control, `proj ...` commands are also allowed.
- Do not require commands when normal language already expresses the intent.

## Goal

Let the human check and steer project progress from chat without needing to open Codex directly.

## Reply Style

- Keep replies short and scannable.
- Use bullets, not tables.
- Lead with the answer, then the next action.
- When useful, include file paths or branch/status facts.
- If you took an action, say exactly what you did.
- If you did not take an action, say why.

## Identity / Provenance

If the user asks things like:

- `session key`
- `cwd`
- `你现在是哪个 session`
- `确认一下是不是本机 codex`

Run `python3 scripts/print_discord_session_context.py` first and answer with the exact values it returns.

Use `.openclaw/discord-session-context.json` only as a fallback cache if the script cannot resolve the live binding.

Do not infer, paraphrase, swap, or rename these fields. Copy the values exactly.

Important mapping:

- `session key` = `openclaw_session_key`
- `session id` = `openclaw_session_id`
- `acpx session id` = `acpx_session_id`
- `cwd` = `workspace`

Preferred shared-chat format:

- `session key`: `<openclaw_session_key>`
- `session id`: `<openclaw_session_id>`
- `acpx session id`: `<acpx_session_id>`
- `cwd`: `<workspace>`

## Natural-Language Intents

Map common shared-chat requests to project-control behavior.

### Progress

If the user asks:

- `现在进度怎么样`
- `最新进展`
- `把项目进度说一下`

Reply with:

- workspace confirmation
- branch / working tree snapshot
- current focus
- latest meaningful progress
- current blockers
- next 1-3 steps

Use recent memory, git state, and current workspace docs.

Before answering, verify:

- workspace is `/Users/marvin.xa/Desktop/Polymarket`
- current branch
- whether a recent check/test result exists

Use this exact progress reply skeleton whenever the user asks for progress in shared chat:

- `workspace`: `/Users/marvin.xa/Desktop/Polymarket`
- `branch`: `<current branch>`
- `checks`: `<latest known test/check result, or "no recent check run">`
- `focus`: `<current focus>`
- `progress`: `<latest meaningful progress>`
- `blockers`: `<current blockers or none>`
- `next`: `<next 1-3 steps>`

If you cannot verify the workspace, say that explicitly and do not pretend the progress is authoritative.

### Next Action

If the user asks:

- `下一步做什么`
- `接下来先干嘛`
- `继续推进`

Reply with the next 1-3 concrete actions.

If "继续" is clear and safe, continue the current task instead of only answering.

### Blockers

If the user asks:

- `现在卡在哪`
- `最大的 blocker 是什么`
- `还有什么没解决`

Reply with blockers, unknowns, and dependencies. If none, say so clearly.

### Latest Changes

If the user asks:

- `今天改了什么`
- `最新提交是什么`
- `把最新改动总结一下`

Summarize recent repo activity and working tree changes in plain language.

### Readiness

If the user asks:

- `现在能提测吗`
- `能发版吗`
- `离上线还差什么`

Give a brief readiness view:

- done
- risky
- missing before ship

Also include:

- workspace confirmation
- latest known check/test result
- whether the answer is based on code changes only or on an actual verification run

### Execute Work

If the user asks:

- `继续修`
- `跑一下测试`
- `帮我处理这个问题`
- `把这个功能继续做完`

Attempt the task in this workspace when it is internal and safe.

For destructive, external, or ambiguous actions, ask before executing.

## Optional `proj` Commands

These are the commands verified to work in this environment:

### `proj help`

Return the supported commands with one-line explanations.

### `proj status`

Report the current git-oriented project state:

- branch
- dirty/clean working tree
- modified and untracked file summary

If useful, add a one-line operator summary.

### `proj latest`

Summarize recent commits or latest meaningful repo activity.

### `proj diff`

Summarize the current uncommitted work in plain language. Do not dump raw patches unless asked.

### `proj log`

Show recent commit history in a compact form.

### `proj check`

Run relevant checks/tests and summarize failures or readiness.

### `proj run <task>`

Attempt the task in this workspace when it is internal and safe.

Examples:

- `proj run tests`
- `proj run inspect failing daemon state test`
- `proj run continue wallet follower fix`

Rules:

- Execute directly when the task is read-only or an internal code/task operation.
- If the task would be destructive, external, credential-sensitive, or ambiguous, ask for confirmation first.
- After execution, report outcome, files touched, and next step.

## Parsing Rules

- Command matching is case-insensitive.
- Ignore extra whitespace.
- If a message starts with `proj ` but does not match a known command, treat it as `proj help`.
- If a normal non-command message arrives, treat it as the primary interface, not as fallback.
- In shared chat, prefer intent recognition over command correction.

## Safety

- Never expose secrets, tokens, private keys, or sensitive local config in chat.
- Never run destructive commands without confirmation.
- Never assume `proj run` means "do anything possible"; keep actions within the current workspace and task.

## Progress Sources

When answering progress commands, prefer these sources in order:

1. `memory/YYYY-MM-DD.md` for recent decisions and progress
2. `git status --short` for current local changes
3. Relevant docs such as `README.md`, plans, checklists, and work notes
4. Latest available check/test evidence from the current workspace

If the sources disagree, say so explicitly.
