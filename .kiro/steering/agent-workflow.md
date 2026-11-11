---
inclusion: always
---

# Agent Workflow Rules

Process rules learned from past sessions. They apply to every task.

## Editing files through the shell
- When no file-edit tool is available, write content in small
  chunks with short lines (under ~80 chars per line).
- Never send one large script or heredoc with long lines in a
  single command; it gets mangled or hangs until timeout.
- Prefer stitching small temp files together, then delete them.

## Failed attempts
- After two failures with the same approach, stop. Diagnose the
  root cause and switch approaches, or give the user the text to
  paste. Do not keep retrying close variations.

## Verifying results
- Confirm an edit landed by checking the target file directly
  (read it, or `git diff -- <file>`), not from a repo-wide
  `git diff --stat` summary.

## Docker containers from test runs
- Run `docker ps` before and after any test run that may start
  containers (Testcontainers, docker compose).
- Remove containers started by that run once it finishes.
- Never remove containers that existed before the run, or that
  belong to another project, without asking the user.
