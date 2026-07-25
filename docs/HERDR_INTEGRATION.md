# Herdr integration

jcode running inside a [Herdr](https://herdr.dev) pane reports authoritative
`idle`/`working`/`blocked`/`done` state and its native session id directly to
the Herdr socket, so the pane shows up in the Herdr sidebar with live status
instead of relying on screen-scraping — the same "lifecycle authority" role
OpenCode, Pi, and Kimi use.

Live-validated (2026-07-25) against a real Herdr 0.7.5 server (protocol 18):
a jcode process running as the foreground process in a Herdr pane registers
correctly and shows up in `herdr agent list` with live idle/working state.

## How it works

No installation step is needed. jcode auto-detects Herdr via env vars Herdr
already injects into every pane it spawns (`HERDR_ENV`, `HERDR_SOCKET_PATH`,
`HERDR_PANE_ID`) and reports lifecycle state directly over the socket. This
matches the OpenCode plugin model but needs no plugin file, since the
reporting logic is compiled into the jcode binary.

The wire protocol is newline-delimited JSON over a Unix domain socket at
`$HERDR_SOCKET_PATH`:

```jsonc
// pane.report_agent — authoritative lifecycle state
{"id":"jcode","method":"pane.report_agent","params":{
  "pane_id":"w1:p1","source":"herdr:jcode","agent":"jcode",
  "state":"working","message":"building docs"}}

// pane.report_agent_session — native session id for restore
{"id":"jcode","method":"pane.report_agent_session","params":{
  "pane_id":"w1:p1","source":"herdr:jcode","agent":"jcode",
  "agent_session_id":"<jcode-session-id>"}}
```

`state` is one of `idle`, `working`, `blocked`, `done`.

## Implementation

- `crates/jcode-base/src/herdr.rs` — socket client + env detection.
  Fire-and-forget, one connection per report; zero-cost when not under
  Herdr. Because jcode's client/server architecture can have one
  long-lived server driving many concurrent Herdr panes, identity
  (pane id + socket path) is resolved per-session from a registry
  populated on client `Subscribe`, falling back to the process env for
  the single-pane case (`jcode run`, standalone).
- Wired into the existing lifecycle hook dispatch sites:
  - `crates/jcode-app-core/src/agent.rs` (`fire_session_lifecycle_hook`) —
    `idle` on session create/attach/resume, `done` on session end.
  - `crates/jcode-app-core/src/agent/turn_execution.rs` — `working` on
    turn start, `idle` on turn end.
  - `crates/jcode-base/src/safety.rs` — `blocked` when a permission
    request is queued, `idle` when the last pending one resolves.
  - `crates/jcode-app-core/src/server/client_lifecycle.rs` — registers
    the Herdr identity on `Subscribe`.
- Tests: `crates/jcode-base/src/herdr.rs` unit tests (wire-format
  assertions) plus `crates/jcode-base/tests/herdr_socket.rs` end-to-end
  UDS round-trip tests, including the full
  `request_permission -> blocked -> record_decision -> idle` flow
  through the real `SafetySystem`. 10 tests total, all passing.

## Manual verification

Run `herdr`, start `jcode` in a pane, confirm the sidebar shows `jcode`
with `idle`. Submit a prompt and confirm it flips to `working`, then back
to `idle` when the turn ends.
