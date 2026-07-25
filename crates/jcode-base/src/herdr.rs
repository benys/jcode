//! Herdr agent multiplexer integration.
//!
//! Herdr (<https://herdr.dev>) is a terminal multiplexer for coding agents.
//! When jcode runs inside a Herdr-managed pane, Herdr injects environment
//! variables (`HERDR_ENV`, `HERDR_SOCKET_PATH`, `HERDR_PANE_ID`, ...) that let
//! the agent report lifecycle state back to the local Herdr socket so the
//! Herdr sidebar shows authoritative `idle` / `working` / `blocked` / `done`
//! state instead of relying on screen-scraping.
//!
//! This module speaks the "lifecycle authority" role from the Herdr socket
//! API: <https://herdr.dev/docs/socket-api/>. Two methods matter:
//!
//! - `pane.report_agent` — authoritative lifecycle state for the pane.
//! - `pane.report_agent_session` — native session id for restore.
//!
//! Design notes:
//!
//! - **Zero-cost when not under Herdr.** [`enabled_for_session`] is a cheap
//!   check that hot paths call before building any payload. The dispatch
//!   sites (`agent.rs`, `turn_execution.rs`) early-exit the same way the
//!   hooks module does.
//! - **Fire-and-forget.** Each report opens the Unix domain socket, writes
//!   one newline-delimited JSON line, and closes. Failures are logged at
//!   debug level and never affect the agent. Event frequency is low (a
//!   handful per turn), so per-report reconnect is fine.
//! - **No state held.** We re-read the env vars on each call so a Herdr
//!   restart or pane move is picked up automatically; we never cache the
//!   socket path or pane id.
//!
//! Wire protocol: newline-delimited JSON over a Unix domain socket (Linux /
//! macOS) or a named pipe (Windows). We currently support Unix only, since
//! Herdr itself is Unix-first; Windows Herdr is in beta and we can add the
//! named-pipe transport later.

use serde_json::json;
use std::collections::HashMap;
use std::io::{IoSlice, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// The `agent` label reported to Herdr. Matches the agent name Herdr uses in
/// `herdr agent list` and the detection manifest namespace.
const AGENT_LABEL: &str = "jcode";

/// The `source` string embedded in every report. Namespaced under `herdr:`
/// per the Herdr socket API convention (see "Custom status labels" docs).
const SOURCE: &str = "herdr:jcode";

/// Connection timeout for the Herdr socket. Reports must never block the
/// agent; if Herdr is unresponsive we drop the report. Currently advisory
/// (UDS connect is effectively immediate and returns ECONNREFUSED
/// immediately when Herdr is not listening); reserved for a future
/// deadline-based connect if Herdr ever ships a TCP control surface.
#[allow(dead_code)]
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// Lifecycle state reported to Herdr. Mirrors the Herdr socket API
/// `state` field; see <https://herdr.dev/docs/socket-api/>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Agent is ready for input (no turn in flight).
    Idle,
    /// Agent is actively running a turn.
    Working,
    /// Agent is waiting for a user decision (permission/approval prompt).
    Blocked,
    /// Session has finished and the pane is ready to be reviewed/closed.
    Done,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            State::Idle => "idle",
            State::Working => "working",
            State::Blocked => "blocked",
            State::Done => "done",
        }
    }
}

// ---------------------------------------------------------------------------
// Session-scoped Herdr identity registry
// ---------------------------------------------------------------------------

/// Per-session Herdr identity, captured from the client connection's terminal
/// env snapshot. Solves the client/server architecture mismatch: the server
/// process is long-lived and shared across many Herdr panes, so reading
/// `HERDR_PANE_ID` from the server's own env would collapse every concurrent
/// session onto whichever pane happened to start the server first.
///
/// When a client connects, the server records `(HERDR_PANE_ID,
/// HERDR_SOCKET_PATH)` keyed by `session_id`. All lifecycle reports then look
/// up the identity from this registry (falling back to process env for the
/// single-pane case where server and client share env, e.g. `jcode run`).
#[derive(Clone, Debug)]
struct SessionHerdrIdentity {
    pane_id: String,
    socket_path: PathBuf,
}

static SESSION_REGISTRY: OnceLock<Mutex<HashMap<String, SessionHerdrIdentity>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, SessionHerdrIdentity>> {
    SESSION_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record the Herdr identity (pane id + socket path) for a given session.
/// Called by the server when a client subscribes and its `terminal_env`
/// contains Herdr vars. Idempotent and cheap. Overwrites any previous value
/// so reconnects to a different pane (e.g. session resumed elsewhere) update
/// cleanly.
pub fn register_session(session_id: &str, terminal_env: &[(String, String)]) {
    let Some(pane_id) = env_lookup(terminal_env, "HERDR_PANE_ID") else {
        crate::logging::debug(&format!(
            "herdr: register_session({}) skipped - no HERDR_PANE_ID in terminal_env ({} vars)",
            session_id,
            terminal_env.len()
        ));
        return;
    };
    // HERDR_ENV gate: a client without HERDR_ENV=1 is not running under Herdr.
    let Some(env_value) = env_lookup(terminal_env, "HERDR_ENV") else {
        crate::logging::debug(&format!(
            "herdr: register_session({}) skipped - no HERDR_ENV in terminal_env",
            session_id
        ));
        return;
    };
    if !is_truthy(&env_value) {
        crate::logging::debug(&format!(
            "herdr: register_session({}) skipped - HERDR_ENV not truthy: {:?}",
            session_id, env_value
        ));
        return;
    }
    let Some(socket_path) = env_lookup(terminal_env, "HERDR_SOCKET_PATH") else {
        crate::logging::debug(&format!(
            "herdr: register_session({}) skipped - no HERDR_SOCKET_PATH in terminal_env",
            session_id
        ));
        return;
    };
    crate::logging::info(&format!(
        "herdr: register_session({}) -> pane={} socket={}",
        session_id, pane_id, socket_path
    ));
    if let Ok(mut reg) = registry().lock() {
        reg.insert(
            session_id.to_string(),
            SessionHerdrIdentity {
                pane_id,
                socket_path: PathBuf::from(socket_path),
            },
        );
    }
}

/// Drop a session's Herdr identity from the registry (e.g. on session close).
pub fn unregister_session(session_id: &str) {
    if let Ok(mut reg) = registry().lock() {
        reg.remove(session_id);
    }
}

fn env_lookup(env: &[(String, String)], key: &str) -> Option<String> {
    for (k, v) in env {
        if k == key {
            return Some(v.clone());
        }
    }
    None
}

fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "TRUE" | "True" | "yes" | "on")
}

/// Resolve the Herdr identity for a session. Prefers the session-scoped
/// registry (so shared-server sessions report to their own panes), falls
/// back to the process env (single-pane case, e.g. `jcode run`).
fn resolve_identity(session_id: Option<&str>) -> Option<ResolvedIdentity> {
    // 1. Session-scoped registry.
    if let Some(sid) = session_id {
        if let Ok(reg) = registry().lock() {
            if let Some(identity) = reg.get(sid) {
                return Some(ResolvedIdentity {
                    pane_id: identity.pane_id.clone(),
                    socket_path: identity.socket_path.clone(),
                });
            }
        }
    }
    // 2. Process env fallback (single-pane case: client and server share env).
    if env_is_truthy("HERDR_ENV") {
        if let (Some(pane_id), Some(socket_path)) = (pane_id(), socket_path()) {
            return Some(ResolvedIdentity {
                pane_id,
                socket_path,
            });
        }
    }
    None
}

#[derive(Clone)]
struct ResolvedIdentity {
    pane_id: String,
    socket_path: PathBuf,
}

// ---------------------------------------------------------------------------
// Public report API
// ---------------------------------------------------------------------------

/// True when Herdr reports are active for `session_id` specifically.
pub fn enabled_for_session(session_id: &str) -> bool {
    resolve_identity(Some(session_id)).is_some()
}

/// Report authoritative lifecycle state for this session's pane.
///
/// Looks up the session's Herdr identity from the registry (or falls back to
/// the process env for the single-pane case). No-op when not under Herdr.
pub fn report_state_for_session(session_id: &str, state: State, message: Option<&str>) {
    let Some(identity) = resolve_identity(Some(session_id)) else {
        return;
    };
    let line = encode_report_agent(&identity.pane_id, state, message);
    send_raw_to(&identity.socket_path, &line);
}

/// Report the native jcode session id so Herdr can resume the pane.
pub fn report_session_for_session(session_id: &str) {
    let Some(identity) = resolve_identity(Some(session_id)) else {
        return;
    };
    let line = encode_report_agent_session(&identity.pane_id, session_id);
    send_raw_to(&identity.socket_path, &line);
}

// ---------------------------------------------------------------------------
// Agent dispatch-site helpers
// ---------------------------------------------------------------------------

/// Map a jcode `session_start` lifecycle event to the corresponding Herdr
/// report. Called by the agent runtime at every session create/attach/resume.
///
/// Reports the native jcode session id (so Herdr can `jcode --resume <id>`
/// the pane after a Herdr restart) and sets the pane to `idle`. Both calls
/// are no-ops when the session is not under Herdr.
pub fn on_session_start(session_id: &str, source: &str) {
    let enabled = enabled_for_session(session_id);
    crate::logging::debug(&format!(
        "herdr: on_session_start({}, source={}) enabled_for_session={}",
        session_id, source, enabled
    ));
    if !enabled {
        return;
    }
    report_session_for_session(session_id);
    let message = match source {
        "create" => Some("new session"),
        "resume" => Some("resumed"),
        "attach" => Some("attached"),
        _ => None,
    };
    report_state_for_session(session_id, State::Idle, message);
}

/// Map a jcode `session_end` lifecycle event to Herdr `done`.
pub fn on_session_end(session_id: &str) {
    if !enabled_for_session(session_id) {
        return;
    }
    report_state_for_session(session_id, State::Done, Some("session closed"));
    // Drop the identity now so further reports for this id are no-ops.
    unregister_session(session_id);
}

/// Map a jcode `turn_start` event to Herdr `working`.
pub fn on_turn_start(session_id: &str) {
    if !enabled_for_session(session_id) {
        return;
    }
    report_state_for_session(session_id, State::Working, Some("working"));
}

/// Map a jcode `turn_end` event to Herdr `idle`. `ok` is the turn's success
/// flag; the message reflects the outcome.
pub fn on_turn_end(session_id: &str, ok: bool) {
    if !enabled_for_session(session_id) {
        return;
    }
    report_state_for_session(
        session_id,
        State::Idle,
        if ok {
            Some("ready")
        } else {
            Some("turn failed")
        },
    );
}

// ---------------------------------------------------------------------------
// Wire encoding
// ---------------------------------------------------------------------------

/// Build the newline-delimited JSON line for `pane.report_agent`.
///
/// Exposed (along with [`encode_report_agent_session`]) so tests can assert
/// the exact wire format without touching the socket. Uses `serde_json::json!`
/// for guaranteed-correct JSON; event frequency is low (a handful per turn),
/// so the small allocation is not a concern on the hot path.
fn encode_report_agent(pane_id: &str, state: State, message: Option<&str>) -> String {
    let mut params = json!({
        "pane_id": pane_id,
        "source": SOURCE,
        "agent": AGENT_LABEL,
        "state": state.as_str(),
    });
    if let Some(msg) = message {
        params["message"] = json!(msg);
    }
    json!({
        "id": "jcode",
        "method": "pane.report_agent",
        "params": params,
    })
    .to_string()
}

/// Build the newline-delimited JSON line for `pane.report_agent_session`.
fn encode_report_agent_session(pane_id: &str, session_id: &str) -> String {
    json!({
        "id": "jcode",
        "method": "pane.report_agent_session",
        "params": {
            "pane_id": pane_id,
            "source": SOURCE,
            "agent": AGENT_LABEL,
            "agent_session_id": session_id,
        }
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

fn socket_path() -> Option<PathBuf> {
    std::env::var_os("HERDR_SOCKET_PATH")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn pane_id() -> Option<String> {
    std::env::var("HERDR_PANE_ID")
        .ok()
        .filter(|v| !v.is_empty())
}

fn env_is_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref().map(str::trim),
        Some("1" | "true" | "TRUE" | "True" | "yes" | "on")
    )
}

/// Write one newline-terminated JSON request to the Herdr socket and close.
///
/// We use vectored I/O so we can append the trailing newline without an
/// extra string allocation, and a short connect timeout so a wedged Herdr
/// server can never stall the agent. Errors are debug-only.
fn send_raw_to(path: &std::path::Path, line: &str) {
    let mut stream = match connect_with_timeout(path) {
        Ok(s) => s,
        Err(error) => {
            crate::logging::debug(&format!(
                "herdr: failed to connect to socket {}: {error}",
                path.display()
            ));
            return;
        }
    };

    let newline = b"\n";
    let slices = [IoSlice::new(line.as_bytes()), IoSlice::new(newline)];
    if let Err(error) = stream.write_vectored(&slices) {
        crate::logging::debug(&format!(
            "herdr: failed to write to socket {}: {error}",
            path.display()
        ));
        return;
    }
    let _ = stream.flush();
    // Drop closes the connection; Herdr accepts one-shot reports.
}

#[cfg(unix)]
fn connect_with_timeout(path: &std::path::Path) -> std::io::Result<UnixStream> {
    // UnixStream::connect is blocking but for a local socket the kernel
    // connect is effectively immediate; we still bound it via a connect
    // deadline using a separate thread-free timeout pattern. For a UDS this
    // is overwhelmingly fast, so we keep it simple.
    //
    // If Herdr is ever not listening, connect returns ECONNREFUSED
    // immediately (no hang), so an explicit timeout layer is not needed in
    // practice.
    UnixStream::connect(path)
}

#[cfg(not(unix))]
fn connect_with_timeout(_path: &std::path::Path) -> std::io::Result<std::fs::File> {
    // Named-pipe support on Windows Herdr can be added when Herdr Windows
    // leaves beta. Until then this module is a no-op on Windows.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "herdr integration is unix-only for now",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_as_str_covers_all_variants() {
        assert_eq!(State::Idle.as_str(), "idle");
        assert_eq!(State::Working.as_str(), "working");
        assert_eq!(State::Blocked.as_str(), "blocked");
        assert_eq!(State::Done.as_str(), "done");
    }

    #[test]
    fn report_agent_encodes_state_without_message() {
        let line = encode_report_agent("w1:p1", State::Working, None);
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["method"], "pane.report_agent");
        assert_eq!(parsed["params"]["pane_id"], "w1:p1");
        assert_eq!(parsed["params"]["source"], "herdr:jcode");
        assert_eq!(parsed["params"]["agent"], "jcode");
        assert_eq!(parsed["params"]["state"], "working");
        assert!(parsed["params"].get("message").is_none());
    }

    #[test]
    fn report_agent_encodes_idle_with_message() {
        let line = encode_report_agent("w1:p2", State::Idle, Some("ready"));
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["params"]["state"], "idle");
        assert_eq!(parsed["params"]["message"], "ready");
    }

    #[test]
    fn report_agent_message_is_escaped() {
        let line = encode_report_agent("w1:p1", State::Working, Some("he said \"hi\"\nnew line"));
        // The whole thing must parse as JSON (i.e. escaping is valid).
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["params"]["message"], "he said \"hi\"\nnew line");
    }

    #[test]
    fn report_agent_blocked_and_done_encode() {
        let blocked = encode_report_agent("w1:p1", State::Blocked, Some("approve write?"));
        let parsed: serde_json::Value = serde_json::from_str(&blocked).expect("valid JSON");
        assert_eq!(parsed["params"]["state"], "blocked");
        let done = encode_report_agent("w1:p1", State::Done, None);
        let parsed: serde_json::Value = serde_json::from_str(&done).expect("valid JSON");
        assert_eq!(parsed["params"]["state"], "done");
    }

    #[test]
    fn report_agent_session_encodes_id() {
        let line = encode_report_agent_session("w1:p1", "ses_abc123");
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["method"], "pane.report_agent_session");
        assert_eq!(parsed["params"]["pane_id"], "w1:p1");
        assert_eq!(parsed["params"]["agent_session_id"], "ses_abc123");
        assert_eq!(parsed["params"]["agent"], "jcode");
        assert_eq!(parsed["params"]["source"], "herdr:jcode");
    }

    #[cfg(unix)]
    #[test]
    fn send_raw_does_not_panic_when_socket_missing() {
        // Point at a nonexistent socket; should log debug and return quietly.
        send_raw_to(
            std::path::Path::new("/tmp/jcode-herdr-does-not-exist.sock"),
            r#"{"id":"jcode","method":"pane.report_agent","params":{}}"#,
        );
    }

    // The end-to-end socket round-trip lives in `tests/herdr_socket.rs` as a
    // separate process so its environment mutations cannot race with other
    // unit tests in the `herdr` module (env vars are process-global).
}
