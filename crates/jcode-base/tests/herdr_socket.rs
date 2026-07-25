//! End-to-end socket round trip for the Herdr integration.
//!
//! Lives in `tests/` (separate process) so its `HERDR_*` env mutations
//! cannot race with other `herdr::*` unit tests in the lib, since env vars
//! are process-global and Rust's default test harness runs unit tests in
//! parallel threads within one process.

#![cfg(unix)]

use std::io::Read;
use std::os::unix::net::UnixListener;
use std::sync::Mutex;

use jcode_base::herdr;
use jcode_base::safety::{PermissionRequest, SafetySystem, Urgency};

/// All tests in this file mutate the same `HERDR_*` process env vars and
/// open real Unix sockets, so they must run strictly one at a time. Cargo's
/// default test harness parallelizes tests within a binary across threads,
/// which would let two tests race on `HERDR_SOCKET_PATH` and cause the
/// accept side of one to block forever on the wrong socket. Holding this
/// guard for the whole test body serializes them.
static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn end_to_end_report_writes_to_real_socket() {
    let _guard = ENV_LOCK.lock().unwrap();
    let dir = tempfile::TempDir::new().expect("temp dir");
    let sock = dir.path().join("herdr.sock");
    let listener = UnixListener::bind(&sock).expect("bind");

    jcode_base::env::set_var("HERDR_ENV", "1");
    jcode_base::env::set_var("HERDR_SOCKET_PATH", &sock);
    jcode_base::env::set_var("HERDR_PANE_ID", "w1:p1");

    // Spawn the accept side; we expect exactly one connection with one
    // newline-terminated JSON request.
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).expect("read");
        String::from_utf8(buf).expect("utf8")
    });

    // Drive the public API the same way the agent runtime does at turn start.
    herdr::on_turn_start("ses_e2e_1");

    let received = handle.join().expect("accept thread panicked");

    jcode_base::env::remove_var("HERDR_ENV");
    jcode_base::env::remove_var("HERDR_SOCKET_PATH");
    jcode_base::env::remove_var("HERDR_PANE_ID");

    let line = received.trim_end_matches('\n');
    let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSON on socket");
    assert_eq!(parsed["method"], "pane.report_agent");
    assert_eq!(parsed["params"]["pane_id"], "w1:p1");
    assert_eq!(parsed["params"]["source"], "herdr:jcode");
    assert_eq!(parsed["params"]["agent"], "jcode");
    assert_eq!(parsed["params"]["state"], "working");
    assert_eq!(parsed["params"]["message"], "working");
    assert!(received.ends_with('\n'), "request is newline-terminated");
}

#[test]
fn end_to_end_reports_native_session_id_for_restore() {
    let _guard = ENV_LOCK.lock().unwrap();
    let dir = tempfile::TempDir::new().expect("temp dir");
    let sock = dir.path().join("herdr.sock");
    let listener = UnixListener::bind(&sock).expect("bind");

    jcode_base::env::set_var("HERDR_ENV", "1");
    jcode_base::env::set_var("HERDR_SOCKET_PATH", &sock);
    jcode_base::env::set_var("HERDR_PANE_ID", "w2:p3");

    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).expect("read");
        String::from_utf8(buf).expect("utf8")
    });

    // `on_session_start` emits two reports: the session-id report first
    // (so Herdr can resume the pane) and then an `idle` state report.
    herdr::on_session_start("ses_xyz789", "create");

    let received = handle.join().expect("accept thread panicked");

    jcode_base::env::remove_var("HERDR_ENV");
    jcode_base::env::remove_var("HERDR_SOCKET_PATH");
    jcode_base::env::remove_var("HERDR_PANE_ID");

    let line = received.trim_end_matches('\n');
    let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSON on socket");
    // The first write should be the session-id report.
    assert_eq!(parsed["method"], "pane.report_agent_session");
    assert_eq!(parsed["params"]["pane_id"], "w2:p3");
    assert_eq!(parsed["params"]["agent_session_id"], "ses_xyz789");
    assert_eq!(parsed["params"]["agent"], "jcode");
}

#[test]
fn permission_request_reports_blocked_and_resolution_reports_idle() {
    let _guard = ENV_LOCK.lock().unwrap();
    let dir = tempfile::TempDir::new().expect("temp dir");
    let sock = dir.path().join("herdr.sock");
    let listener = UnixListener::bind(&sock).expect("bind");

    jcode_base::env::set_var("HERDR_ENV", "1");
    jcode_base::env::set_var("HERDR_SOCKET_PATH", &sock);
    jcode_base::env::set_var("HERDR_PANE_ID", "w3:p9");

    // The safety system routes every permission request through
    // dispatch_permission_notification, which we wired to also report
    // `blocked` to Herdr.
    let system = SafetySystem::new();
    let request = PermissionRequest {
        id: jcode_base::safety::new_request_id(),
        action: "create_pull_request".to_string(),
        description: "open PR for branch feat/x".to_string(),
        rationale: "PR is ready to merge after CI".to_string(),
        urgency: Urgency::Normal,
        wait: true,
        created_at: chrono::Utc::now(),
        context: None,
        session_id: Some("ses_e2e_blocked".to_string()),
    };

    // Collect every Herdr socket write made during this test. Each write is
    // one connection+newline; we accept them all on a single thread.
    let request_id = request.id.clone();
    let listener_handle = std::thread::spawn(move || {
        let mut frames = Vec::new();
        // We expect at least 2 frames: blocked (on request) + idle (on
        // resolve). Accept up to 4 to be robust to spurious extra writes,
        // with a short accept timeout so the test cannot hang forever if
        // production code stops writing earlier than expected.
        listener
            .set_nonblocking(true)
            .expect("set_nonblocking on listener");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = Vec::new();
                    if stream.read_to_end(&mut buf).is_ok() {
                        if let Ok(s) = String::from_utf8(buf) {
                            frames.push(s);
                        }
                    }
                    if frames.len() >= 4 {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(_) => break,
            }
        }
        frames
    });

    // 1) Submit the request -> should emit a `blocked` report.
    // Register a Herdr identity for the session first, the way the server
    // does when a client subscribes with terminal_env.
    herdr::register_session(
        "ses_e2e_blocked",
        &[
            ("HERDR_ENV".to_string(), "1".to_string()),
            (
                "HERDR_SOCKET_PATH".to_string(),
                sock.to_string_lossy().into_owned(),
            ),
            ("HERDR_PANE_ID".to_string(), "w3:p9".to_string()),
        ],
    );
    let _ = system.request_permission(request);
    // 2) Resolve it -> should emit an `idle` report (queue now empty).
    system
        .record_decision_for_session(&request_id, true, "test", None, Some("ses_e2e_blocked"))
        .expect("record_decision_for_session");

    let frames = listener_handle.join().expect("listener thread panicked");

    jcode_base::env::remove_var("HERDR_ENV");
    jcode_base::env::remove_var("HERDR_SOCKET_PATH");
    jcode_base::env::remove_var("HERDR_PANE_ID");

    assert!(
        !frames.is_empty(),
        "expected at least one Herdr socket write from the permission flow"
    );

    let states: Vec<String> = frames
        .iter()
        .filter_map(|raw| {
            let line = raw.trim_end_matches('\n');
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| {
                    if v["method"] == "pane.report_agent" {
                        v["params"]["state"].as_str().map(|s| s.to_string())
                    } else {
                        None
                    }
                })
        })
        .collect();

    assert!(
        states.iter().any(|s| s == "blocked"),
        "expected a `blocked` report after request_permission; got states={:?}",
        states
    );
    assert!(
        states.iter().any(|s| s == "idle"),
        "expected an `idle` report after resolving the request; got states={:?}",
        states
    );
}
