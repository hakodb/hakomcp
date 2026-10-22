use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as JsonValue};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("hakomcp-stdio-{tag}-{nanos}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Client {
    fn spawn(args: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_hakomcp"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn hakomcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, value: JsonValue) {
        let mut line = serde_json::to_string(&value).expect("serialize");
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).expect("write");
        self.stdin.flush().expect("flush");
    }

    fn read(&mut self) -> JsonValue {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read");
        assert!(!line.is_empty(), "hakomcp closed stdout");
        serde_json::from_str(&line).expect("parse response")
    }

    fn read_response(&mut self, id: u64) -> JsonValue {
        loop {
            let value = self.read();
            if value["id"].as_u64() == Some(id) {
                return value;
            }
        }
    }

    fn close(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn call(&mut self, id: u64, name: &str, arguments: JsonValue) -> JsonValue {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments }
        }));
        self.read_response(id)
    }

    fn initialize(&mut self) {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "stdio-test", "version": "1" }
            }
        }));
        assert!(self.read_response(1)["result"].is_object());
    }
}

#[test]
fn stdio_handshake_and_read_profile() {
    let dir = temp_dir("read");
    let db = dir.to_string_lossy().to_string();
    let mut client = Client::spawn(&["--db", &db, "--profile", "read"]);

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "stdio-test", "version": "1" }
        }
    }));
    let init = client.read();
    assert_eq!(init["result"]["serverInfo"]["name"], "hakomcp");
    assert!(init["result"]["instructions"].is_string());

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    }));
    let tools = client.read();
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["name"].as_str().expect("name"))
        .collect();
    assert!(names.contains(&"document_get"));
    assert!(names.contains(&"collection_list"));
    assert!(!names.contains(&"document_put"));
    assert!(!names.contains(&"index_create"));

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "put",
            "arguments": { "collection": "notes", "doc": { "title": "nope" } }
        }
    }));
    let call = client.read();
    assert_eq!(call["error"]["code"].as_i64(), Some(-32602));

    client.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stdio_subscription_notification_and_job() {
    let dir = temp_dir("subscribe");
    let db = dir.to_string_lossy().to_string();
    let mut client = Client::spawn(&[
        "--db",
        &db,
        "--profile",
        "admin",
        "--allow-destructive",
        "--subscribe-to",
        "control",
    ]);

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "stdio-test", "version": "1" }
        }
    }));
    let init = client.read_response(1);
    assert_eq!(
        init["result"]["capabilities"]["resources"]["subscribe"],
        true
    );

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "resources/subscribe",
        "params": { "uri": "hakodb://collections/control" }
    }));
    assert!(client.read_response(2)["result"].is_object());

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "document_put",
            "arguments": { "collection": "control", "id": "c1", "doc": { "enabled": true } }
        }
    }));

    let mut saw_put = false;
    let mut saw_notification = false;
    for _ in 0..8 {
        let value = client.read();
        if value["id"].as_u64() == Some(3) {
            saw_put = true;
        }
        if value["method"] == "notifications/resources/updated"
            && value["params"]["uri"] == "hakodb://collections/control"
        {
            saw_notification = true;
        }
        if saw_put && saw_notification {
            break;
        }
    }
    assert!(saw_put, "missing put response");
    assert!(saw_notification, "missing subscription notification");

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": { "name": "compact", "arguments": {} }
    }));
    let compact = client.read_response(4);
    let compact_text = compact["result"]["content"][0]["text"]
        .as_str()
        .expect("compact result");
    let compact: JsonValue = serde_json::from_str(compact_text).expect("compact json");
    let job_id = compact["job_id"].as_str().expect("job id").to_string();

    let mut terminal = None;
    for _ in 0..40 {
        client.send(json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": { "name": "job_get", "arguments": { "id": job_id } }
        }));
        let job = client.read_response(5);
        let text = job["result"]["content"][0]["text"]
            .as_str()
            .expect("job result");
        let job: JsonValue = serde_json::from_str(text).expect("job json");
        let state = job["state"].as_str().unwrap_or("pending");
        if state == "succeeded" || state == "failed" {
            terminal = Some(state.to_string());
            break;
        }
        sleep(Duration::from_millis(50));
    }
    assert_eq!(terminal.as_deref(), Some("succeeded"));

    client.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stdio_subscriptions_are_disabled_without_allowlist() {
    let dir = temp_dir("no-subscribe");
    let db = dir.to_string_lossy().to_string();
    let mut client = Client::spawn(&["--db", &db]);

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "stdio-test", "version": "1" }
        }
    }));
    let init = client.read_response(1);
    assert_eq!(
        init["result"]["capabilities"]["resources"]["subscribe"],
        false
    );

    client.send(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "resources/subscribe",
        "params": { "uri": "hakodb://collections/control" }
    }));
    let subscribe = client.read_response(2);
    assert_eq!(subscribe["error"]["code"].as_i64(), Some(-32602));

    client.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn restart_recovers_documents_after_a_hard_kill() {
    let dir = temp_dir("restart");
    let db = dir.to_string_lossy().to_string();

    let mut first = Client::spawn(&["--db", &db]);
    first.initialize();
    let put = first.call(
        2,
        "document_put",
        json!({ "collection": "notes", "id": "n1", "doc": { "title": "durable" } }),
    );
    assert!(put["result"].get("isError").is_none(), "{put}");
    first.kill();

    let mut second = Client::spawn(&["--db", &db]);
    second.initialize();
    let get = second.call(
        2,
        "document_get",
        json!({ "collection": "notes", "id": "n1" }),
    );
    let text = get["result"]["content"][0]["text"].as_str().unwrap();
    let value: JsonValue = serde_json::from_str(text).unwrap();
    assert_eq!(value["document"]["title"], "durable");

    second.close();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn large_results_are_capped_and_raiseable() {
    let dir = temp_dir("large");
    let db = dir.to_string_lossy().to_string();
    let payload = "x".repeat(1_500_000);

    let mut capped = Client::spawn(&["--db", &db]);
    capped.initialize();
    let put = capped.call(
        2,
        "document_put",
        json!({ "collection": "notes", "id": "big", "doc": { "body": payload } }),
    );
    assert!(put["result"].get("isError").is_none(), "{put}");
    let get = capped.call(
        3,
        "document_get",
        json!({ "collection": "notes", "id": "big" }),
    );
    assert_eq!(get["result"]["isError"], true, "{get}");
    capped.close();

    let mut raised = Client::spawn(&["--db", &db, "--max-result-bytes", "2097152"]);
    raised.initialize();
    let get = raised.call(
        2,
        "document_get",
        json!({ "collection": "notes", "id": "big" }),
    );
    assert!(get["result"].get("isError").is_none(), "{get}");

    raised.close();
    let _ = std::fs::remove_dir_all(dir);
}
