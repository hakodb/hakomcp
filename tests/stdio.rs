use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

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

    fn close(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
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
    assert!(names.contains(&"get"));
    assert!(!names.contains(&"put"));
    assert!(!names.contains(&"create_index"));

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
