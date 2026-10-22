#![cfg(feature = "net-sync")]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as JsonValue};

// Both tests bind real ports; serialising them avoids free-port races.
static SERIAL: Mutex<()> = Mutex::new(());

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("hakomcp-sync-{tag}-{nanos}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr").port()
}

struct Client {
    child: Child,
    stdin: ChildStdin,
    responses: Receiver<JsonValue>,
}

impl Client {
    fn spawn(args: &[String], env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_hakomcp"));
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn hakomcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let (tx, responses) = channel();
        thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            let mut line = String::new();
            while stdout.read_line(&mut line).unwrap_or(0) > 0 {
                if let Ok(value) = serde_json::from_str(&line) {
                    let _ = tx.send(value);
                }
                line.clear();
            }
        });
        Self {
            child,
            stdin,
            responses,
        }
    }

    fn send(&mut self, value: JsonValue) {
        let mut line = serde_json::to_string(&value).expect("serialize");
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).expect("write");
        self.stdin.flush().expect("flush");
    }

    fn read_response(&mut self, id: u64, timeout: Duration) -> JsonValue {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let value = self
                .responses
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for response {id}"));
            if value["id"].as_u64() == Some(id) {
                return value;
            }
        }
    }

    fn initialize(&mut self) {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "sync-test", "version": "1" }
            }
        }));
        let init = self.read_response(1, Duration::from_secs(10));
        assert!(init["result"].is_object(), "{init}");
    }

    fn call(&mut self, id: u64, name: &str, arguments: JsonValue) -> JsonValue {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments }
        }));
        let response = self.read_response(id, Duration::from_secs(30));
        assert!(
            response["result"].get("isError").is_none(),
            "{name} failed: {response}"
        );
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("missing text for {name}: {response}"));
        serde_json::from_str(text).unwrap_or_else(|_| panic!("invalid json for {name}: {text}"))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(200));
    }
}

fn spawn_node(dir: &std::path::Path, env: &[(&str, &str)]) -> Client {
    let args = vec![
        "--db".to_string(),
        dir.to_string_lossy().to_string(),
        "--profile".to_string(),
        "sync".to_string(),
        "--allow-network".to_string(),
    ];
    Client::spawn(&args, env)
}

#[test]
fn mesh_sync_converges_writes_and_deletes() {
    let _serial = SERIAL.lock().unwrap();
    let dir_a = temp_dir("mesh-a");
    let dir_b = temp_dir("mesh-b");
    let port_a = free_port();
    let port_b = free_port();
    let secret = "mesh-test-room-key";

    let mut a = spawn_node(&dir_a, &[("HAKOMCP_SYNC_ROOM_KEY", secret)]);
    let mut b = spawn_node(&dir_b, &[("HAKOMCP_SYNC_ROOM_KEY", secret)]);
    a.initialize();
    b.initialize();

    a.call(
        2,
        "sync_start",
        json!({
            "mode": "mesh",
            "node_id": "node-a",
            "room": "mesh-room",
            "port": port_a,
            "static_peers": [format!("127.0.0.1:{port_b}")],
            "discovery": "off"
        }),
    );
    b.call(
        2,
        "sync_start",
        json!({
            "mode": "mesh",
            "node_id": "node-b",
            "room": "mesh-room",
            "port": port_b,
            "static_peers": [],
            "discovery": "off"
        }),
    );

    let mut next_id = 10;
    wait_for("mesh peers", || {
        next_id += 1;
        let peers = a.call(next_id, "sync_peers", json!({}));
        (peers["status"]["peer_count"].as_u64().unwrap_or(0) >= 1).then_some(())
    });

    a.call(
        3,
        "document_put",
        json!({
            "collection": "notes",
            "id": "n1",
            "doc": { "title": "from mesh a" }
        }),
    );
    wait_for("replicated write", || {
        next_id += 1;
        let got = b.call(
            next_id,
            "document_get",
            json!({ "collection": "notes", "id": "n1" }),
        );
        (got["found"] == true).then_some(())
    });

    a.call(
        4,
        "document_delete",
        json!({ "collection": "notes", "id": "n1" }),
    );
    wait_for("replicated delete", || {
        next_id += 1;
        let got = b.call(
            next_id,
            "document_get",
            json!({ "collection": "notes", "id": "n1" }),
        );
        (got["found"] == false).then_some(())
    });

    let status = a.call(5, "sync_status", json!({}));
    assert_eq!(status["active"], true);
    assert_eq!(status["mode"], "mesh");
    assert!(status.get("room_key").is_none(), "{status}");
    assert!(!status.to_string().contains(secret));

    a.call(6, "sync_stop", json!({}));
    b.call(6, "sync_stop", json!({}));

    let _ = std::fs::remove_dir_all(dir_a);
    let _ = std::fs::remove_dir_all(dir_b);
}

#[cfg(feature = "cloud-sync")]
#[test]
fn cloud_sync_converges_writes_and_redacts_secrets() {
    let _serial = SERIAL.lock().unwrap();
    let dir_a = temp_dir("cloud-server");
    let dir_b = temp_dir("cloud-client");
    let port = free_port();
    let secret = "cloud-test-room-key";
    let token = "cloud-test-auth-token";
    let env = [
        ("HAKOMCP_SYNC_ROOM_KEY", secret),
        ("HAKOMCP_CLOUD_AUTH_TOKEN", token),
    ];

    let mut server = spawn_node(&dir_a, &env);
    let mut client = spawn_node(&dir_b, &env);
    server.initialize();
    client.initialize();

    server.call(
        2,
        "sync_start",
        json!({
            "mode": "cloud_server",
            "node_id": "server",
            "room": "cloud-room",
            "bind": format!("127.0.0.1:{port}"),
            "max_clients": 8
        }),
    );
    thread::sleep(Duration::from_millis(500));
    client.call(
        2,
        "sync_start",
        json!({
            "mode": "cloud_client",
            "node_id": "client",
            "room": "cloud-room",
            "server_url": format!("ws://127.0.0.1:{port}")
        }),
    );

    let mut next_id = 10;
    wait_for("cloud connection", || {
        next_id += 1;
        let status = server.call(next_id, "sync_status", json!({}));
        (status["active_clients"].as_u64().unwrap_or(0) >= 1).then_some(())
    });

    server.call(
        3,
        "document_put",
        json!({
            "collection": "notes",
            "id": "n1",
            "doc": { "title": "from cloud server" }
        }),
    );
    wait_for("cloud replicated write", || {
        next_id += 1;
        let got = client.call(
            next_id,
            "document_get",
            json!({ "collection": "notes", "id": "n1" }),
        );
        (got["found"] == true).then_some(())
    });

    server.call(
        4,
        "document_delete",
        json!({ "collection": "notes", "id": "n1" }),
    );
    wait_for("cloud replicated delete", || {
        next_id += 1;
        let got = client.call(
            next_id,
            "document_get",
            json!({ "collection": "notes", "id": "n1" }),
        );
        (got["found"] == false).then_some(())
    });

    for node in [&mut server, &mut client] {
        next_id += 1;
        let status = node.call(next_id, "sync_status", json!({}));
        assert_eq!(status["active"], true);
        assert!(status.get("room_key").is_none(), "{status}");
        let text = status.to_string();
        assert!(!text.contains(secret), "{text}");
        assert!(!text.contains(token), "{text}");
    }

    server.call(5, "sync_stop", json!({}));
    client.call(5, "sync_stop", json!({}));

    let _ = std::fs::remove_dir_all(dir_a);
    let _ = std::fs::remove_dir_all(dir_b);
}
