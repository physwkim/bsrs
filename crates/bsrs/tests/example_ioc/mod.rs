//! Boots an epics-rs example IOC for the `#[ignore]`d tests that run against
//! one. `BSRS_EPICS_RS` names an epics-rs checkout with the examples built:
//!
//! ```text
//! cargo build --release -p ophyd-test-ioc -p mini-beamline -p sim-detector -p qsrv-ioc \
//!     --features ophyd-test-ioc/ioc,mini-beamline/ioc,sim-detector/ioc,qsrv-ioc/ioc
//! BSRS_EPICS_RS=<checkout> cargo nextest run -p bsrs --run-ignored only
//! ```
//!
//! The CA and PVA clients read their environment once per process, so a
//! process boots one IOC, on the first [`boot`], and points both clients at
//! it. Under `cargo test` that IOC is shared by every test in the binary.
//! The IOC exits with this process: its iocsh ends on stdin EOF.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, OnceLock};
use std::time::Duration;

struct Booted {
    bin: String,
    // Holds the IOC's stdin open; the IOC ends when it closes.
    _ioc: Child,
}

static BOOTED: OnceLock<Booted> = OnceLock::new();

const READY: &str = "BSRS_EXAMPLE_IOC_READY";
const BOOT_TIMEOUT: Duration = Duration::from_secs(120);
const ATTEMPTS: usize = 16;
// Room for a full detector frame over CA; the C default is 16 kB.
const MAX_ARRAY_BYTES: &str = "16000000";

/// Boot `bin` from `examples/<example>` once per process and point the CA
/// and PVA clients at it. Call it before any client exists.
pub fn boot(bin: &str, example: &str) {
    let booted = BOOTED.get_or_init(|| Booted {
        bin: bin.to_string(),
        _ioc: start(bin, example),
    });
    assert_eq!(
        booted.bin, bin,
        "one example IOC per test binary: the clients read their environment once"
    );
}

fn start(bin: &str, example: &str) -> Child {
    for _ in 0..ATTEMPTS {
        let (ca, pva) = free_ports();
        if let Some(ioc) = try_start(bin, example, ca, pva) {
            // SAFETY: every test in a binary that boots an IOC calls `boot`
            // first, and `get_or_init` holds the others until this returns.
            unsafe {
                std::env::set_var("EPICS_CA_NAME_SERVERS", format!("127.0.0.1:{ca}"));
                std::env::set_var("EPICS_CA_ADDR_LIST", "");
                std::env::set_var("EPICS_CA_AUTO_ADDR_LIST", "NO");
                std::env::set_var("EPICS_CA_MAX_ARRAY_BYTES", MAX_ARRAY_BYTES);
                std::env::set_var("EPICS_PVA_NAME_SERVERS", format!("127.0.0.1:{pva}"));
                std::env::set_var("EPICS_PVA_ADDR_LIST", "");
                std::env::set_var("EPICS_PVA_AUTO_ADDR_LIST", "NO");
            }
            return ioc;
        }
    }
    panic!("{bin} lost its port {ATTEMPTS} times in a row");
}

/// A CA port free on TCP and UDP and a distinct PVA port free on TCP. A hint
/// only: the IOC's bind is the verdict.
fn free_ports() -> (u16, u16) {
    loop {
        let ca = TcpListener::bind("0.0.0.0:0").expect("an ephemeral port");
        let ca_port = ca.local_addr().expect("bound").port();
        if UdpSocket::bind(("0.0.0.0", ca_port)).is_err() {
            continue;
        }
        let pva = TcpListener::bind("0.0.0.0:0").expect("an ephemeral port");
        return (ca_port, pva.local_addr().expect("bound").port());
    }
}

/// Boot `bin` on CA port `ca` and PVA port `pva`. `None` only when the IOC
/// reports that one of the two was taken; it is then gone.
pub fn try_start(bin: &str, example: &str, ca: u16, pva: u16) -> Option<Child> {
    let root = PathBuf::from(
        std::env::var_os("BSRS_EPICS_RS")
            .expect("BSRS_EPICS_RS names an epics-rs checkout with the example IOCs built"),
    );
    let exe = root.join("target/release").join(bin);
    let (reader, writer) = std::io::pipe().expect("a pipe");
    let mut child = Command::new(&exe)
        .arg("ioc/st.cmd")
        .current_dir(root.join("examples").join(example))
        .env("EPICS_CAS_SERVER_PORT", ca.to_string())
        .env("EPICS_PVAS_SERVER_PORT", pva.to_string())
        .env("EPICS_CAS_BEACON_ADDR_LIST", "127.0.0.1:1")
        .env("EPICS_CAS_AUTO_BEACON_ADDR_LIST", "NO")
        .env("EPICS_CA_MAX_ARRAY_BYTES", MAX_ARRAY_BYTES)
        .stdin(Stdio::piped())
        // One pipe, so a port warning is read before the marker it precedes.
        .stdout(writer.try_clone().expect("a pipe"))
        .stderr(writer)
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", exe.display()));
    // The shell reads this only once both servers are bound.
    let stdin = child.stdin.as_mut().expect("piped stdin");
    writeln!(stdin, "echo {READY}").expect("write to the IOC's stdin");

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut log = String::new();
        let mut lines = BufReader::new(reader).lines();
        for line in lines.by_ref() {
            let Ok(line) = line else { break };
            log.push_str(&line);
            log.push('\n');
            if line.ends_with(READY) && !line.contains("echo") {
                let _ = tx.send((true, std::mem::take(&mut log)));
                // Drained to the end: a full pipe would stall the IOC.
                lines.for_each(drop);
                return;
            }
        }
        let _ = tx.send((false, log));
    });

    let (ready, log) = match rx.recv_timeout(BOOT_TIMEOUT) {
        Ok(verdict) => verdict,
        Err(_) => {
            let _ = child.kill();
            panic!("{bin} did not reach its shell in {BOOT_TIMEOUT:?}");
        }
    };
    let ca_udp_taken = log.contains(&format!("UDP search responder bind 0.0.0.0:{ca}\""))
        && log.contains("AddrInUse");
    let ca_tcp_taken = log.contains("Configured TCP port was unavailable");
    let pva_taken = log.contains(&format!("unable to bind port {pva},"));
    if ready && !ca_tcp_taken && !pva_taken {
        return Some(child);
    }
    let _ = child.kill();
    let status = child.wait().expect("reap the IOC");
    if ca_udp_taken || ca_tcp_taken || pva_taken {
        return None;
    }
    panic!("{bin} exited before its shell ({status}):\n{log}");
}
