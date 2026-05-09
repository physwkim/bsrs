//! `cirrus doctor` — env validation.
//!
//! Quick checks an operator can run before starting a beamline session
//! to catch the common "why won't anything work" causes:
//!
//! - cirrus runtime spins up
//! - `EPICS_CA_ADDR_LIST` / `EPICS_CA_AUTO_ADDR_LIST` are sane
//! - optional Tiled URL responds (when --tiled-url is supplied)
//! - optional Kafka broker accepts a TCP connection (when --kafka is
//!   supplied)
//!
//! Each check prints a single line:
//!
//! ```text
//! [ ok ]   tokio runtime
//! [ ok ]   EPICS_CA_ADDR_LIST = 10.0.0.255:5064
//! [warn]   EPICS_CA_AUTO_ADDR_LIST = NO  (not auto-detecting interfaces)
//! [fail]   tiled http://localhost:8000  (connection refused)
//! ```
//!
//! Exit code 0 if every check is `ok`, 1 if any `fail` (warnings ok).

use clap::Args;
use std::time::Duration;

/// CLI arguments for `cirrus doctor`.
#[derive(Args, Debug)]
pub struct DoctorArgs {
    /// Optional Tiled URL to ping (HTTP GET / endpoint).
    #[arg(long)]
    pub tiled_url: Option<String>,

    /// Optional Kafka broker `host:port` to TCP-probe.
    #[arg(long)]
    pub kafka: Option<String>,

    /// Connection timeout for HTTP / TCP probes.
    #[arg(long, default_value_t = 2)]
    pub timeout_secs: u64,
}

#[derive(Copy, Clone, Debug)]
enum Verdict {
    Ok,
    Warn,
    Fail,
}

fn print_line(v: Verdict, label: &str, detail: &str) {
    let tag = match v {
        Verdict::Ok => "[ ok ]",
        Verdict::Warn => "[warn]",
        Verdict::Fail => "[fail]",
    };
    println!("{tag}   {label} {detail}");
}

/// Entry point. Returns process exit code.
pub fn run(args: DoctorArgs) -> i32 {
    let timeout = Duration::from_secs(args.timeout_secs.max(1));
    let mut any_fail = false;

    // 1. tokio runtime starts.
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => {
            print_line(Verdict::Ok, "tokio runtime", "(multi-thread)");
            rt
        }
        Err(e) => {
            print_line(Verdict::Fail, "tokio runtime", &format!("({e})"));
            return 1;
        }
    };

    // 2. EPICS env vars.
    match std::env::var("EPICS_CA_ADDR_LIST") {
        Ok(v) if !v.trim().is_empty() => {
            print_line(Verdict::Ok, "EPICS_CA_ADDR_LIST", &format!("= {v}"));
        }
        _ => {
            print_line(
                Verdict::Warn,
                "EPICS_CA_ADDR_LIST",
                "= <unset>  (only AUTO_ADDR_LIST will be used)",
            );
        }
    }
    match std::env::var("EPICS_CA_AUTO_ADDR_LIST") {
        Ok(v) if v.eq_ignore_ascii_case("NO") => {
            print_line(
                Verdict::Warn,
                "EPICS_CA_AUTO_ADDR_LIST",
                "= NO  (not auto-detecting interfaces)",
            );
        }
        Ok(v) => {
            print_line(Verdict::Ok, "EPICS_CA_AUTO_ADDR_LIST", &format!("= {v}"));
        }
        Err(_) => {
            print_line(
                Verdict::Ok,
                "EPICS_CA_AUTO_ADDR_LIST",
                "= <unset>  (defaults to YES)",
            );
        }
    }

    // 3. Optional Tiled probe.
    if let Some(url) = args.tiled_url.as_deref() {
        let probe = format!("{}/api/v1/", url.trim_end_matches('/'));
        let r = rt.block_on(async {
            tokio::time::timeout(timeout, async { reqwest_get_status(&probe).await }).await
        });
        match r {
            Ok(Ok(code)) if code < 500 => {
                print_line(Verdict::Ok, "tiled", &format!("{probe}  → HTTP {code}"));
            }
            Ok(Ok(code)) => {
                print_line(Verdict::Warn, "tiled", &format!("{probe}  → HTTP {code}"));
            }
            Ok(Err(e)) => {
                print_line(Verdict::Fail, "tiled", &format!("{probe}  → {e}"));
                any_fail = true;
            }
            Err(_) => {
                print_line(
                    Verdict::Fail,
                    "tiled",
                    &format!("{probe}  → timeout after {:?}", timeout),
                );
                any_fail = true;
            }
        }
    }

    // 4. Optional Kafka probe (TCP-only).
    if let Some(addr) = args.kafka.as_deref() {
        let r = rt.block_on(async {
            tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await
        });
        match r {
            Ok(Ok(_)) => {
                print_line(Verdict::Ok, "kafka tcp", &format!("{addr}  (open)"));
            }
            Ok(Err(e)) => {
                print_line(Verdict::Fail, "kafka tcp", &format!("{addr}  → {e}"));
                any_fail = true;
            }
            Err(_) => {
                print_line(
                    Verdict::Fail,
                    "kafka tcp",
                    &format!("{addr}  → timeout after {:?}", timeout),
                );
                any_fail = true;
            }
        }
    }

    if any_fail {
        1
    } else {
        0
    }
}

/// Tiny inline HTTP GET that returns the status code as `u16`. Avoids
/// dragging reqwest into the unconditional dep set — uses `tokio::net`
/// + a raw HTTP/1.1 request.
async fn reqwest_get_status(url: &str) -> Result<u16, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Tiny inline parser: only http:// is supported (https would
    // require dragging in rustls). `host:port/path` is enough to
    // form a valid HTTP/1.1 request.
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "doctor only probes http:// (https unsupported)".to_string())?;
    let (host_port, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port): (String, u16) = match host_port.rfind(':') {
        Some(i) => {
            let h = &host_port[..i];
            let p: u16 = host_port[i + 1..]
                .parse()
                .map_err(|e| format!("bad port: {e}"))?;
            (h.to_string(), p)
        }
        None => (host_port.to_string(), 80),
    };
    let mut s = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: cirrus-doctor/0.1\r\n\r\n",
        path, host
    );
    s.write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = [0u8; 16];
    let n = s.read(&mut buf).await.map_err(|e| format!("read: {e}"))?;
    let line = std::str::from_utf8(&buf[..n]).map_err(|_| "non-utf8 status line".to_string())?;
    // Expect "HTTP/1.1 NNN ...".
    let mut parts = line.split_whitespace();
    parts.next();
    let code: u16 = parts
        .next()
        .ok_or_else(|| format!("malformed status line: {line:?}"))?
        .parse()
        .map_err(|e| format!("bad code: {e}"))?;
    Ok(code)
}
