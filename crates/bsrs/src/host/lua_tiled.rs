//! Optional `tiled.*` Lua surface — exposes read-side access to a
//! Tiled HTTP catalog from bsrs REPL plans. Behind the `tiled`
//! Cargo feature on bsrs-cli.
//!
//! Backed by the `tiled-rs` crate (https://github.com/physwkim/tiled-rs).
//!
//! ## Lua API
//!
//! ```lua
//! local cat = tiled.from_uri("http://localhost:8000")        -- root container
//! for _, k in ipairs(cat:keys()) do print(k) end             -- list runs
//! local run = cat:get("scan_42")                             -- fetch a child
//! print(run:metadata())                                      -- JSON string
//! local n = cat:len()                                        -- size
//! ```
//!
//! Returned userdata is a generic `LuaTiledNode` that can wrap any
//! Tiled child (container, array, table, ...). The user calls
//! `:keys()` only on container nodes; non-containers return an
//! empty list.
//!
//! ## Threading
//!
//! All `tiled-rs` client calls are async; the bindings drive them via
//! `bsrs_runtime().block_on(...)` from the REPL thread (same
//! pattern as `RE:run`). Safe with mlua's reentrant lock since the
//! REPL thread re-enters its own mutex.

#![cfg(feature = "tiled")]

use std::sync::Arc;

use mlua::{Lua, UserData, UserDataMethods};
use tiled_rs::client::{from_uri_with_options, AnyClient, ContextOptions};
use tokio::sync::Mutex as TMutex;
use url::Url;

/// Wraps a `tiled_rs::client::AnyClient` for use as Lua userdata.
pub struct LuaTiledNode {
    inner: Arc<TMutex<Option<AnyClient>>>,
    label: String,
}

impl LuaTiledNode {
    fn from_any(client: AnyClient, label: String) -> Self {
        Self {
            inner: Arc::new(TMutex::new(Some(client))),
            label,
        }
    }
}

impl UserData for LuaTiledNode {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, n, ()| {
            Ok(format!("TiledNode({})", n.label))
        });
        methods.add_method("label", |_, n, ()| Ok(n.label.clone()));

        // keys() — list child names. Empty for non-container nodes.
        methods.add_method("keys", |_, n, ()| {
            let inner = n.inner.clone();
            crate::core::runtime::bsrs_runtime().block_on(async move {
                let g = inner.lock().await;
                match g.as_ref() {
                    Some(AnyClient::Container(c)) => c
                        .keys()
                        .await
                        .map_err(|e| mlua::Error::RuntimeError(format!("tiled keys: {e}"))),
                    _ => Ok(Vec::new()),
                }
            })
        });

        // len() — child count for containers; 0 otherwise.
        methods.add_method("len", |_, n, ()| {
            let inner = n.inner.clone();
            crate::core::runtime::bsrs_runtime().block_on(async move {
                let g = inner.lock().await;
                match g.as_ref() {
                    Some(AnyClient::Container(c)) => c
                        .len()
                        .await
                        .map(|n| n as i64)
                        .map_err(|e| mlua::Error::RuntimeError(format!("tiled len: {e}"))),
                    _ => Ok(0),
                }
            })
        });

        // get(key) — fetch a child. Returns a new TiledNode userdata.
        methods.add_method("get", |_, n, key: String| {
            let inner = n.inner.clone();
            let label = format!("{}/{}", n.label, key);
            let any = crate::core::runtime::bsrs_runtime().block_on(async move {
                let g = inner.lock().await;
                match g.as_ref() {
                    Some(AnyClient::Container(c)) => c
                        .get(&key)
                        .await
                        .map_err(|e| mlua::Error::RuntimeError(format!("tiled get: {e}"))),
                    _ => Err(mlua::Error::RuntimeError(
                        "tiled get: not a container".into(),
                    )),
                }
            })?;
            Ok(LuaTiledNode::from_any(any, label))
        });

        // metadata() — JSON string of the node's metadata Item.
        methods.add_method("metadata", |_, n, ()| {
            let inner = n.inner.clone();
            crate::core::runtime::bsrs_runtime().block_on(async move {
                let g = inner.lock().await;
                let item = match g.as_ref() {
                    Some(AnyClient::Container(c)) => c.base().item().clone(),
                    Some(AnyClient::Array(c)) => c.base().item().clone(),
                    Some(AnyClient::Table(c)) => c.base().item().clone(),
                    Some(AnyClient::Sparse(c)) => c.base().item().clone(),
                    Some(AnyClient::Awkward(c)) => c.base().item().clone(),
                    Some(_) => {
                        return Err(mlua::Error::RuntimeError(
                            "tiled metadata: client variant not introspectable from Lua".into(),
                        ));
                    }
                    None => {
                        return Err(mlua::Error::RuntimeError(
                            "tiled metadata: node already consumed".into(),
                        ));
                    }
                };
                serde_json::to_string(&item.attributes.metadata)
                    .map_err(|e| mlua::Error::RuntimeError(format!("tiled metadata: {e}")))
            })
        });
    }
}

/// Register the global `tiled.*` namespace on the given Lua state.
pub fn register(lua: &Lua) -> mlua::Result<()> {
    let tiled = lua.create_table()?;

    // tiled.from_uri(url, [api_key]) -> TiledNode
    tiled.set(
        "from_uri",
        lua.create_function(|_, (uri, api_key): (String, Option<String>)| {
            let mut opts = ContextOptions::default();
            if let Some(k) = api_key {
                opts = opts.api_key(k);
            }
            // Validate URL up front so we get a clean error before
            // any HTTP traffic.
            Url::parse(&uri)
                .map_err(|e| mlua::Error::RuntimeError(format!("tiled.from_uri: bad url: {e}")))?;
            let uri_for_async = uri.clone();
            let any = crate::core::runtime::bsrs_runtime().block_on(async move {
                from_uri_with_options(&uri_for_async, opts, false)
                    .await
                    .map_err(|e| mlua::Error::RuntimeError(format!("tiled.from_uri: {e}")))
            })?;
            Ok(LuaTiledNode::from_any(any, uri))
        })?,
    )?;

    lua.globals().set("tiled", tiled)?;
    Ok(())
}
