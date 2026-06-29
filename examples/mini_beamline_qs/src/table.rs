//! `Table` — composite 2-axis stage on top of the mini-beamline
//! `mini:dot:mtrx` / `mini:dot:mtry` sim motors.
//!
//! No scalar setpoint (it's 2D), so no `MovableObj` / `LocatableObj`.
//! Registered as `ReadableObj` only (so `read()` returns both axes in
//! one Reading), plus `#[lua_methods]` for explicit 2D motion:
//!
//! - `table:move_to_xy(x, y)` — set both axes in parallel and wait.
//! - `table:at_xy()` — `{x = ..., y = ...}` table.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsrs::core::error::Result;
use bsrs::core::msg::{NamedObj, ReadableObj};
use bsrs::core::reading::ReadingValue;
use bsrs::event_model::DataKey;
use bsrs::host::ca_devices::CaMotor;

/// Composite 2-axis table.
pub struct Table {
    name: String,
    x: Arc<CaMotor>,
    y: Arc<CaMotor>,
}

impl Table {
    pub async fn connect(
        name: &str,
        x_val_pv: &str,
        x_rbv_pv: &str,
        y_val_pv: &str,
        y_rbv_pv: &str,
    ) -> Result<Arc<Self>> {
        let x = CaMotor::connect_async("table_x", x_val_pv, x_rbv_pv).await?;
        let y = CaMotor::connect_async("table_y", y_val_pv, y_rbv_pv).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            x,
            y,
        }))
    }
}

impl NamedObj for Table {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "Table",
            "children": ["table_x", "table_y"],
        })
    }
}

#[async_trait]
impl ReadableObj for Table {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let (rx, ry) = tokio::join!(self.x.read_dyn(), self.y.read_dyn());
        let mut out = rx?;
        out.extend(ry?);
        Ok(out)
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        let (dx, dy) = tokio::join!(self.x.describe_dyn(), self.y.describe_dyn());
        let mut out = dx?;
        out.extend(dy?);
        Ok(out)
    }
}

#[bsrs::lua_methods]
impl Table {
    /// Issue parallel puts on both axes and await both completions.
    #[lua_method]
    pub async fn move_to_xy(&self, x: f64, y: f64) -> Result<(), String> {
        use bsrs::core::msg::MovableObj;
        let (sx, sy) = tokio::join!(self.x.set_dyn(x), self.y.set_dyn(y));
        let (rx, ry) = tokio::join!(sx, sy);
        rx.map_err(|e| format!("x: {e:?}"))?;
        ry.map_err(|e| format!("y: {e:?}"))?;
        Ok(())
    }

    /// Current position as `{x=..., y=...}`. Lua receives a table.
    #[lua_method]
    pub async fn at_xy(&self) -> Result<serde_json::Value, String> {
        use bsrs::core::msg::LocatableObj;
        let (lx, ly) = tokio::join!(self.x.locate_dyn(), self.y.locate_dyn());
        let lx = lx.map_err(|e| e.to_string())?;
        let ly = ly.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "x": lx.readback,
            "y": ly.readback,
        }))
    }
}
