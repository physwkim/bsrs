//! Plan = `Stream<Item = PlanItem>`. The engine consumes the stream serially.

use crate::core::msg::Msg;
use futures::stream::BoxStream;

/// One item in the plan stream — a message plus an optional channel to receive
/// the engine's response back into the plan.
#[non_exhaustive]
pub enum PlanItem {
    /// Just a message; no response needed.
    Bare(Msg),
}

impl From<Msg> for PlanItem {
    fn from(m: Msg) -> Self {
        PlanItem::Bare(m)
    }
}

/// A plan: a stream of `PlanItem`s.
pub type Plan = BoxStream<'static, PlanItem>;

/// Helper: wrap a `Stream<Msg>` (or a generator) into a boxed plan.
pub fn plan_box<S>(s: S) -> Plan
where
    S: futures::Stream<Item = Msg> + Send + 'static,
{
    use futures::stream::StreamExt;
    s.map(PlanItem::Bare).boxed()
}
