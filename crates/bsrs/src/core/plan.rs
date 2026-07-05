//! Plan = `Stream<Item = PlanItem>`. The engine consumes the stream serially.

use crate::core::msg::{Msg, MsgResult};
use futures::stream::BoxStream;
use tokio::sync::oneshot;

/// One item in the plan stream — a message, optionally paired with a channel
/// that carries the engine's [`MsgResult`] back into the plan.
#[non_exhaustive]
pub enum PlanItem {
    /// Just a message; no response needed. The engine handles it and moves on.
    Bare(Msg),
    /// A message whose engine result the plan needs *inline*. The engine
    /// handles the message, then sends the resulting [`MsgResult`] through the
    /// channel; the plan awaits the receiver to branch on it (e.g.
    /// `collect_while_completing` loops until `Wait` reports the group done).
    /// The channel replaces bluesky's `value = yield from <plan>` return path,
    /// which bsrs's message-stream model cannot otherwise express.
    Respond(Msg, oneshot::Sender<MsgResult>),
}

impl From<Msg> for PlanItem {
    fn from(m: Msg) -> Self {
        PlanItem::Bare(m)
    }
}

impl PlanItem {
    /// The message this item carries, borrowed for inspection (both variants
    /// carry one). Lets preprocessors match on the `Msg` without discarding a
    /// [`PlanItem::Respond`]'s response channel.
    pub fn msg(&self) -> &Msg {
        match self {
            PlanItem::Bare(m) | PlanItem::Respond(m, _) => m,
        }
    }

    /// Transform the carried message 1:1, preserving any response channel. The
    /// structural primitive preprocessors use to rewrite a `Msg` (e.g.
    /// `relative_set_wrapper` biasing a `Set`) while keeping a `Respond` item's
    /// sender intact.
    pub fn map_msg(self, f: impl FnOnce(Msg) -> Msg) -> PlanItem {
        match self {
            PlanItem::Bare(m) => PlanItem::Bare(f(m)),
            PlanItem::Respond(m, tx) => PlanItem::Respond(f(m), tx),
        }
    }
}

/// Build a [`PlanItem::Respond`] and the receiver a plan awaits for the result.
///
/// Usage inside an `async_stream::stream!` plan body:
/// ```ignore
/// let (item, rx) = respond(Msg::Wait { group, error_on_timeout: false, timeout });
/// yield item;
/// let done = matches!(rx.await, Ok(MsgResult::WaitComplete { done: true }));
/// ```
/// A dropped sender (the engine failed the message before responding) surfaces
/// as `Err(RecvError)` on the receiver; treat it as "not done / no value".
pub fn respond(msg: Msg) -> (PlanItem, oneshot::Receiver<MsgResult>) {
    let (tx, rx) = oneshot::channel();
    (PlanItem::Respond(msg, tx), rx)
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

/// Helper: box a stream that already yields [`PlanItem`]s directly — the form
/// used by plans that mix [`PlanItem::Bare`] with [`respond`]-produced items.
pub fn plan_items<S>(s: S) -> Plan
where
    S: futures::Stream<Item = PlanItem> + Send + 'static,
{
    use futures::stream::StreamExt;
    s.boxed()
}
