//! [`Command`]: a typed remote-procedure call, ophyd-async
//! `core/_command.py`. A [`TriggerableCommand`], one that takes no arguments
//! and returns nothing (an EPICS `.PROC` field), is the replacement for
//! [`SignalX`](crate::devices::SignalX).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use serde::Serialize;

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{NamedObj, TriggerableObj};
use crate::core::status::{Status, StatusError};
use crate::devices::device::{ConnectOnce, Device};
use crate::devices::signal::shown;
use crate::protocols_async::Triggerable;

/// ophyd-async `DEFAULT_TIMEOUT`.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// A backend for a [`Command`]: ophyd-async `CommandBackend`. `Args` is what
/// the command takes, a tuple for several, and `Output` what it returns.
#[async_trait]
pub trait CommandBackend: Send + Sync + 'static {
    /// The command's arguments.
    type Args: Send + 'static;
    /// The command's result.
    type Output: Serialize + Send + 'static;
    /// Source of the command named `name`.
    fn source(&self, name: &str) -> String;
    /// Connect to the underlying hardware.
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Execute the command and return its result.
    async fn execute(&self, args: Self::Args) -> Result<Self::Output>;
}

/// A device that executes a typed command: ophyd-async `Command`.
pub struct Command<B: CommandBackend> {
    backend: Arc<B>,
    name: String,
    timeout: Option<Duration>,
    connect: ConnectOnce,
}

/// A [`Command`] that takes no arguments and returns nothing, triggered as a
/// [`Triggerable`]: ophyd-async `TriggerableCommand`.
pub type TriggerableCommand<B> = Command<B>;

impl<B: CommandBackend> Command<B> {
    /// A command named `name` over `backend`, with ophyd-async's default
    /// 10 s timeout.
    pub fn new(backend: Arc<B>, name: impl Into<String>) -> Self {
        Self {
            backend,
            name: name.into(),
            timeout: Some(DEFAULT_TIMEOUT),
            connect: ConnectOnce::default(),
        }
    }

    /// Bound each execute by `timeout`, or by nothing for `None`.
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// The command's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The command's source.
    pub fn source(&self) -> String {
        self.backend.source(&self.name)
    }

    /// What bounds each execute.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Connect the backend. A connect still running, or one that succeeded,
    /// is joined instead of starting another; one that failed is retried.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        self.join_connect(timeout, false).await
    }

    /// Start a new backend connect even if the last one succeeded.
    pub async fn reconnect(&self, timeout: Duration) -> Result<()> {
        self.join_connect(timeout, true).await
    }

    async fn join_connect(&self, timeout: Duration, force: bool) -> Result<()> {
        self.connect
            .join(force, || {
                tracing::debug!("Connecting to {}", self.source());
                let backend = self.backend.clone();
                async move { backend.connect(timeout).await }
            })
            .await
    }

    /// Execute the command within its timeout and return its result.
    pub async fn execute(&self, args: B::Args) -> Result<B::Output> {
        tracing::debug!("Executing command {}", self.name);
        let result = within(self.timeout, self.backend.execute(args)).await?;
        tracing::debug!("Command {} returned {}", self.name, shown(&result));
        Ok(result)
    }
}

impl<B: CommandBackend<Args = (), Output = ()>> Command<B> {
    /// Execute within the command's timeout; the returned `Status` is
    /// resolved.
    pub async fn trigger(&self) -> Status {
        self.trigger_with_timeout(self.timeout).await
    }

    /// Execute within `timeout` instead of the command's own.
    pub async fn trigger_with_timeout(&self, timeout: Option<Duration>) -> Status {
        let source = self.source();
        tracing::debug!("Putting default value to backend at source {source}");
        match within(timeout, self.backend.execute(())).await {
            Ok(()) => {
                tracing::debug!("Successfully put default value to backend at source {source}");
                Status::done()
            }
            Err(e) => Status::fail(StatusError::Failed(e.to_string())),
        }
    }
}

/// `fut`, failing with [`BsrsError::Timeout`] past `timeout`: ophyd-async
/// `_wait_for`.
async fn within<R>(timeout: Option<Duration>, fut: impl Future<Output = Result<R>>) -> Result<R> {
    match timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .unwrap_or(Err(BsrsError::Timeout(d))),
        None => fut.await,
    }
}

impl<B: CommandBackend> Device for Command<B> {
    fn name(&self) -> &str {
        &self.name
    }
    fn connect_all_boxed<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.connect(timeout))
    }
}

impl<B: CommandBackend> NamedObj for Command<B> {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "Command",
            "source": self.source(),
        })
    }
}

#[async_trait]
impl<B: CommandBackend<Args = (), Output = ()>> Triggerable for Command<B> {
    fn name(&self) -> &str {
        &self.name
    }
    async fn trigger(&self) -> Status {
        self.trigger().await
    }
}

#[async_trait]
impl<B: CommandBackend<Args = (), Output = ()>> TriggerableObj for Command<B> {
    async fn trigger_dyn(&self) -> Status {
        self.trigger().await
    }
}

/// A command's callback, async or made so.
type CommandFn<A, R> = Arc<dyn Fn(A) -> BoxFuture<'static, Result<R>> + Send + Sync>;

/// A [`Command`] backend that runs a callback: ophyd-async
/// `SoftCommandBackend`. Calls are serialized, as hardware commands should
/// not run concurrently.
pub struct SoftCommandBackend<A, R> {
    command_cb: CommandFn<A, R>,
    lock: tokio::sync::Mutex<()>,
}

impl<A, R> SoftCommandBackend<A, R> {
    /// Run `command_cb` on each execute.
    pub fn new<F, Fut>(command_cb: F) -> Self
    where
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        Self {
            command_cb: Arc::new(move |args| Box::pin(command_cb(args))),
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl<A, R> CommandBackend for SoftCommandBackend<A, R>
where
    A: Send + 'static,
    R: Serialize + Send + 'static,
{
    type Args = A;
    type Output = R;
    fn source(&self, name: &str) -> String {
        format!("softcmd://{name}")
    }
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, args: A) -> Result<R> {
        let _serial = self.lock.lock().await;
        (self.command_cb)(args).await
    }
}

/// A [`Command`] named `name` that runs `command_cb`: ophyd-async
/// `soft_command`.
pub fn soft_command<A, R, F, Fut>(
    command_cb: F,
    name: impl Into<String>,
) -> Command<SoftCommandBackend<A, R>>
where
    A: Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    Command::new(Arc::new(SoftCommandBackend::new(command_cb)), name)
}

/// Callback run on every mock execute in place of the command: ophyd-async
/// `callback_on_mock_execute`'s callback, the sync form.
pub type MockExecuteCallback<A, R> = Box<dyn Fn(A) -> Result<R> + Send + Sync>;

struct MockInner<A, R> {
    initial: Arc<dyn CommandBackend<Args = A, Output = R>>,
    /// What an execute runs with no callback set.
    fallback: CommandFn<A, R>,
    callback: Mutex<Option<CommandFn<A, R>>>,
    /// Every execute's arguments, in call order (ophyd-async `execute_mock`).
    calls: Mutex<Vec<A>>,
    lock: tokio::sync::Mutex<()>,
}

/// Mock [`Command`] backend for unit tests: ophyd-async
/// `MockCommandBackend`. Records every execute and runs a callback if one is
/// set, otherwise what it mocks: a soft command's own callback, or for a
/// hardware backend the output's default. Cheap to clone, so a test can keep
/// a handle while a `Command` owns another.
pub struct MockCommandBackend<A, R> {
    inner: Arc<MockInner<A, R>>,
}

impl<A, R> Clone for MockCommandBackend<A, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A, R> MockCommandBackend<A, R>
where
    A: Clone + Send + 'static,
    R: Serialize + Send + 'static,
{
    /// Mock `soft`, still running its callback until one is set.
    pub fn new(soft: SoftCommandBackend<A, R>) -> Self {
        let fallback = soft.command_cb.clone();
        Self::over(Arc::new(soft), fallback)
    }

    /// Mock a hardware backend, returning `R::default()` until a callback
    /// is set.
    pub fn wrapping<B>(initial: B) -> Self
    where
        B: CommandBackend<Args = A, Output = R>,
        R: Default,
    {
        Self::over(
            Arc::new(initial),
            Arc::new(|_| Box::pin(async { Ok(R::default()) })),
        )
    }

    fn over(
        initial: Arc<dyn CommandBackend<Args = A, Output = R>>,
        fallback: CommandFn<A, R>,
    ) -> Self {
        Self {
            inner: Arc::new(MockInner {
                initial,
                fallback,
                callback: Mutex::new(None),
                calls: Mutex::new(Vec::new()),
                lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// Run `cb` on each execute instead; `None` restores what is mocked.
    pub fn set_execute_callback(&self, cb: Option<MockExecuteCallback<A, R>>) {
        *self.inner.callback.lock().unwrap() = cb.map(|f| -> CommandFn<A, R> {
            Arc::new(move |args| Box::pin(std::future::ready(f(args))))
        });
    }

    /// [`set_execute_callback`](Self::set_execute_callback) for an async
    /// callback.
    pub fn set_execute_callback_async<F, Fut>(&self, cb: F)
    where
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        *self.inner.callback.lock().unwrap() = Some(Arc::new(move |args| Box::pin(cb(args))));
    }

    /// Every execute's arguments, in call order.
    pub fn calls(&self) -> Vec<A> {
        self.inner.calls.lock().unwrap().clone()
    }
}

impl<A, R> crate::devices::BackendFromPv for MockCommandBackend<A, R>
where
    A: Clone + Send + 'static,
    R: Default + Serialize + Send + 'static,
{
    fn from_pv(_pv: &str) -> Self {
        Self::new(SoftCommandBackend::new(|_| async { Ok(R::default()) }))
    }
}

#[async_trait]
impl<A, R> CommandBackend for MockCommandBackend<A, R>
where
    A: Clone + Send + 'static,
    R: Serialize + Send + 'static,
{
    type Args = A;
    type Output = R;
    fn source(&self, name: &str) -> String {
        format!("mock+{}", self.inner.initial.source(name))
    }
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        // ophyd-async raises here because Device.connect(mock=True) swaps the
        // backend without calling connect. bsrs has no runtime backend swap
        // (the backend type is fixed on the Command), so a device built over
        // the mock connects transparently. No-op.
        Ok(())
    }
    async fn execute(&self, args: A) -> Result<R> {
        let _serial = self.inner.lock.lock().await;
        self.inner.calls.lock().unwrap().push(args.clone());
        let cb = self.inner.callback.lock().unwrap().clone();
        cb.unwrap_or_else(|| self.inner.fallback.clone())(args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::devices::DeviceVector;
    use std::fmt::Debug;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const T: Duration = Duration::from_secs(1);

    #[derive(Clone, Debug, Default, PartialEq, Serialize)]
    enum Choice {
        #[default]
        A,
        B,
    }

    #[derive(Clone, Debug, Default, PartialEq, Serialize)]
    struct Table {
        a: Vec<i32>,
        b: Vec<String>,
    }

    fn echo<V: Send + 'static>() -> SoftCommandBackend<V, V> {
        SoftCommandBackend::new(|v| async move { Ok(v) })
    }

    async fn echoes<V>(value: V)
    where
        V: Clone + Debug + Default + PartialEq + Serialize + Send + Sync + 'static,
    {
        let cmd = Command::new(Arc::new(echo::<V>()), "test_cmd");
        cmd.connect(T).await.unwrap();
        assert_eq!(cmd.execute(value.clone()).await.unwrap(), value);

        // test_mock_command_backend_default_values: a mocked soft command
        // still runs its callback, a mocked hardware one returns a default.
        let soft = Command::new(Arc::new(MockCommandBackend::new(echo::<V>())), "test_cmd");
        assert_eq!(soft.execute(value.clone()).await.unwrap(), value);
        let hw = Command::new(
            Arc::new(MockCommandBackend::wrapping(echo::<V>())),
            "test_cmd",
        );
        assert_eq!(hw.execute(value).await.unwrap(), V::default());
    }

    // ophyd-async test_soft_command_execution's TEST_PARAMS, each a type a
    // command can take and return.
    #[tokio::test]
    async fn soft_command_returns_what_its_callback_does() {
        echoes(true).await;
        echoes(42_i64).await;
        echoes(2.5_f64).await;
        echoes("hello".to_string()).await;
        echoes(Choice::B).await;
        echoes(vec![true, false]).await;
        echoes(vec![1_i8, 2]).await;
        echoes(vec![1_u8, 2]).await;
        echoes(vec![1_i16, 2]).await;
        echoes(vec![1_u16, 2]).await;
        echoes(vec![1_i32, 2]).await;
        echoes(vec![1_u32, 2]).await;
        echoes(vec![1_i64, 2]).await;
        echoes(vec![1_u64, 2]).await;
        echoes(vec![1.1_f32, 2.2]).await;
        echoes(vec![1.1_f64, 2.2]).await;
        echoes(vec![vec![1_i64, 2], vec![3, 4]]).await;
        echoes(vec!["a".to_string(), "b".into()]).await;
        echoes(vec![Choice::A, Choice::B]).await;
        echoes(Table {
            a: vec![1],
            b: vec!["hi".into()],
        })
        .await;
    }

    // ophyd-async test_mock_command_backend_custom_callback.
    #[tokio::test]
    async fn mock_callback_replaces_the_command_until_cleared() {
        let soft =
            SoftCommandBackend::new(|(a, b): (i64, String)| async move { Ok(format!("{b}_{a}")) });
        let mock = MockCommandBackend::new(soft);
        let cmd = Command::new(Arc::new(mock.clone()), "test_cmd");
        mock.set_execute_callback(Some(Box::new(|(a, b)| Ok(format!("mock_{b}_{a}")))));
        assert_eq!(
            cmd.execute((3, "test".into())).await.unwrap(),
            "mock_test_3"
        );
        assert_eq!(mock.calls(), [(3, "test".to_string())]);
        mock.set_execute_callback(None);
        assert_eq!(cmd.execute((1, "x".into())).await.unwrap(), "x_1");
    }

    fn doubling(calls: &Arc<Mutex<Vec<i64>>>) -> SoftCommandBackend<i64, i64> {
        let calls = calls.clone();
        SoftCommandBackend::new(move |v: i64| {
            calls.lock().unwrap().push(v);
            async move { Ok(v * 2) }
        })
    }

    // ophyd-async test_soft_command_mock_calls_original_func.
    #[tokio::test]
    async fn mocked_soft_command_runs_its_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let cmd = Command::new(
            Arc::new(MockCommandBackend::new(doubling(&calls))),
            "test_cmd",
        );
        assert_eq!(cmd.execute(5).await.unwrap(), 10);
        assert_eq!(*calls.lock().unwrap(), [5]);
    }

    // ophyd-async test_soft_command_mock_side_effect_overrides_func, with a
    // sync and an async override.
    #[tokio::test]
    async fn mock_callback_overrides_the_soft_callback() {
        for asynchronous in [false, true] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let mock = MockCommandBackend::new(doubling(&calls));
            let cmd = Command::new(Arc::new(mock.clone()), "test_cmd");
            if asynchronous {
                mock.set_execute_callback_async(|_| async { Ok(99) });
            } else {
                mock.set_execute_callback(Some(Box::new(|_| Ok(99))));
            }
            assert_eq!(cmd.execute(5).await.unwrap(), 99);
            assert!(calls.lock().unwrap().is_empty());
            mock.set_execute_callback(None);
            assert_eq!(cmd.execute(5).await.unwrap(), 10);
            assert_eq!(*calls.lock().unwrap(), [5]);
        }
    }

    // ophyd-async test_mock_command_backend_properties. Its connect raises,
    // as ophyd-async's mock connect swaps the backend; bsrs connects a
    // device built over the mock (see `MockCommandBackend::connect`).
    #[tokio::test]
    async fn mock_source_names_what_it_mocks() {
        let mock = MockCommandBackend::new(SoftCommandBackend::new(|a: i64| async move {
            Ok(a.to_string())
        }));
        assert_eq!(mock.source("test"), "mock+softcmd://test");
        mock.connect(T).await.unwrap();
    }

    // ophyd-async test_soft_command_factory.
    #[tokio::test]
    async fn soft_command_builds_a_command() {
        let rw = soft_command(|x: i64| async move { Ok(x.to_string()) }, "rw");
        rw.connect(T).await.unwrap();
        assert_eq!(rw.execute(123).await.unwrap(), "123");
        let x = soft_command(|()| async { Ok(()) }, "x");
        x.connect(T).await.unwrap();
        x.execute(()).await.unwrap();
    }

    // ophyd-async test_execution_error_wrapping.
    #[tokio::test]
    async fn callback_error_is_the_command_error() {
        let cmd = soft_command(
            |()| async { Err::<(), _>(BsrsError::Backend("Boom".into())) },
            "test_cmd",
        );
        cmd.connect(T).await.unwrap();
        let e = cmd.execute(()).await.unwrap_err();
        assert!(e.to_string().contains("Boom"), "{e}");
    }

    // ophyd-async test_command_logging.
    #[tokio::test]
    async fn connect_and_execute_are_logged() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let cmd = soft_command(|x: i64| async move { Ok(x.to_string()) }, "mycmd");
        cmd.connect(T).await.unwrap();
        assert!(caplog.text().contains("Connecting to softcmd://mycmd"));
        assert_eq!(cmd.execute(42).await.unwrap(), "42");
        let text = caplog.text();
        assert!(text.contains("Executing command mycmd"), "{text}");
        assert!(text.contains("Command mycmd returned 42"), "{text}");
    }

    // ophyd-async test_command_trigger.
    #[tokio::test]
    async fn trigger_runs_within_the_timeout_given() {
        let called = Arc::new(AtomicUsize::new(0));
        let n = called.clone();
        let backend = SoftCommandBackend::new(move |()| {
            n.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            }
        });
        let cmd: TriggerableCommand<_> =
            Command::new(Arc::new(backend), "test_cmd").with_timeout(Some(Duration::from_secs(5)));
        cmd.connect(T).await.unwrap();
        cmd.trigger().await.await.unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 1);
        cmd.trigger_with_timeout(Some(Duration::from_secs(10)))
            .await
            .await
            .unwrap();
        let e = cmd
            .trigger_with_timeout(Some(Duration::from_millis(50)))
            .await
            .await
            .unwrap_err();
        assert!(e.to_string().contains("timeout after 50ms"), "{e}");
    }

    // ophyd-async test_fill_child_command_vector_index.
    #[tokio::test]
    async fn device_vector_holds_commands() {
        let mut vector = DeviceVector::new();
        for i in 1..4 {
            let cmd = soft_command(|()| async { Ok(0_i64) }, format!("my_cmd-{i}"));
            vector.insert(i, cmd);
        }
        vector.connect_all(T).await.unwrap();
        assert_eq!(vector.len(), 3);
        for i in 1..4 {
            assert_eq!(vector.get(i).unwrap().execute(()).await.unwrap(), 0);
        }
    }

    struct Hardware;

    #[async_trait]
    impl CommandBackend for Hardware {
        type Args = ();
        type Output = ();
        fn source(&self, name: &str) -> String {
            format!("hw://{name}")
        }
        async fn connect(&self, _timeout: Duration) -> Result<()> {
            Ok(())
        }
        async fn execute(&self, _args: ()) -> Result<()> {
            Ok(())
        }
    }

    // ophyd-async test_mock_triggerable_command_null_sig_backend.
    #[tokio::test]
    async fn mocked_hardware_trigger_is_recorded() {
        let mock = MockCommandBackend::wrapping(Hardware);
        let cmd = Command::new(Arc::new(mock.clone()), "hw_cmd");
        cmd.connect(T).await.unwrap();
        assert_eq!(cmd.source(), "mock+hw://hw_cmd");
        cmd.trigger().await.await.unwrap();
        assert_eq!(mock.calls().len(), 1);
    }

    // SoftCommandBackend's lock: hardware commands do not run concurrently.
    #[tokio::test]
    async fn soft_executes_run_one_at_a_time() {
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let (r, m) = (running.clone(), most.clone());
        let cmd = soft_command(
            move |()| {
                let (r, m) = (r.clone(), m.clone());
                async move {
                    m.fetch_max(r.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    r.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            "serial",
        );
        let (a, b, c) = tokio::join!(cmd.execute(()), cmd.execute(()), cmd.execute(()));
        a.and(b).and(c).unwrap();
        assert_eq!(most.load(Ordering::SeqCst), 1);
    }
}
