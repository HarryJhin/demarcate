use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use dtx_core::{
    BoxFuture, Isolation, ManagerSettings, PlatformTransactionManager, Savepoint, TemplateError, TransactionDefinition,
    TransactionError, TransactionStatus, TransactionTemplate, synchronization,
};
use super::{
    Connection, DataSource, DataSourceTransactionDriver, SharedConnection, SqlError, Statement, data_source_key,
};

type SqlResult<T> = Result<T, SqlError>;

#[derive(Clone, Default)]
pub struct CallLog(Arc<Mutex<Vec<String>>>);

impl CallLog {
    pub fn record(&self, call: impl Into<String>) {
        self.0.lock().unwrap().push(call.into());
    }
    pub fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    pub fn times_starting_with(&self, prefix: &str) -> usize {
        self.calls().iter().filter(|recorded| recorded.starts_with(prefix)).count()
    }
    pub fn times(&self, call: &str) -> usize {
        self.calls().iter().filter(|recorded| recorded.as_str() == call).count()
    }
    pub fn verify(&self, call: &str) {
        self.verify_times(call, 1);
    }
    pub fn verify_times(&self, call: &str, times: usize) {
        assert_eq!(self.times(call), times, "expected {times} call(s) of {call}, log: {:?}", self.calls());
    }
    pub fn verify_in_order(&self, expected: &[&str]) {
        let calls = self.calls();
        let mut position = 0;
        for call in expected {
            match calls[position..].iter().position(|recorded| recorded == call) {
                Some(found) => position += found + 1,
                None => panic!("expected {call} in order {expected:?}, log: {calls:?}"),
            }
        }
    }
    pub fn verify_exactly(&self, expected: &[&str]) {
        assert_eq!(self.calls(), expected, "interactions differ");
    }
    pub fn verify_no_interactions(&self) {
        assert!(self.calls().is_empty(), "expected no interactions, log: {:?}", self.calls());
    }
}

#[derive(Clone)]
pub struct MockSettings {
    pub auto_commit: bool,
    pub read_only: bool,
    pub isolation: Isolation,
    pub supports_savepoints: bool,
    pub failures: HashMap<&'static str, String>,
}

impl Default for MockSettings {
    fn default() -> Self {
        Self {
            auto_commit: false,
            read_only: false,
            isolation: Isolation::Default,
            supports_savepoints: false,
            failures: HashMap::new(),
        }
    }
}

pub struct MockConnection {
    pub log: CallLog,
    pub settings: Arc<Mutex<MockSettings>>,
}

impl MockConnection {
    fn call<T>(&self, name: &'static str, recorded: String, value: T) -> SqlResult<T> {
        self.log.record(recorded);
        match self.settings.lock().unwrap().failures.get(name) {
            Some(message) => Err(SqlError::new(message.clone())),
            None => Ok(value),
        }
    }
}

struct MockStatement {
    log: CallLog,
    prefix: &'static str,
}

impl Statement for MockStatement {
    fn execute_update<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, SqlResult<u64>> {
        Box::pin(async move {
            self.log.record(format!("{}.executeUpdate({sql})", self.prefix));
            Ok(0)
        })
    }
    fn set_query_timeout(&mut self, seconds: i32) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move {
            self.log.record(format!("{}.setQueryTimeout({seconds})", self.prefix));
            Ok(())
        })
    }
    fn close(&mut self) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move {
            self.log.record(format!("{}.close", self.prefix));
            Ok(())
        })
    }
}

impl Connection for MockConnection {
    fn auto_commit(&mut self) -> BoxFuture<'_, SqlResult<bool>> {
        Box::pin(async move {
            let value = self.settings.lock().unwrap().auto_commit;
            self.call("getAutoCommit", "getAutoCommit".into(), value)
        })
    }
    fn set_auto_commit(&mut self, auto_commit: bool) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("setAutoCommit", format!("setAutoCommit({auto_commit})"), ()) })
    }
    fn commit(&mut self) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("commit", "commit".into(), ()) })
    }
    fn rollback(&mut self) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("rollback", "rollback".into(), ()) })
    }
    fn is_read_only(&mut self) -> BoxFuture<'_, SqlResult<bool>> {
        Box::pin(async move {
            let value = self.settings.lock().unwrap().read_only;
            self.call("isReadOnly", "isReadOnly".into(), value)
        })
    }
    fn set_read_only(&mut self, read_only: bool) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("setReadOnly", format!("setReadOnly({read_only})"), ()) })
    }
    fn transaction_isolation(&mut self) -> BoxFuture<'_, SqlResult<Isolation>> {
        Box::pin(async move {
            let value = self.settings.lock().unwrap().isolation;
            self.call("getTransactionIsolation", "getTransactionIsolation".into(), value)
        })
    }
    fn set_transaction_isolation(&mut self, isolation: Isolation) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move {
            self.call("setTransactionIsolation", format!("setTransactionIsolation({isolation:?})"), ())
        })
    }
    fn supports_savepoints(&mut self) -> BoxFuture<'_, SqlResult<bool>> {
        Box::pin(async move { Ok(self.settings.lock().unwrap().supports_savepoints) })
    }
    fn set_savepoint<'a>(&'a mut self, name: &'a str) -> BoxFuture<'a, SqlResult<Savepoint>> {
        Box::pin(async move {
            let savepoint: Savepoint = Arc::new(name.to_owned());
            self.call("setSavepoint", format!("setSavepoint({name})"), savepoint)
        })
    }
    fn rollback_to_savepoint<'a>(&'a mut self, savepoint: &'a Savepoint) -> BoxFuture<'a, SqlResult<()>> {
        Box::pin(async move {
            let name = savepoint.downcast_ref::<String>().cloned().unwrap_or_default();
            self.call("rollback(sp)", format!("rollback({name})"), ())
        })
    }
    fn release_savepoint<'a>(&'a mut self, savepoint: &'a Savepoint) -> BoxFuture<'a, SqlResult<()>> {
        Box::pin(async move {
            let name = savepoint.downcast_ref::<String>().cloned().unwrap_or_default();
            self.call("releaseSavepoint", format!("releaseSavepoint({name})"), ())
        })
    }
    fn create_statement(&mut self) -> BoxFuture<'_, SqlResult<Box<dyn Statement>>> {
        Box::pin(async move {
            let statement: Box<dyn Statement> = Box::new(MockStatement { log: self.log.clone(), prefix: "stmt" });
            self.call("createStatement", "createStatement".into(), statement)
        })
    }
    fn prepare_statement<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, SqlResult<Box<dyn Statement>>> {
        Box::pin(async move {
            let statement: Box<dyn Statement> = Box::new(MockStatement { log: self.log.clone(), prefix: "ps" });
            self.call("prepareStatement", format!("prepareStatement({sql})"), statement)
        })
    }
    fn warnings(&mut self) -> BoxFuture<'_, SqlResult<Vec<String>>> {
        Box::pin(async move { self.call("getWarnings", "getWarnings".into(), Vec::new()) })
    }
    fn clear_warnings(&mut self) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("clearWarnings", "clearWarnings".into(), ()) })
    }
    fn close(&mut self) -> BoxFuture<'_, SqlResult<()>> {
        Box::pin(async move { self.call("close", "close".into(), ()) })
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct Con {
    pub shared: SharedConnection,
    pub log: CallLog,
    pub settings: Arc<Mutex<MockSettings>>,
}

impl Con {
    pub fn new() -> Self {
        let log = CallLog::default();
        let settings = Arc::new(Mutex::new(MockSettings::default()));
        let shared = super::shared(MockConnection { log: log.clone(), settings: settings.clone() });
        Self { shared, log, settings }
    }
    pub fn given(&self, configure: impl FnOnce(&mut MockSettings)) -> &Self {
        configure(&mut self.settings.lock().unwrap());
        self
    }
    pub fn will_throw(&self, method: &'static str, message: &str) -> &Self {
        self.settings.lock().unwrap().failures.insert(method, message.to_owned());
        self
    }
    pub fn is(&self, other: &SharedConnection) -> bool {
        Arc::ptr_eq(&self.shared, other)
    }
}

pub struct MockDataSource {
    connections: Mutex<VecDeque<SharedConnection>>,
    failure: Mutex<Option<String>>,
    pub log: CallLog,
}

impl MockDataSource {
    pub fn returning(connections: &[&Con]) -> Arc<dyn DataSource> {
        Arc::new(Self {
            connections: Mutex::new(connections.iter().map(|con| con.shared.clone()).collect()),
            failure: Mutex::new(None),
            log: CallLog::default(),
        })
    }
    pub fn returning_with_log(connections: &[&Con]) -> (Arc<dyn DataSource>, CallLog) {
        let log = CallLog::default();
        let data_source: Arc<dyn DataSource> = Arc::new(Self {
            connections: Mutex::new(connections.iter().map(|con| con.shared.clone()).collect()),
            failure: Mutex::new(None),
            log: log.clone(),
        });
        (data_source, log)
    }
    pub fn failing(message: &str) -> Arc<dyn DataSource> {
        Arc::new(Self {
            connections: Mutex::new(VecDeque::new()),
            failure: Mutex::new(Some(message.to_owned())),
            log: CallLog::default(),
        })
    }
}

impl DataSource for MockDataSource {
    fn connection(&self) -> BoxFuture<'_, SqlResult<SharedConnection>> {
        Box::pin(async move {
            self.log.record("getConnection");
            if let Some(message) = self.failure.lock().unwrap().clone() {
                return Err(SqlError::new(message));
            }
            let mut connections = self.connections.lock().unwrap();
            if connections.len() > 1 {
                Ok(connections.pop_front().expect("connection"))
            } else {
                Ok(connections.front().expect("connection").clone())
            }
        })
    }
}

pub type Tm = Arc<dyn PlatformTransactionManager>;

pub fn manager(data_source: &Arc<dyn DataSource>) -> Tm {
    manager_with(data_source, |_| {})
}

pub fn manager_with(data_source: &Arc<dyn DataSource>, configure: impl FnOnce(&mut ManagerSettings)) -> Tm {
    manager_from(DataSourceTransactionDriver::new(data_source.clone()), configure)
}

pub fn manager_from(driver: DataSourceTransactionDriver, configure: impl FnOnce(&mut ManagerSettings)) -> Tm {
    let mut settings = ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() };
    configure(&mut settings);
    Arc::new(driver.into_manager_with(settings))
}

pub fn template(manager: &Tm, definition: TransactionDefinition) -> TransactionTemplate {
    TransactionTemplate::with_definition(manager.clone(), definition)
}

pub async fn execute<T, F, Fut>(template: &TransactionTemplate, action: F) -> Result<T, TransactionError>
where
    F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
    Fut: Future<Output = Result<T, TransactionError>>,
{
    template.execute(action).await.map_err(|error| match error {
        TemplateError::Application(error) | TemplateError::Transaction(error) => error,
        TemplateError::Rollback { rollback, .. } => rollback,
    })
}

pub fn has_resource(data_source: &Arc<dyn DataSource>) -> bool {
    synchronization::has_resource(&data_source_key(data_source))
}

pub async fn scoped<F: Future>(future: F) -> F::Output {
    synchronization::scope(async {
        let output = future.await;
        assert!(synchronization::resource_keys().is_empty());
        assert!(!synchronization::is_synchronization_active());
        assert!(!synchronization::is_current_transaction_read_only());
        assert!(!synchronization::is_actual_transaction_active());
        output
    })
    .await
}

pub fn application_exception() -> TransactionError {
    TransactionError::Other("Application exception".into())
}
