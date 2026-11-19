macro_rules! sqlx_driver {
    (
        database: $database:ty,
        pool: $pool:ty,
        connection: $connection:ty,
        transaction: $transaction:ty,
        backend: $backend:ident,
        object: $object:ident,
        driver: $driver:ident,
        manager: $manager:ident,
        name: $name:literal,
        database_name: $database_name:literal,
        backend_doc: $backend_doc:literal,
        pool_doc: $pool_doc:literal,
        acquire_note: $acquire_note:literal $(,)?
    ) => {
        pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

        #[doc(hidden)]
        pub enum Conn {
            Pooled(sqlx::pool::PoolConnection<$database>),
            Transaction($transaction),
        }

        impl Conn {
            fn connection(&mut self) -> &mut $connection {
                match self {
                    Self::Pooled(connection) => connection,
                    Self::Transaction(transaction) => transaction,
                }
            }
            fn connection_ref(&self) -> &$connection {
                match self {
                    Self::Pooled(connection) => connection,
                    Self::Transaction(transaction) => transaction,
                }
            }
        }

        #[doc = concat!("The [`ConnectionBackend`](demarcate_connection::ConnectionBackend) for ", $database_name, " through sqlx.")]
        ///
        #[doc = $backend_doc]
        pub struct $backend {
            pool: $pool,
        }

        impl demarcate_connection::ConnectionBackend for $backend {
            type Connection = Conn;
            type Error = sqlx::Error;

            const NAME: &'static str = $name;

            const UNUSABLE_MESSAGE: &'static str = UNUSABLE_MESSAGE;

            fn validate(
                &self,
                definition: &demarcate_core::TransactionDefinition,
            ) -> Result<(), demarcate_core::TransactionError> {
                validate(definition)
            }

            async fn acquire(&self) -> Result<Conn, sqlx::Error> {
                Ok(Conn::Pooled(self.pool.acquire().await?))
            }

            async fn begin(&self, definition: &demarcate_core::TransactionDefinition) -> Result<Conn, sqlx::Error> {
                Ok(Conn::Transaction(begin(&self.pool, definition).await?))
            }

            fn in_transaction(connection: &Conn) -> bool {
                matches!(connection, Conn::Transaction(_))
            }

            async fn commit(&self, connection: Conn) -> Result<(), sqlx::Error> {
                match connection {
                    Conn::Transaction(started) => commit(started).await,
                    Conn::Pooled(_) => Ok(()),
                }
            }

            async fn rollback(&self, connection: Conn) -> Result<(), sqlx::Error> {
                match connection {
                    Conn::Transaction(started) => rollback(started).await,
                    Conn::Pooled(_) => Ok(()),
                }
            }

            async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), sqlx::Error> {
                execute(connection.connection(), statement).await
            }

            async fn apply_timeout(
                &self,
                connection: &mut Conn,
                remaining: std::time::Duration,
            ) -> Result<(), sqlx::Error> {
                apply_timeout(connection.connection(), remaining).await
            }

            fn taints_transaction(&self, error: &sqlx::Error) -> bool {
                taints_transaction(error)
            }

            fn ends_transaction(&self, error: &sqlx::Error) -> bool {
                ends_transaction(error)
            }

            async fn is_usable(&self, connection: &mut Conn) -> bool {
                is_usable(connection.connection()).await
            }

            fn release_failure_is_ignorable(&self, error: &sqlx::Error) -> bool {
                release_failure_is_ignorable(error)
            }
        }

        /// The connection of one transaction, or of one synchronization scope without a transaction,
        /// bound to the transaction context under the key of its pool.
        pub type ConnectionHolder = demarcate_connection::ConnectionHolder<$backend>;

        #[doc = concat!("A ", $database_name, " pool that runs every query on the connection of the current transaction.")]
        ///
        /// Pass a reference to it wherever sqlx takes an executor:
        /// `sqlx::query("...").execute(&pool)`. Inside a transaction the query runs on the connection of
        /// that transaction. Outside of one it runs on a pooled connection in autocommit mode, unless the
        /// pool was made with [`transaction_required`](TransactionalPool::transaction_required).
        ///
        /// The queries of one transaction must be awaited one after another. Awaiting two of them at
        /// once returns an error; it does not open a second connection.
        ///
        #[doc = $pool_doc]
        ///
        /// An error of the transaction infrastructure raised during a query arrives as
        /// [`sqlx::Error::Configuration`] that wraps the
        /// [`TransactionError`](demarcate_core::TransactionError).
        ///
        /// Cloning is cheap; clones share the pool and the transaction.
        #[derive(Clone)]
        pub struct TransactionalPool {
            source: demarcate_connection::ConnectionSource<$backend>,
        }

        impl std::fmt::Debug for TransactionalPool {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TransactionalPool")
                    .field("inner", self.inner())
                    .field("key", self.resource_key())
                    .field("transaction_required", &self.is_transaction_required())
                    .finish()
            }
        }

        impl TransactionalPool {
            /// Wraps a sqlx pool.
            ///
            #[doc = concat!("Create one `TransactionalPool` for each `", stringify!($pool), "` and clone it. Two pools created from the")]
            #[doc = concat!("same `", stringify!($pool), "` do not see each other's transactions.")]
            pub fn new(pool: $pool) -> Self {
                Self { source: demarcate_connection::ConnectionSource::new($backend { pool }) }
            }

            /// Connects a sqlx pool with the given settings and wraps it.
            ///
            /// A setting that is not given keeps the default of sqlx. The transaction timeout of the
            /// settings belongs to the transaction manager and is not used here.
            ///
            /// # Errors
            ///
            /// Returns the error of sqlx when the URL is not valid or the database cannot be reached.
            pub async fn connect(settings: &demarcate_connection::PoolSettings) -> Result<Self, sqlx::Error> {
                let mut options = sqlx::pool::PoolOptions::<$database>::new();
                if let Some(max_connections) = settings.get_max_connections() {
                    options = options.max_connections(max_connections);
                }
                if let Some(min_connections) = settings.get_min_connections() {
                    options = options.min_connections(min_connections);
                }
                if let Some(acquire_timeout) = settings.get_acquire_timeout() {
                    options = options.acquire_timeout(acquire_timeout);
                }
                if let Some(idle_timeout) = settings.get_idle_timeout() {
                    options = options.idle_timeout(idle_timeout);
                }
                options.connect(settings.url()).await.map(Self::new)
            }

            /// Returns a pool that refuses to run a query outside a transaction.
            ///
            /// Give this pool to a repository whose functions must always run inside a transaction. A call
            /// without one returns [`TransactionError::NoTransaction`](demarcate_core::TransactionError::NoTransaction)
            /// instead of running in autocommit mode.
            pub fn transaction_required(&self) -> Self {
                Self { source: self.source.transaction_required() }
            }

            /// Returns whether this pool refuses to run a query outside a transaction.
            pub fn is_transaction_required(&self) -> bool {
                self.source.is_transaction_required()
            }

            /// Returns the sqlx pool. A query on it does not take part in any transaction.
            pub fn inner(&self) -> &$pool {
                &self.source.backend().pool
            }

            /// Returns the key under which the connection of a transaction is bound to the context.
            pub fn resource_key(&self) -> &demarcate_core::ResourceKey {
                self.source.resource_key()
            }

            /// Returns whether a connection of this pool is bound to the context of the calling future.
            pub fn is_connection_transactional(&self) -> bool {
                self.source.is_connection_transactional()
            }

            /// Returns the connection of the current transaction.
            ///
            /// # Errors
            ///
            /// Returns [`TransactionError::NoTransaction`](demarcate_core::TransactionError::NoTransaction)
            /// when the calling future runs in no transaction of this pool, and otherwise the errors of
            /// [`acquire`](TransactionalPool::acquire).
            pub async fn acquire_in_transaction(
                &self,
            ) -> Result<TransactionalConnection, demarcate_core::TransactionError> {
                self.source.acquire_in_transaction().await.map(TransactionalConnection)
            }

            /// Returns the connection of the current transaction, or a pooled connection when there is none.
            ///
            /// Prefer passing the pool as the executor. Use this for an API that needs a
            #[doc = concat!("`&mut ", stringify!($connection), "`. Drop the returned connection before the next query through the pool and")]
            /// before calling another transactional function; while it is alive the connection is in use.
            ///
            #[doc = $acquire_note]
            ///
            /// # Errors
            ///
            /// Returns an error when the connection of the transaction is already in use, when a nested
            /// transaction of another call owns it, when the transaction that the calling future joined has
            /// completed, or when no connection can be obtained. Returns
            /// [`TransactionError::TimedOut`](demarcate_core::TransactionError::TimedOut) when the deadline of
            /// the transaction has passed, and
            /// [`TransactionError::NoTransaction`](demarcate_core::TransactionError::NoTransaction) when the
            /// pool requires a transaction and none is running.
            pub async fn acquire(&self) -> Result<TransactionalConnection, demarcate_core::TransactionError> {
                self.source.acquire().await.map(TransactionalConnection)
            }
        }

        /// The error of [`TransactionalPool::connect`].
        pub type ConnectError = sqlx::Error;

        impl From<$pool> for TransactionalPool {
            fn from(pool: $pool) -> Self {
                Self::new(pool)
            }
        }

        fn into_sqlx(error: demarcate_core::TransactionError) -> sqlx::Error {
            sqlx::Error::Configuration(Box::new(error))
        }

        impl<'p> sqlx::Executor<'p> for &'_ TransactionalPool {
            type Database = $database;

            fn fetch_many<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> futures_core::stream::BoxStream<
                'e,
                Result<
                            sqlx::Either<<$database as sqlx::Database>::QueryResult, <$database as sqlx::Database>::Row>,
                            sqlx::Error,
                        >,
            >
            where
                'p: 'e,
                E: 'q + sqlx::Execute<'q, $database>,
            {
                use futures_util::TryStreamExt;
                let pool = self.clone();
                Box::pin(async_stream::try_stream! {
                    let mut connection = pool.acquire().await.map_err(into_sqlx)?;
                    let transaction = connection.0.transaction_holder();
                    let mut rows = (&mut *connection).fetch_many(query);
                    loop {
                        match rows.try_next().await {
                            Ok(Some(step)) => yield step,
                            Ok(None) => {
                                break;
                            }
                            Err(error) => {
                                if let Some(holder) = &transaction {
                                    holder.statement_failed(&error);
                                }
                                Err(error)?;
                            }
                        }
                    }
                })
            }

            fn fetch_optional<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> futures_core::future::BoxFuture<'e, Result<Option<<$database as sqlx::Database>::Row>, sqlx::Error>>
            where
                'p: 'e,
                E: 'q + sqlx::Execute<'q, $database>,
            {
                let pool = self.clone();
                Box::pin(async move {
                    let mut connection = pool.acquire().await.map_err(into_sqlx)?;
                    let result = (&mut *connection).fetch_optional(query).await;
                    connection.0.statement_finished(result.as_ref().err());
                    result
                })
            }

            fn prepare_with<'e>(
                self,
                sql: sqlx::SqlStr,
                parameters: &'e [<$database as sqlx::Database>::TypeInfo],
            ) -> futures_core::future::BoxFuture<'e, Result<<$database as sqlx::Database>::Statement, sqlx::Error>>
            where
                'p: 'e,
            {
                let pool = self.clone();
                Box::pin(async move {
                    let mut connection = pool.acquire().await.map_err(into_sqlx)?;
                    let result = (&mut *connection).prepare_with(sql, parameters).await;
                    connection.0.statement_finished(result.as_ref().err());
                    result
                })
            }

            fn describe<'e>(
                self,
                sql: sqlx::SqlStr,
            ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<$database>, sqlx::Error>>
            where
                'p: 'e,
            {
                let pool = self.clone();
                Box::pin(async move {
                    let mut connection = pool.acquire().await.map_err(into_sqlx)?;
                    let result = (&mut *connection).describe(sql).await;
                    connection.0.statement_finished(result.as_ref().err());
                    result
                })
            }
        }

        /// A connection obtained from a [`TransactionalPool`]: the connection of the current transaction,
        /// or a pooled connection.
        ///
        #[doc = concat!("It dereferences to [`", stringify!($connection), "`]. Dropping it does not commit or roll back; the transaction")]
        /// manager completes the transaction.
        pub struct TransactionalConnection(demarcate_connection::ConnectionHandle<$backend>);

        impl std::ops::Deref for TransactionalConnection {
            type Target = $connection;
            fn deref(&self) -> &Self::Target {
                self.0.connection_ref()
            }
        }

        impl std::ops::DerefMut for TransactionalConnection {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.0.connection()
            }
        }

        #[doc(hidden)]
        pub type $object = demarcate_connection::ConnectionTransactionObject<$backend>;

        #[doc = concat!("The [`TransactionDriver`](demarcate_core::TransactionDriver) for ", $database_name, " through sqlx.")]
        ///
        /// A nested transaction is a savepoint on the connection of the outer transaction. While it
        /// runs, that connection belongs to the nested call, and a future beside it that uses the
        /// connection gets an error.
        pub type $driver = demarcate_connection::ConnectionDriver<$backend>;

        #[doc = concat!("The transaction manager for ", $database_name, " through sqlx.")]
        pub type $manager = demarcate_core::TransactionManager<$driver>;

        /// Creates a transaction manager for the given pool, with nested transactions allowed.
        ///
        /// `demarcate::install` of this module creates and registers it in one call. Call this function
        /// to register the manager yourself:
        /// `demarcate::registry().set_default(Arc::new(manager(pool.clone())))`.
        pub fn manager(pool: TransactionalPool) -> $manager {
            demarcate_connection::manager(pool.source)
        }

        /// Creates a transaction manager for the given pool with the given settings.
        pub fn manager_with_settings(pool: TransactionalPool, settings: demarcate_core::ManagerSettings) -> $manager {
            demarcate_connection::manager_with_settings(pool.source, settings)
        }
    };
}
