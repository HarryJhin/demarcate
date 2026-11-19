use std::fmt;

use demarcate_connection::SettingsError;

/// A transaction manager could not be installed because another one holds its place.
///
/// Every `#[transactional]` function of the process uses the installed managers, so a second
/// installation in the same place would silently move those functions to another database.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InstallError {
    /// A default transaction manager is already installed.
    DefaultAlreadyInstalled,
    /// A transaction manager is already installed under this name.
    NameAlreadyInstalled(String),
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefaultAlreadyInstalled => f.write_str(
                "A default transaction manager is already installed. Install the manager of a second \
                 database under a name with install_as, and name it in \
                 #[transactional(transaction_manager = \"...\")]",
            ),
            Self::NameAlreadyInstalled(name) => {
                write!(f, "A transaction manager is already installed under the name '{name}'")
            }
        }
    }
}

impl std::error::Error for InstallError {}

/// A pool could not be set up from settings.
#[derive(Debug)]
#[non_exhaustive]
pub enum SetupError<E> {
    /// A setting could not be read.
    Settings(SettingsError),
    /// The connection library could not connect the pool.
    Connect(E),
    /// The transaction manager could not be installed.
    Install(InstallError),
}

impl<E: fmt::Display> fmt::Display for SetupError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Settings(error) => write!(f, "{error}"),
            Self::Connect(error) => write!(f, "Could not connect the pool: {error}"),
            Self::Install(error) => write!(f, "{error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for SetupError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Settings(error) => Some(error),
            Self::Connect(error) => Some(error),
            Self::Install(error) => Some(error),
        }
    }
}

impl<E> From<SettingsError> for SetupError<E> {
    fn from(error: SettingsError) -> Self {
        Self::Settings(error)
    }
}

impl<E> From<InstallError> for SetupError<E> {
    fn from(error: InstallError) -> Self {
        Self::Install(error)
    }
}

#[cfg(any(
    feature = "sqlx-postgres",
    feature = "sqlx-mysql",
    feature = "sqlx-sqlite",
    feature = "sea-orm",
    feature = "tokio-postgres",
    feature = "diesel-async-postgres",
    feature = "diesel-async-mysql"
))]
pub(crate) fn register(
    name: Option<&str>,
    manager: std::sync::Arc<dyn demarcate_core::PlatformTransactionManager>,
) -> Result<(), InstallError> {
    let registry = demarcate_interceptor::registry();
    match name {
        None => registry.set_default_if_absent(manager).then_some(()).ok_or(InstallError::DefaultAlreadyInstalled),
        Some(name) => registry
            .register_if_absent(name, manager)
            .then_some(())
            .ok_or_else(|| InstallError::NameAlreadyInstalled(name.to_owned())),
    }
}

#[cfg(any(
    feature = "sqlx-postgres",
    feature = "sqlx-mysql",
    feature = "sqlx-sqlite",
    feature = "sea-orm",
    feature = "tokio-postgres",
    feature = "diesel-async-postgres",
    feature = "diesel-async-mysql"
))]
macro_rules! install_functions {
    (pool: $pool:ident, what: $what:literal) => {
        /// Creates the transaction manager for the given pool and installs it as the default
        /// manager: the one of every `#[transactional]` function that names no manager.
        ///
        #[doc = concat!("Pass ", $what, ", or a [`", stringify!($pool), "`] that wraps it. Give the returned value to the")]
        /// data access code. The manager allows nested transactions.
        ///
        /// # Errors
        ///
        /// Returns an error when a default manager is already installed.
        pub fn install(pool: impl Into<$pool>) -> Result<$pool, $crate::InstallError> {
            install_manager(None, pool.into(), $crate::connection::default_manager_settings())
        }

        /// Creates the transaction manager for the given pool and installs it under a name, for
        /// the functions that say `#[transactional(transaction_manager = "name")]`.
        ///
        /// # Errors
        ///
        /// Returns an error when a manager is already installed under the name.
        pub fn install_as(name: &str, pool: impl Into<$pool>) -> Result<$pool, $crate::InstallError> {
            install_manager(Some(name), pool.into(), $crate::connection::default_manager_settings())
        }

        fn install_manager(
            name: Option<&str>,
            pool: $pool,
            settings: $crate::ManagerSettings,
        ) -> Result<$pool, $crate::InstallError> {
            $crate::install::register(name, std::sync::Arc::new(manager_with_settings(pool.clone(), settings)))?;
            Ok(pool)
        }
    };
    (pool: $pool:ident, what: $what:literal, connect: $error:ident) => {
        install_functions! { pool: $pool, what: $what }

        /// Connects a pool with the given settings, creates its transaction manager with the
        /// transaction timeout of the settings, and installs it as the default manager.
        ///
        /// # Errors
        ///
        /// Returns an error when the pool cannot be connected, and when a default manager is
        /// already installed.
        pub async fn install_from(
            settings: &$crate::connection::PoolSettings,
        ) -> Result<$pool, $crate::SetupError<$error>> {
            connect_and_install(None, settings).await
        }

        /// Connects a pool with the given settings and installs its transaction manager under a
        /// name.
        ///
        /// # Errors
        ///
        /// Returns an error when the pool cannot be connected, and when a manager is already
        /// installed under the name.
        pub async fn install_from_as(
            name: &str,
            settings: &$crate::connection::PoolSettings,
        ) -> Result<$pool, $crate::SetupError<$error>> {
            connect_and_install(Some(name), settings).await
        }

        /// Reads the settings from the environment, connects a pool, and installs its transaction
        /// manager as the default manager.
        ///
        /// The variables are those of
        /// [`PoolSettings::from_env`]($crate::connection::PoolSettings::from_env): `DATABASE_URL` and
        /// the optional `DATABASE_MAX_CONNECTIONS`, `DATABASE_MIN_CONNECTIONS`,
        /// `DATABASE_ACQUIRE_TIMEOUT_SECONDS`, `DATABASE_IDLE_TIMEOUT_SECONDS` and
        /// `DATABASE_TRANSACTION_TIMEOUT_SECONDS`.
        ///
        /// # Errors
        ///
        /// Returns an error when a variable is missing or not valid, when the pool cannot be
        /// connected, and when a default manager is already installed.
        pub async fn install_from_env() -> Result<$pool, $crate::SetupError<$error>> {
            install_from(&$crate::connection::PoolSettings::from_env()?).await
        }

        async fn connect_and_install(
            name: Option<&str>,
            settings: &$crate::connection::PoolSettings,
        ) -> Result<$pool, $crate::SetupError<$error>> {
            let pool = <$pool>::connect(settings).await.map_err($crate::SetupError::Connect)?;
            Ok(install_manager(name, pool, settings.manager_settings())?)
        }
    };
}
