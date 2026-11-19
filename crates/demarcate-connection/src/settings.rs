use std::fmt;
use std::time::Duration;

use demarcate_core::ManagerSettings;

/// The settings of the manager that [`manager`](crate::manager) creates:
/// [`ManagerSettings::default`] with nested transactions allowed.
pub fn default_manager_settings() -> ManagerSettings {
    ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() }
}

const MAX_CONNECTIONS: u32 = 100_000;
const MAX_SECONDS: u32 = 365 * 24 * 60 * 60;

/// The settings of one connection pool and of its transaction manager.
///
/// Create them in code with [`new`](PoolSettings::new), or read them from the environment with
/// [`from_env`](PoolSettings::from_env). A setting that is not given keeps the default of the
/// connection library.
///
/// The values that are set in code are not checked: they go to the connection library as they
/// are, and a value that the library does not accept, such as a pool of zero connections, fails
/// there.
///
/// The `Debug` output does not contain the URL, because a URL can contain a password.
#[derive(Clone, PartialEq, Eq)]
pub struct PoolSettings {
    url: String,
    max_connections: Option<u32>,
    min_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    transaction_timeout: Option<Duration>,
}

impl fmt::Debug for PoolSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolSettings")
            .field("url", &"<redacted>")
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("idle_timeout", &self.idle_timeout)
            .field("transaction_timeout", &self.transaction_timeout)
            .finish()
    }
}

impl PoolSettings {
    /// Creates settings for the database at the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: None,
            min_connections: None,
            acquire_timeout: None,
            idle_timeout: None,
            transaction_timeout: None,
        }
    }

    /// Reads the settings from the environment variables of the process.
    ///
    /// | Variable | Meaning |
    /// |:--|:--|
    /// | `DATABASE_URL` | The URL of the database. Required. |
    /// | `DATABASE_MAX_CONNECTIONS` | The largest number of connections in the pool. |
    /// | `DATABASE_MIN_CONNECTIONS` | The number of connections that the pool keeps open. |
    /// | `DATABASE_ACQUIRE_TIMEOUT_SECONDS` | How long a caller waits for a connection. |
    /// | `DATABASE_IDLE_TIMEOUT_SECONDS` | How long an unused connection stays open. |
    /// | `DATABASE_TRANSACTION_TIMEOUT_SECONDS` | The timeout of a transaction that declares none. |
    ///
    /// # Errors
    ///
    /// Returns an error when `DATABASE_URL` is not set, is empty or is not Unicode. Returns an error
    /// when a number of connections is not a whole number from 1 to 100000
    /// (`DATABASE_MIN_CONNECTIONS` can be 0), and when a number of seconds is not a whole number
    /// from 1 to 31536000, one year. An optional variable that is empty counts as not set. The
    /// error names the variable and never contains its value.
    pub fn from_env() -> Result<Self, SettingsError> {
        Self::from_lookup("", |name| std::env::var(name).ok())
    }

    /// Reads the settings of a second database from environment variables with a prefix.
    ///
    /// With the prefix `REPORTING`, the variables are `REPORTING_DATABASE_URL`,
    /// `REPORTING_DATABASE_MAX_CONNECTIONS` and so on.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`from_env`](PoolSettings::from_env).
    pub fn from_env_with_prefix(prefix: &str) -> Result<Self, SettingsError> {
        Self::from_lookup(prefix, |name| std::env::var(name).ok())
    }

    /// Reads the settings from the given source of variables instead of the environment.
    ///
    /// `lookup` returns the value of a variable, or `None` when it is not set. Use it to read the
    /// settings from a configuration file or a secret store that the application already has.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`from_env`](PoolSettings::from_env).
    pub fn from_lookup(prefix: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, SettingsError> {
        let name = |suffix: &str| {
            if prefix.is_empty() { format!("DATABASE_{suffix}") } else { format!("{prefix}_DATABASE_{suffix}") }
        };
        let value = |suffix: &str| {
            let variable = name(suffix);
            let value = lookup(&variable).map(|value| value.trim().to_owned()).filter(|value| !value.is_empty());
            (variable, value)
        };
        let number = |suffix: &str, minimum: u32, maximum: u32| -> Result<Option<u32>, SettingsError> {
            let (variable, value) = value(suffix);
            let Some(value) = value else { return Ok(None) };
            match value.parse::<u32>() {
                Ok(number) if (minimum..=maximum).contains(&number) => Ok(Some(number)),
                _ => Err(SettingsError::Invalid { variable, minimum, maximum }),
            }
        };
        let connections = |suffix: &str, minimum: u32| number(suffix, minimum, MAX_CONNECTIONS);
        let seconds = |suffix: &str| {
            number(suffix, 1, MAX_SECONDS).map(|seconds| seconds.map(|seconds| Duration::from_secs(seconds.into())))
        };

        let (url_variable, url) = value("URL");
        let url = url.ok_or(SettingsError::Missing { variable: url_variable })?;
        Ok(Self {
            url,
            max_connections: connections("MAX_CONNECTIONS", 1)?,
            min_connections: connections("MIN_CONNECTIONS", 0)?,
            acquire_timeout: seconds("ACQUIRE_TIMEOUT_SECONDS")?,
            idle_timeout: seconds("IDLE_TIMEOUT_SECONDS")?,
            transaction_timeout: seconds("TRANSACTION_TIMEOUT_SECONDS")?,
        })
    }

    /// Sets the largest number of connections in the pool.
    #[must_use]
    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    /// Sets the number of connections that the pool keeps open.
    #[must_use]
    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = Some(min_connections);
        self
    }

    /// Sets how long a caller waits for a connection.
    #[must_use]
    pub fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = Some(acquire_timeout);
        self
    }

    /// Sets how long an unused connection stays open.
    #[must_use]
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    /// Sets the timeout of a transaction that declares none.
    #[must_use]
    pub fn transaction_timeout(mut self, transaction_timeout: Duration) -> Self {
        self.transaction_timeout = Some(transaction_timeout);
        self
    }

    /// Returns the URL of the database. It can contain a password: do not log it.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the largest number of connections in the pool, when one is set.
    pub fn get_max_connections(&self) -> Option<u32> {
        self.max_connections
    }

    /// Returns the number of connections that the pool keeps open, when one is set.
    pub fn get_min_connections(&self) -> Option<u32> {
        self.min_connections
    }

    /// Returns how long a caller waits for a connection, when a time is set.
    pub fn get_acquire_timeout(&self) -> Option<Duration> {
        self.acquire_timeout
    }

    /// Returns how long an unused connection stays open, when a time is set.
    pub fn get_idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    /// Returns the timeout of a transaction that declares none, when one is set.
    pub fn get_transaction_timeout(&self) -> Option<Duration> {
        self.transaction_timeout
    }

    /// Returns the settings of the transaction manager for this pool:
    /// [`default_manager_settings`] with the transaction timeout of these settings.
    pub fn manager_settings(&self) -> ManagerSettings {
        ManagerSettings { default_timeout: self.transaction_timeout, ..default_manager_settings() }
    }
}

/// A setting that could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SettingsError {
    /// A required variable is not set, or is empty.
    Missing {
        /// The name of the variable.
        variable: String,
    },
    /// A variable is set to a value that is not a whole number in the allowed range.
    Invalid {
        /// The name of the variable.
        variable: String,
        /// The smallest value that is allowed.
        minimum: u32,
        /// The largest value that is allowed.
        maximum: u32,
    },
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { variable } => write!(f, "{variable} must be set"),
            Self::Invalid { variable, minimum, maximum } => {
                write!(f, "{variable} must be a whole number from {minimum} to {maximum}")
            }
        }
    }
}

impl std::error::Error for SettingsError {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn read(prefix: &str, variables: &[(&str, &str)]) -> Result<PoolSettings, SettingsError> {
        let variables: HashMap<String, String> =
            variables.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect();
        PoolSettings::from_lookup(prefix, |name| variables.get(name).cloned())
    }

    #[test]
    fn reads_every_variable() {
        let settings = read(
            "",
            &[
                ("DATABASE_URL", "postgres://app:secret@db/orders"),
                ("DATABASE_MAX_CONNECTIONS", "20"),
                ("DATABASE_MIN_CONNECTIONS", "0"),
                ("DATABASE_ACQUIRE_TIMEOUT_SECONDS", "5"),
                ("DATABASE_IDLE_TIMEOUT_SECONDS", "600"),
                ("DATABASE_TRANSACTION_TIMEOUT_SECONDS", "30"),
            ],
        )
        .unwrap();

        assert_eq!(settings.url(), "postgres://app:secret@db/orders");
        assert_eq!(settings.get_max_connections(), Some(20));
        assert_eq!(settings.get_min_connections(), Some(0));
        assert_eq!(settings.get_acquire_timeout(), Some(Duration::from_secs(5)));
        assert_eq!(settings.get_idle_timeout(), Some(Duration::from_secs(600)));
        assert_eq!(settings.get_transaction_timeout(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn only_the_url_is_required() {
        let settings = read("", &[("DATABASE_URL", "sqlite:orders.db")]).unwrap();

        assert_eq!(settings, PoolSettings::new("sqlite:orders.db"));
    }

    #[test]
    fn a_missing_or_empty_url_names_the_variable() {
        let missing = SettingsError::Missing { variable: "DATABASE_URL".into() };

        assert_eq!(read("", &[]), Err(missing.clone()));
        assert_eq!(read("", &[("DATABASE_URL", "  ")]), Err(missing));
    }

    #[test]
    fn a_prefix_selects_the_variables_of_a_second_database() {
        let variables = [("DATABASE_URL", "postgres://db/orders"), ("REPORTING_DATABASE_URL", "postgres://db/reports")];

        assert_eq!(read("REPORTING", &variables).unwrap().url(), "postgres://db/reports");
        assert_eq!(read("AUDIT", &variables), Err(SettingsError::Missing { variable: "AUDIT_DATABASE_URL".into() }));
    }

    #[test]
    fn a_value_that_is_not_an_allowed_number_is_rejected() {
        for (variable, value, minimum, maximum) in [
            ("DATABASE_MAX_CONNECTIONS", "0", 1, 100_000),
            ("DATABASE_MAX_CONNECTIONS", "many", 1, 100_000),
            ("DATABASE_MAX_CONNECTIONS", "100001", 1, 100_000),
            ("DATABASE_MIN_CONNECTIONS", "-1", 0, 100_000),
            ("DATABASE_ACQUIRE_TIMEOUT_SECONDS", "0", 1, 31_536_000),
            ("DATABASE_ACQUIRE_TIMEOUT_SECONDS", "18446744073709551615", 1, 31_536_000),
            ("DATABASE_IDLE_TIMEOUT_SECONDS", "1.5", 1, 31_536_000),
            ("DATABASE_TRANSACTION_TIMEOUT_SECONDS", "30s", 1, 31_536_000),
            ("DATABASE_TRANSACTION_TIMEOUT_SECONDS", "31536001", 1, 31_536_000),
        ] {
            assert_eq!(
                read("", &[("DATABASE_URL", "postgres://db/orders"), (variable, value)]),
                Err(SettingsError::Invalid { variable: variable.into(), minimum, maximum }),
                "{variable}={value}"
            );
        }
    }

    #[test]
    fn the_largest_values_are_accepted_and_an_empty_optional_variable_is_not_set() {
        let settings = read(
            "",
            &[
                ("DATABASE_URL", "postgres://db/orders"),
                ("DATABASE_MAX_CONNECTIONS", "100000"),
                ("DATABASE_MIN_CONNECTIONS", " "),
                ("DATABASE_TRANSACTION_TIMEOUT_SECONDS", "31536000"),
            ],
        )
        .unwrap();

        assert_eq!(settings.get_max_connections(), Some(100_000));
        assert_eq!(settings.get_min_connections(), None);
        assert_eq!(settings.get_transaction_timeout(), Some(Duration::from_secs(31_536_000)));
    }

    #[test]
    fn the_debug_output_and_the_errors_do_not_contain_the_url() {
        let settings = PoolSettings::new("postgres://app:secret@db/orders");
        let error =
            read("", &[("DATABASE_URL", "postgres://app:secret@db/orders"), ("DATABASE_MAX_CONNECTIONS", "secret")])
                .unwrap_err();

        assert!(!format!("{settings:?}").contains("secret"));
        assert!(!format!("{error} {error:?}").contains("secret"));
    }

    #[test]
    fn the_manager_settings_keep_nested_transactions_and_carry_the_timeout() {
        let settings = PoolSettings::new("postgres://db/orders").transaction_timeout(Duration::from_secs(30));

        assert_eq!(
            settings.manager_settings(),
            ManagerSettings {
                nested_transaction_allowed: true,
                default_timeout: Some(Duration::from_secs(30)),
                ..ManagerSettings::default()
            }
        );
        assert_eq!(PoolSettings::new("postgres://db/orders").manager_settings(), default_manager_settings());
    }
}
