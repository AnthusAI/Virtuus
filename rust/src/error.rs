use std::error::Error as StdError;
use std::fmt;

/// Errors that can be returned from Virtuus operations.
#[derive(Debug, Clone)]
pub enum Error {
    /// A record or resource was not found.
    NotFound {
        /// Description of what was not found.
        message: String,
    },
    /// A conditional check failed during an update operation.
    ConditionalCheckFailed {
        /// Description of the failed condition.
        message: String,
    },
    /// A validation error occurred.
    Validation {
        /// The validation error message.
        message: String,
    },
    /// An unknown table was referenced.
    UnknownTable {
        /// The name of the table.
        name: String,
    },
    /// An unknown index was referenced.
    UnknownIndex {
        /// The name of the table.
        table: String,
        /// The name of the index.
        name: String,
    },
    /// An invalid token was provided.
    InvalidToken {
        /// The reason the token is invalid.
        reason: String,
    },
    /// An IO error occurred.
    Io {
        /// The path involved in the error.
        path: String,
        /// The error message.
        message: String,
    },
    /// A parse error occurred.
    Parse {
        /// The path of the file being parsed.
        path: String,
        /// The error message.
        message: String,
    },
    /// A database lock could not be acquired.
    Locked {
        /// Description of the lock.
        message: String,
    },
}

#[cfg(not(tarpaulin_include))]
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound { message } => write!(f, "NotFound: {}", message),
            Error::ConditionalCheckFailed { message } => {
                write!(f, "ConditionalCheckFailed: {}", message)
            }
            Error::Validation { message } => write!(f, "Validation: {}", message),
            Error::UnknownTable { name } => write!(f, "UnknownTable: {}", name),
            Error::UnknownIndex { table, name } => write!(f, "UnknownIndex: {} on {}", name, table),
            Error::InvalidToken { reason } => write!(f, "InvalidToken: {}", reason),
            Error::Io { path, message } => write!(f, "Io: {}: {}", path, message),
            Error::Parse { path, message } => write!(f, "Parse: {}: {}", path, message),
            Error::Locked { message } => write!(f, "Locked: {}", message),
        }
    }
}

#[cfg(not(tarpaulin_include))]
impl StdError for Error {}

#[cfg(not(tarpaulin_include))]
impl From<String> for Error {
    fn from(message: String) -> Self {
        Error::Validation { message }
    }
}

#[cfg(not(tarpaulin_include))]
impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Error::Validation {
            message: message.to_string(),
        }
    }
}

/// A specialized result type for Virtuus operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_display() {
        let err = Error::NotFound {
            message: "user 123 not found".to_string(),
        };
        assert_eq!(err.to_string(), "NotFound: user 123 not found");
    }

    #[test]
    fn test_conditional_check_failed_display() {
        let err = Error::ConditionalCheckFailed {
            message: "version mismatch".to_string(),
        };
        assert_eq!(err.to_string(), "ConditionalCheckFailed: version mismatch");
    }

    #[test]
    fn test_validation_display() {
        let err = Error::Validation {
            message: "invalid field".to_string(),
        };
        assert_eq!(err.to_string(), "Validation: invalid field");
    }

    #[test]
    fn test_unknown_table_display() {
        let err = Error::UnknownTable {
            name: "users".to_string(),
        };
        assert_eq!(err.to_string(), "UnknownTable: users");
    }

    #[test]
    fn test_unknown_index_display() {
        let err = Error::UnknownIndex {
            table: "users".to_string(),
            name: "by_email".to_string(),
        };
        assert_eq!(err.to_string(), "UnknownIndex: by_email on users");
    }

    #[test]
    fn test_invalid_token_display() {
        let err = Error::InvalidToken {
            reason: "malformed".to_string(),
        };
        assert_eq!(err.to_string(), "InvalidToken: malformed");
    }

    #[test]
    fn test_io_display() {
        let err = Error::Io {
            path: "/path/to/file".to_string(),
            message: "file not found".to_string(),
        };
        assert_eq!(err.to_string(), "Io: /path/to/file: file not found");
    }

    #[test]
    fn test_parse_display() {
        let err = Error::Parse {
            path: "config.json".to_string(),
            message: "invalid json".to_string(),
        };
        assert_eq!(err.to_string(), "Parse: config.json: invalid json");
    }

    #[test]
    fn test_locked_display() {
        let err = Error::Locked {
            message: "database locked".to_string(),
        };
        assert_eq!(err.to_string(), "Locked: database locked");
    }

    #[test]
    fn test_from_string() {
        let err = Error::from("validation error".to_string());
        assert_eq!(err.to_string(), "Validation: validation error");
    }

    #[test]
    fn test_from_str() {
        let err = Error::from("validation error");
        assert_eq!(err.to_string(), "Validation: validation error");
    }
}
