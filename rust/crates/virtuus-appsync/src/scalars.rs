//! AWS AppSync custom scalars and their validators.
//!
//! Implements validators for AWS-specific scalar types used in AppSync schemas.

use regex::Regex;
use std::sync::OnceLock;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum ScalarValidationError {
    #[error("Invalid AWSDateTime format")]
    InvalidAWSDateTime,
    #[error("Invalid AWSDate format")]
    InvalidAWSDate,
    #[error("Invalid AWSTime format")]
    InvalidAWSTime,
    #[error("Invalid AWSTimestamp format")]
    InvalidAWSTimestamp,
    #[error("Invalid AWSJSON format")]
    InvalidAWSJSON,
    #[error("Invalid AWSEmail format")]
    InvalidAWSEmail,
    #[error("Invalid AWSURL format")]
    InvalidAWSURL,
    #[error("Invalid AWSPhone format")]
    InvalidAWSPhone,
    #[error("Invalid AWSIPAddress format")]
    InvalidAWSIPAddress,
}

/// Validator for AWS AppSync custom scalars.
pub struct AwsScalarValidator;

impl AwsScalarValidator {
    /// Validates an AWSDateTime value.
    /// Format: ISO 8601 datetime (e.g., "2026-09-24T12:34:56Z" or "2026-09-24T12:34:56+00:00")
    pub fn validate_aws_datetime(value: &str) -> Result<(), ScalarValidationError> {
        fn get_datetime_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| {
                Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
                    .unwrap()
            })
        }

        if get_datetime_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSDateTime)
        }
    }

    /// Validates an AWSDate value.
    /// Format: YYYY-MM-DD (e.g., "2026-09-24")
    pub fn validate_aws_date(value: &str) -> Result<(), ScalarValidationError> {
        fn get_date_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap())
        }

        if get_date_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSDate)
        }
    }

    /// Validates an AWSTime value.
    /// Format: HH:MM:SS with optional timezone (e.g., "12:34:56" or "12:34:56Z")
    pub fn validate_aws_time(value: &str) -> Result<(), ScalarValidationError> {
        fn get_time_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| Regex::new(r"^\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})?$").unwrap())
        }

        if get_time_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSTime)
        }
    }

    /// Validates an AWSTimestamp value.
    /// Format: Unix timestamp in seconds (numeric)
    pub fn validate_aws_timestamp(value: &str) -> Result<(), ScalarValidationError> {
        if value.parse::<i64>().is_ok() {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSTimestamp)
        }
    }

    /// Validates AWSJSON value.
    /// Must be valid JSON.
    pub fn validate_awsjson(value: &str) -> Result<(), ScalarValidationError> {
        if serde_json::from_str::<serde_json::Value>(value).is_ok() {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSJSON)
        }
    }

    /// Validates an AWSEmail value.
    /// Basic email format validation.
    pub fn validate_aws_email(value: &str) -> Result<(), ScalarValidationError> {
        fn get_email_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| {
                Regex::new(r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$").unwrap()
            })
        }

        if get_email_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSEmail)
        }
    }

    /// Validates an AWSURL value.
    /// Must start with http:// or https://
    pub fn validate_awsurl(value: &str) -> Result<(), ScalarValidationError> {
        fn get_url_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| Regex::new(r"^https?://[^\s]+$").unwrap())
        }

        if get_url_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSURL)
        }
    }

    /// Validates an AWSPhone value.
    /// Basic phone number format with optional formatting
    pub fn validate_aws_phone(value: &str) -> Result<(), ScalarValidationError> {
        fn get_phone_regex() -> &'static Regex {
            static REGEX: OnceLock<Regex> = OnceLock::new();
            REGEX.get_or_init(|| Regex::new(r"^\+?[0-9\s()+-]{9,}$").unwrap())
        }

        if get_phone_regex().is_match(value) {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSPhone)
        }
    }

    /// Validates an AWSIPAddress value.
    /// Must be a valid IPv4 or IPv6 address.
    pub fn validate_aws_ip_address(value: &str) -> Result<(), ScalarValidationError> {
        if value.parse::<std::net::IpAddr>().is_ok() {
            Ok(())
        } else {
            Err(ScalarValidationError::InvalidAWSIPAddress)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_aws_datetime() {
        assert!(AwsScalarValidator::validate_aws_datetime("2026-09-24T12:34:56Z").is_ok());
        assert!(AwsScalarValidator::validate_aws_datetime("2026-09-24T12:34:56+00:00").is_ok());
        assert!(AwsScalarValidator::validate_aws_datetime("2026-09-24T12:34:56.123Z").is_ok());
        assert!(AwsScalarValidator::validate_aws_datetime("not-a-date").is_err());
    }

    #[test]
    fn test_validate_aws_date() {
        assert!(AwsScalarValidator::validate_aws_date("2026-09-24").is_ok());
        assert!(AwsScalarValidator::validate_aws_date("2026/09/24").is_err());
    }

    #[test]
    fn test_validate_aws_email() {
        assert!(AwsScalarValidator::validate_aws_email("test@example.com").is_ok());
        assert!(AwsScalarValidator::validate_aws_email("not-an-email").is_err());
    }

    #[test]
    fn test_validate_awsurl() {
        assert!(AwsScalarValidator::validate_awsurl("https://example.com").is_ok());
        assert!(AwsScalarValidator::validate_awsurl("http://example.com").is_ok());
        assert!(AwsScalarValidator::validate_awsurl("not-a-url").is_err());
    }

    #[test]
    fn test_validate_aws_time() {
        assert!(AwsScalarValidator::validate_aws_time("12:34:56").is_ok());
        assert!(AwsScalarValidator::validate_aws_time("12:34:56Z").is_ok());
        assert!(AwsScalarValidator::validate_aws_time("12:34:56+00:00").is_ok());
        assert!(AwsScalarValidator::validate_aws_time("not-a-time").is_err());
    }

    #[test]
    fn test_validate_aws_timestamp() {
        assert!(AwsScalarValidator::validate_aws_timestamp("1234567890").is_ok());
        assert!(AwsScalarValidator::validate_aws_timestamp("0").is_ok());
        assert!(AwsScalarValidator::validate_aws_timestamp("not-a-timestamp").is_err());
    }

    #[test]
    fn test_validate_awsjson() {
        assert!(AwsScalarValidator::validate_awsjson("{}").is_ok());
        assert!(AwsScalarValidator::validate_awsjson("{\"key\": \"value\"}").is_ok());
        assert!(AwsScalarValidator::validate_awsjson("[]").is_ok());
        assert!(AwsScalarValidator::validate_awsjson("not-json").is_err());
    }

    #[test]
    fn test_validate_aws_phone() {
        assert!(AwsScalarValidator::validate_aws_phone("+1234567890").is_ok());
        assert!(AwsScalarValidator::validate_aws_phone("+1 (234) 567-8900").is_ok());
        assert!(AwsScalarValidator::validate_aws_phone("not-a-phone").is_err());
    }

    #[test]
    fn test_validate_aws_ip_address() {
        assert!(AwsScalarValidator::validate_aws_ip_address("192.168.1.1").is_ok());
        assert!(AwsScalarValidator::validate_aws_ip_address("::1").is_ok());
        assert!(AwsScalarValidator::validate_aws_ip_address("2001:db8::8a2e:370:7334").is_ok());
        assert!(AwsScalarValidator::validate_aws_ip_address("not-an-ip").is_err());
        assert!(AwsScalarValidator::validate_aws_ip_address("256.1.1.1").is_err());
    }
}
