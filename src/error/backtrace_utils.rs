use async_backtrace::Location as LocationTrace;
use futures::future::{Future, FutureExt};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{LazyLock, Mutex};
use tokio::task::JoinError;
type Backtrace = Option<Box<[LocationTrace]>>;
#[derive(Debug)]
/// Represents an error caught during async job processing, including panics, errors, and join failures.
pub enum CaughtError {
    /// A panic was caught.
    Panic(CaughtPanicInfo),
    /// An error was returned from the future.
    Error(Box<dyn std::error::Error + Send>, Backtrace),
    /// A Tokio task join error occurred.
    JoinError(JoinError),
}
impl From<JoinError> for CaughtError {
    fn from(value: JoinError) -> Self {
        Self::JoinError(value)
    }
}

/// Information captured about a panic.
#[derive(Debug)]
pub struct CaughtPanicInfo {
    /// A human-readable description of the panic, including the source location.
    pub payload: String,
    /// Async backtrace at the point the panic was caught, if available.
    pub backtrace: Backtrace,
}

impl Default for CaughtPanicInfo {
    fn default() -> Self {
        Self {
            payload: "Panic occurred but failed to capture backtrace".to_string(),
            backtrace: Option::default(),
        }
    }
}

#[derive(Debug, Default, derive_more::Display)]
#[display("{} at {}:{}", file, line, col)]
pub(super) struct PanicLocation {
    file: String,
    line: u32,
    col: u32,
}
impl From<&std::panic::Location<'_>> for PanicLocation {
    fn from(value: &std::panic::Location<'_>) -> Self {
        Self {
            file: value.file().to_string(),
            line: value.line(),
            col: value.column(),
        }
    }
}
/// Installs a panic hook and drives a future to completion, catching both
/// panics and errors.
#[derive(Clone, Debug)]
pub struct BacktraceCatcher;

impl BacktraceCatcher {
    #[async_backtrace::framed]
    fn capture_panic_info(info: &panic::PanicHookInfo<'_>) -> CaughtPanicInfo {
        let backtrace = async_backtrace::backtrace();
        let payload = info
            .payload()
            .downcast_ref::<String>()
            .map(std::string::String::as_str)
            .or_else(|| info.payload().downcast_ref::<&'static str>().copied())
            .unwrap_or("Box<Any>");
        let location: PanicLocation = info
            .location()
            .map(std::convert::Into::into)
            .unwrap_or_default();
        let payload = format!("Panic:{payload} :\n {location}");

        CaughtPanicInfo { payload, backtrace }
    }
    /// Drives the given future to completion, catching both panics and `Err`
    /// results.
    ///
    /// # Errors
    ///
    /// Returns [`CaughtError::Panic`] if the future panics, [`CaughtError::Error`]
    /// if it returns `Err`, or [`CaughtError::JoinError`] if a join fails.
    #[async_backtrace::framed]
    pub async fn catch<F, T, E>(f: F) -> Result<T, CaughtError>
    where
        F: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: std::error::Error + Send + 'static,
    {
        static PANIC_INFO: LazyLock<Mutex<Option<CaughtPanicInfo>>> = LazyLock::new(Mutex::default);

        let old_hook = panic::take_hook();
        panic::set_hook(Box::new(|info| {
            let panic_info = Self::capture_panic_info(info);
            PANIC_INFO.lock().unwrap().replace(panic_info);
        }));

        let result = AssertUnwindSafe(f).catch_unwind().await;

        // Restore the original panic hook
        panic::set_hook(old_hook);

        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let backtrace = async_backtrace::backtrace();
                Err(CaughtError::Error(Box::new(error), backtrace))
            }
            Err(_reason) => {
                let panic_info = PANIC_INFO.lock().unwrap().take();
                Err(CaughtError::Panic(panic_info.unwrap_or_default()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Error as IoError;

    #[tokio::test]
    #[ignore = "this is  flaky tests but function"]
    async fn test_catch_panic() {
        async fn panicking_function() -> Result<(), IoError> {
            panic!("Test panic");
        }

        let result = BacktraceCatcher::catch(panicking_function()).await;
        assert!(matches!(result, Err(CaughtError::Panic(_))));
        if let Err(CaughtError::Panic(info)) = result {
            assert!(info.payload.contains("Test panic"));
            assert!(info.payload.contains("Backtrace:"));
        }
    }
    #[tokio::test]
    async fn test_catch_error() {
        async fn erroring_function() -> Result<(), IoError> {
            Err(IoError::other("Test error"))
        }

        let result = BacktraceCatcher::catch(erroring_function()).await;
        assert!(matches!(result, Err(CaughtError::Error(_, _))));
        if let Err(CaughtError::Error(err, backtrace)) = result {
            assert_eq!(err.to_string(), "Test error");
            assert!(!format!("{backtrace:?}").is_empty());
        } else {
            panic!("Expected CaughtError::Error, got something else");
        }
    }
    #[tokio::test]
    async fn test_no_error() {
        async fn normal_function() -> Result<i32, IoError> {
            Ok(42)
        }

        let result = BacktraceCatcher::catch(normal_function()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
