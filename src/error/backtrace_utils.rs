use futures::future::{Future, FutureExt};
use std::cell::RefCell;
use std::error::Error;
use std::panic::Location;
use std::panic::{self, AssertUnwindSafe};
use std::sync::LazyLock;
use std::sync::Mutex;

use async_backtrace::Location as LocationTrace;
type Backtrace = Option<Box<[LocationTrace]>>;

#[derive(Debug)]
pub enum CaughtError {
    Panic(CaughtPanicInfo),
    Error(Box<dyn Error + Send>, Backtrace),
}
#[derive(Debug)]
pub struct CaughtPanicInfo {
    pub payload: String,
    pub location: PanicLocation,
    pub backtrace: Backtrace,
}
impl CaughtPanicInfo {
    pub fn contains(&self, sub_str: &str) -> bool {
        self.payload.contains(sub_str)
    }
}

impl Default for CaughtPanicInfo {
    fn default() -> Self {
        Self {
            payload: "Panic occurred but failed to capture backtrace".to_string(),
            location: Default::default(),
            backtrace: Default::default(),
        }
    }
}

#[derive(Debug, Default, derive_more::Display)]
#[display("{} at {}:{}", file, line, col)]
pub struct PanicLocation {
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
#[derive(Clone)]
pub struct BacktraceCatcher;

impl BacktraceCatcher {
    #[async_backtrace::framed]
    fn capture_panic_info(info: &panic::PanicHookInfo<'_>) -> CaughtPanicInfo {
        let backtrace = async_backtrace::backtrace();
        let payload = info
            .payload()
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| info.payload().downcast_ref::<&'static str>().copied())
            .unwrap_or("Box<Any>");
        let mut location = info.location().map(|l| l.into()).unwrap_or_default();

        let payload = format!("Panic:{payload} :\n {location}");

        CaughtPanicInfo {
            payload: payload.to_owned(),
            location,
            backtrace,
        }
    }
    #[async_backtrace::framed]
    pub async fn catch<F, T, E>(f: F) -> Result<T, CaughtError>
    where
        F: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: Error + Send + 'static,
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
            Err(reason) => {
                let panic_info = PANIC_INFO.lock().unwrap().take();
                Err(CaughtError::Panic(panic_info.unwrap_or_default()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Error as IoError, ErrorKind},
        time::Duration,
    };

    #[tokio::test]
    #[ignore]
    async fn test_catch_panic() {
        async fn panicking_function() -> Result<(), IoError> {
            panic!("Test panic");
        }

        let result = BacktraceCatcher::catch(panicking_function()).await;
        assert!(matches!(result, Err(CaughtError::Panic(_))));
        if let Err(CaughtError::Panic(info)) = result {
            assert!(info.contains("Test panic"));
            assert!(info.contains("Backtrace:"));
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
            assert!(!format!("{:?}", backtrace).is_empty());
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
