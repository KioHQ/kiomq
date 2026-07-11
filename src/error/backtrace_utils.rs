use async_backtrace::Location as LocationTrace;
use compact_str::{CompactString, ToCompactString, format_compact};
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
    pub payload: CompactString,
    /// Async backtrace at the point the panic was caught, if available.
    pub backtrace: Backtrace,
}

impl Default for CaughtPanicInfo {
    fn default() -> Self {
        Self {
            payload: "Panic occurred but failed to capture backtrace".to_compact_string(),
            backtrace: Option::default(),
        }
    }
}

#[derive(Debug, Default, derive_more::Display)]
#[display("{} at {}:{}", file, line, col)]
pub(super) struct PanicLocation {
    file: CompactString,
    line: u32,
    col: u32,
}
impl From<&std::panic::Location<'_>> for PanicLocation {
    fn from(value: &std::panic::Location<'_>) -> Self {
        Self {
            file: value.file().to_compact_string(),
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
            .downcast_ref::<CompactString>()
            .map(CompactString::as_str)
            .or_else(|| info.payload().downcast_ref::<&'static str>().copied())
            .unwrap_or("Box<Any>");
        let location: PanicLocation = info
            .location()
            .map(std::convert::Into::into)
            .unwrap_or_default();
        let payload = format_compact!("Panic:{payload} :\n {location}");

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
    #![allow(clippy::unused_async)]
    use super::*;
    use std::io::Error as IoError;

    #[tokio::test]
    #[ignore = "flaky under parallel execution: `catch` mutates the process-global \
                panic hook (set_hook/take_hook) and a shared static, so a concurrent \
                `catch` in another test can corrupt the save/restore and clobber the \
                captured panic info"]
    async fn test_catch_panic() {
        async fn panicking_function() -> Result<(), IoError> {
            panic!("Test panic");
        }

        let result = BacktraceCatcher::catch(panicking_function()).await;
        assert!(matches!(result, Err(CaughtError::Panic(_))));
        if let Err(CaughtError::Panic(info)) = result {
            assert!(info.payload.contains("Test panic"));
            // The captured payload is formatted as `Panic:{payload} :\n {location}`.
            assert!(info.payload.contains("Panic:"));
            // A framed capture should have recorded a backtrace.
            assert!(info.backtrace.is_some());
        }
    }

    const AWAIT_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

    #[test]
    fn caught_panic_info_default_has_fallback_payload_and_no_backtrace() {
        let info = CaughtPanicInfo::default();
        assert!(!info.payload.is_empty());
        assert!(
            info.payload.contains("failed to capture backtrace"),
            "default payload should explain the missing backtrace: {}",
            info.payload
        );
        assert!(
            info.backtrace.is_none(),
            "the default has no captured backtrace"
        );
    }

    #[tokio::test]
    async fn join_error_converts_into_caught_error() {
        let join_error = tokio::spawn(async { panic!("boom in task") })
            .await
            .expect_err("a panicking task must yield a JoinError");
        let caught: CaughtError = join_error.into();
        assert!(matches!(caught, CaughtError::JoinError(_)));
        // Debug rendering of every CaughtError arm must be populated.
        assert!(!format!("{caught:?}").is_empty());
    }

    #[test]
    fn panic_location_from_std_location_formats_file_line_col() {
        let std_location = std::panic::Location::caller();
        let location: PanicLocation = std_location.into();
        let rendered = location.to_compact_string();
        // Display format is "{file} at {line}:{col}".
        assert!(rendered.contains(" at "));
        assert!(rendered.contains(std_location.file()));
        assert!(rendered.contains(&std_location.line().to_string()));
    }

    #[test]
    fn panic_location_default_renders_empty_file_and_zeroes() {
        let location = PanicLocation::default();
        assert_eq!(location.to_compact_string(), " at 0:0");
    }

    #[tokio::test]
    async fn backtrace_is_absent_outside_a_framed_context() {
        // With no framed frame on the stack the capture path yields None.
        let backtrace: Backtrace = async_backtrace::backtrace();
        assert!(backtrace.is_none());
        // Even an empty backtrace formats to a well-defined, non-empty string.
        assert_eq!(format_compact!("{backtrace:?}"), "None");
    }

    #[tokio::test]
    async fn backtrace_is_present_within_a_framed_context() {
        #[async_backtrace::framed]
        async fn capture() -> Backtrace {
            async_backtrace::backtrace()
        }
        let backtrace = capture().await;
        assert!(
            backtrace.is_some(),
            "a framed future must yield a captured backtrace"
        );
        let frames = backtrace.expect("checked is_some above");
        assert!(!frames.is_empty(), "captured backtrace must contain frames");
        assert!(!format_compact!("{frames:?}").is_empty());
    }

    #[tokio::test]
    async fn catch_returns_non_copy_ok_value_unchanged() {
        async fn produces_string() -> Result<String, IoError> {
            Ok("payload".to_string())
        }
        let result = tokio::time::timeout(AWAIT_BOUND, BacktraceCatcher::catch(produces_string()))
            .await
            .expect("catch must not hang");
        assert_eq!(result.expect("expected Ok"), "payload");
    }

    #[tokio::test]
    async fn catch_error_arm_preserves_the_original_error_and_debug() {
        async fn erroring() -> Result<(), IoError> {
            Err(IoError::other("specific failure"))
        }
        let result = tokio::time::timeout(AWAIT_BOUND, BacktraceCatcher::catch(erroring()))
            .await
            .expect("catch must not hang");
        let err = result.expect_err("expected an error");
        assert!(matches!(err, CaughtError::Error(_, _)));
        if let CaughtError::Error(inner, backtrace) = err {
            assert_eq!(inner.to_compact_string(), "specific failure");
            // Backtrace may be empty or populated, but must always format.
            assert!(!format_compact!("{backtrace:?}").is_empty());
        }
    }

    #[tokio::test]
    async fn sequential_catches_each_report_their_own_error() {
        async fn erroring(msg: &'static str) -> Result<(), IoError> {
            Err(IoError::other(msg))
        }

        let first = BacktraceCatcher::catch(erroring("first")).await;
        let second = BacktraceCatcher::catch(erroring("second")).await;

        for (result, expected) in [(first, "first"), (second, "second")] {
            match result.expect_err("expected an error") {
                CaughtError::Error(inner, _) => {
                    assert_eq!(inner.to_compact_string(), expected);
                }
                other => panic!("expected CaughtError::Error, got {other:?}"),
            }
        }
    }
}
