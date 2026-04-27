//! Shared error-handling utilities for the harmonic scheduler

use std::error::Error;
use std::fmt::{Display, Formatter, Result};

/// Extension trait providing a `.chain()` method to render an [`Error`]'s full `source()` chain
///
/// ```ignore
/// warn!("operation failed: {}", err.chain());
/// ```
pub trait ErrorExt {
    fn chain(&self) -> impl Display + '_;
}

impl<E: Error + 'static> ErrorExt for E {
    fn chain(&self) -> impl Display + '_ {
        ErrorChain(self)
    }
}

// Blanket impl above requires `Sized`, so bare trait-object references need
// an explicit impl. Add further `dyn Error` variants here if new call sites need them.
impl ErrorExt for dyn Error + Send + Sync {
    fn chain(&self) -> impl Display + '_ {
        ErrorChain(self)
    }
}

struct ErrorChain<'a>(&'a (dyn Error + 'static));

impl Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)?;
        let mut cause = self.0.source();
        while let Some(err) = cause {
            write!(f, ": {err}")?;
            cause = err.source();
        }
        Ok(())
    }
}
