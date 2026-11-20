//! Error classification for retry policies.
//!
//! When several layers of middleware wrap the same failure, the concrete
//! error type produced by the origin service gets erased behind
//! [`BoxError`](crate::BoxError). Retry policies then have no way of telling
//! a transient failure (e.g. a connection reset worth retrying) apart from a
//! permanent one (e.g. an invalid argument or a permission denial that will
//! never succeed). The only available strategy is blind, count-based
//! retrying, which multiplies the load on downstream services with requests
//! that are guaranteed to fail again.
//!
//! This module provides a convention that travels through layers *without*
//! requiring every layer to define its own error type:
//!
//! - An origin service marks its concrete error by implementing
//!   [`Retryable`], or wraps any error in a [`ClassifiedError`].
//! - Middleware keeps wrapping errors the same way it does today (for
//!   example with `?`/`From` into [`BoxError`](crate::BoxError)). As long
//!   as the wrapper preserves [`std::error::Error::source`], the
//!   classification is retained.
//! - The retry policy calls [`classify`] on the error chain. It walks the
//!   `source` chain until it finds a [`ClassifiedError`] and returns the
//!   decision attached to it.
//!
//! Classification is only consulted on the error path, so a successful call
//! pays no additional cost.

use std::error::Error;
use std::fmt;

use crate::BoxError;

/// Marks an error as carrying a retry decision.
///
/// This is implemented by the *origin* error type. Middleware layers do not
/// need to implement it: they keep producing their own (typically
/// type-erased, e.g. [`BoxError`](crate::BoxError)) errors and simply
/// preserve the [`source`](std::error::Error::source) chain.
///
/// The default implementation marks every failure as retryable. An error
/// enum can override [`Retryable::is_retryable`] to make some of its
/// variants permanent:
///
/// ```
/// use tower::retry::classify::Retryable;
///
/// #[derive(Debug)]
/// enum UpstreamError {
///     /// The connection was reset; the request may succeed on retry.
///     ConnectionReset,
///     /// The request was rejected as invalid; retrying cannot help.
///     InvalidArgument,
///     /// The caller is not allowed to perform the request.
///     PermissionDenied,
/// }
///
/// impl std::fmt::Display for UpstreamError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "upstream error")
///     }
/// }
///
/// impl std::error::Error for UpstreamError {}
///
/// impl Retryable for UpstreamError {
///     fn is_retryable(&self) -> bool {
///         matches!(self, UpstreamError::ConnectionReset)
///     }
/// }
/// ```
pub trait Retryable: Error + 'static {
    /// Returns `true` if the operation that produced this error may succeed
    /// when retried, and `false` if the failure is permanent.
    fn is_retryable(&self) -> bool {
        true
    }
}

/// An error that carries an explicit retry decision through the middleware
/// stack.
///
/// The decision is preserved as long as surrounding wrappers keep the
/// [`source`](std::error::Error::source) chain intact, which the standard
/// wrappers ([`Box`]ed [`Error`]s, `anyhow`, `thiserror`, ...) do. A retry
/// policy recovers it with [`classify`].
///
/// A [`ClassifiedError`] can be built directly from any error value, or
/// produced automatically from an error that implements [`Retryable`]:
///
/// ```
/// use tower::retry::classify::ClassifiedError;
///
/// // A permanent failure, e.g. an invalid argument.
/// let err = ClassifiedError::permanent(Box::new(std::io::Error::new(
///     std::io::ErrorKind::InvalidInput,
///     "bad request",
/// )));
/// assert!(!err.is_retryable());
/// ```
#[derive(Debug)]
pub struct ClassifiedError {
    retryable: bool,
    source: BoxError,
}

impl ClassifiedError {
    /// Wraps `source` and marks it as retryable: the failure is considered
    /// transient and retrying the same request may succeed.
    pub fn retryable(source: BoxError) -> Self {
        ClassifiedError {
            retryable: true,
            source,
        }
    }

    /// Wraps `source` and marks it as non-retryable: the failure is permanent
    /// (e.g. an invalid argument or a permission denial) and retrying cannot
    /// help.
    pub fn permanent(source: BoxError) -> Self {
        ClassifiedError {
            retryable: false,
            source,
        }
    }

    /// The retry decision carried by this error.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// Returns a reference to the wrapped error.
    pub fn get_ref(&self) -> &(dyn Error + Send + Sync) {
        &*self.source
    }
}

impl fmt::Display for ClassifiedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}

impl Error for ClassifiedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.source)
    }
}

impl<E> From<E> for ClassifiedError
where
    E: Retryable + Send + Sync,
{
    fn from(source: E) -> Self {
        let retryable = source.is_retryable();
        ClassifiedError {
            retryable,
            source: Box::new(source),
        }
    }
}

/// Classifies an error by walking its [`source`](std::error::Error::source)
/// chain looking for a [`ClassifiedError`].
///
/// Returns:
///
/// - `Some(true)` if the first classification found marks the error as
///   retryable,
/// - `Some(false)` if the first classification found marks the error as
///   permanent,
/// - `None` if no layer attached a classification. The caller decides how to
///   treat unclassified errors (typically falling back to a retry budget or
///   attempt limit).
///
/// The outermost classification wins: a layer closer to the retry policy can
/// deliberately override the decision of an inner layer.
pub fn classify(error: &(dyn Error + 'static)) -> Option<bool> {
    let mut current: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(err) = current {
        if let Some(classified) = err.downcast_ref::<ClassifiedError>() {
            return Some(classified.is_retryable());
        }
        current = err.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[derive(Debug)]
    struct Origin {
        retryable: bool,
    }

    impl fmt::Display for Origin {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "origin error")
        }
    }

    impl Error for Origin {}

    impl Retryable for Origin {
        fn is_retryable(&self) -> bool {
            self.retryable
        }
    }

    #[derive(Debug)]
    struct MiddlewareError {
        source: BoxError,
    }

    impl fmt::Display for MiddlewareError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "middleware error")
        }
    }

    impl Error for MiddlewareError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.source.as_ref())
        }
    }

    #[test]
    fn classifies_retryable_marker() {
        let error = ClassifiedError::from(Origin { retryable: true });
        assert_eq!(classify(&error), Some(true));
    }

    #[test]
    fn classifies_permanent_marker() {
        let error = ClassifiedError::from(Origin { retryable: false });
        assert_eq!(classify(&error), Some(false));
    }

    #[test]
    fn classification_survives_rewrapping_layers() {
        // The origin produces a permanent, classified failure...
        let origin = ClassifiedError::permanent(Box::new(Origin { retryable: false }));
        // ...and several layers re-wrap it without defining any new error
        // type, just like `?` into `BoxError` would.
        let boxed: BoxError = Box::new(MiddlewareError {
            source: Box::new(origin),
        });
        let boxed: BoxError = Box::new(MiddlewareError { source: boxed });

        assert_eq!(classify(boxed.as_ref()), Some(false));
    }

    #[test]
    fn unclassified_errors_return_none() {
        let error = io::Error::new(io::ErrorKind::Other, "no classification");
        assert_eq!(classify(&error), None);
    }

    #[test]
    fn outer_classification_overrides_inner() {
        let inner = ClassifiedError::retryable(Box::new(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset",
        )));
        let outer = ClassifiedError::permanent(Box::new(inner));

        assert_eq!(classify(&outer), Some(false));
    }
}
