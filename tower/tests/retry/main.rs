#![cfg(feature = "retry")]
#[path = "../support.rs"]
mod support;

use std::future;

use tokio_test::{assert_pending, assert_ready_err, assert_ready_ok, task};
use tower::retry::{
    classify::{classify, ClassifiedError},
    Policy,
};
use tower_test::{assert_request_eq, mock};

#[tokio::test(flavor = "current_thread")]
async fn retry_errors() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(RetryErrors);

    assert_ready_ok!(service.poll_ready());

    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry me");

    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_response("world");

    assert_eq!(fut.into_inner().await.unwrap(), "world");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_limit() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(Limit(2));

    assert_ready_ok!(service.poll_ready());

    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("retry 2");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("retry 3");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "retry 3");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_error_inspection() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(UnlessErr("reject"));

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("reject");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "reject");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_cannot_clone_request() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(CannotClone);

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "retry 1");
}

#[tokio::test(flavor = "current_thread")]
async fn success_with_cannot_clone() {
    let _t = support::trace_init();

    // Even though the request couldn't be cloned, if the first request succeeds,
    // it should succeed overall.
    let (mut service, mut handle) = new_service(CannotClone);

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_response("world");
    assert_ready_ok!(fut.poll(), "world");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_mutating_policy() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(MutatingPolicy { remaining: 2 });

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_response("world");
    assert_pending!(fut.poll());
    // the policy alters the request. in real life, this might be setting a header
    assert_request_eq!(handle, "retrying").send_response("world");

    assert_pending!(fut.poll());

    assert_request_eq!(handle, "retrying").send_response("world");

    assert_ready_err!(fut.poll(), "out of retries");
}

#[tokio::test(flavor = "current_thread")]
async fn non_retryable_error_is_not_retried() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(ClassifiedRetry::new(2));

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    // The downstream failure is marked permanent (e.g. invalid argument or
    // permission denied) and re-wrapped by every layer in the stack.
    assert_request_eq!(handle, "hello").send_error(rewrap(ClassifiedError::permanent(Box::new(
        io_error("invalid argument"),
    ))));

    // The policy sees the reason through the wrapper chain and gives up
    // immediately: the request reaches the downstream service exactly once.
    let err = assert_ready_err!(fut.poll());
    assert_eq!(classify(err.as_ref()), Some(false));
    assert_pending!(handle.poll_request(), "no retry attempted");
}

#[tokio::test(flavor = "current_thread")]
async fn retryable_error_still_uses_the_attempt_limit() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(ClassifiedRetry::new(2));

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error(rewrap(ClassifiedError::retryable(Box::new(
        io_error("connection reset"),
    ))));
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error(rewrap(ClassifiedError::retryable(Box::new(
        io_error("connection reset"),
    ))));
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error(rewrap(ClassifiedError::retryable(Box::new(
        io_error("connection reset"),
    ))));
    let err = assert_ready_err!(fut.poll());
    assert_eq!(classify(err.as_ref()), Some(true));
}

#[tokio::test(flavor = "current_thread")]
async fn unclassified_error_falls_back_to_attempt_limit() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(ClassifiedRetry::new(1));

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    // An error without any classification is treated with the legacy
    // count-based behavior.
    assert_request_eq!(handle, "hello").send_error(rewrap(io_error("unclassified")));
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error(rewrap(io_error("unclassified")));
    assert_ready_err!(fut.poll());
}

type Req = &'static str;
type Res = &'static str;
type InnerError = &'static str;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Mock = mock::Mock<Req, Res>;
type Handle = mock::Handle<Req, Res>;

#[derive(Clone)]
struct RetryErrors;

impl Policy<Req, Res, Error> for RetryErrors {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if result.is_err() {
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct Limit(usize);

impl Policy<Req, Res, Error> for Limit {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if result.is_err() && self.0 > 0 {
            self.0 -= 1;
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct UnlessErr(InnerError);

impl Policy<Req, Res, Error> for UnlessErr {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        result.as_ref().err().and_then(|err| {
            if err.to_string() != self.0 {
                Some(future::ready(()))
            } else {
                None
            }
        })
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct CannotClone;

impl Policy<Req, Res, Error> for CannotClone {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, _: &mut Result<Res, Error>) -> Option<Self::Future> {
        unreachable!("retry cannot be called since request isn't cloned");
    }

    fn clone_request(&mut self, _req: &Req) -> Option<Req> {
        None
    }
}

/// Retry policy that makes its decision from the error's reason instead of
/// blindly counting attempts.
///
/// A non-retryable classification found anywhere in the error chain ends the
/// request session immediately. Retryable and unclassified errors are retried
/// until `remaining` attempts are used up.
#[derive(Clone)]
struct ClassifiedRetry {
    remaining: usize,
}

impl ClassifiedRetry {
    fn new(max_retries: usize) -> Self {
        ClassifiedRetry {
            remaining: max_retries,
        }
    }
}

impl Policy<Req, Res, Error> for ClassifiedRetry {
    type Future = future::Ready<()>;

    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        let err = result.as_ref().err()?;

        match classify(err.as_ref()) {
            // The reason is visible: permanent failures are never retried.
            Some(false) => None,
            // Transient failures consume an attempt like before.
            Some(true) if self.remaining == 0 => None,
            Some(true) => {
                self.remaining -= 1;
                Some(future::ready(()))
            }
            // No layer attached a classification; keep the legacy behavior.
            None if self.remaining == 0 => None,
            None => {
                self.remaining -= 1;
                Some(future::ready(()))
            }
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

/// Simulates a middleware layer re-wrapping an error the way `?` into a
/// `BoxError` would: the outer type stays the same, only the
/// `source` chain gains another link.
fn rewrap(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Box::new(Wrapped(Box::new(err)))
}

fn io_error(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

#[derive(Debug)]
struct Wrapped(Error);

impl std::fmt::Display for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "wrapped")
    }
}

impl std::error::Error for Wrapped {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Test policy that changes the request to `retrying` during retries and the result to `"out of retries"`
/// when retries are exhausted.
#[derive(Clone)]
struct MutatingPolicy {
    remaining: usize,
}

impl Policy<Req, Res, Error> for MutatingPolicy
where
    Error: From<&'static str>,
{
    type Future = future::Ready<()>;

    fn retry(&mut self, req: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if self.remaining == 0 {
            *result = Err("out of retries".into());
            None
        } else {
            *req = "retrying";
            self.remaining -= 1;
            Some(future::ready(()))
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

fn new_service<P: Policy<Req, Res, Error> + Clone>(
    policy: P,
) -> (mock::Spawn<tower::retry::Retry<P, Mock>>, Handle) {
    let retry = tower::retry::RetryLayer::new(policy);
    mock::spawn_layer(retry)
}
