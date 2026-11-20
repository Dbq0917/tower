#![cfg(all(
    feature = "buffer",
    feature = "limit",
    feature = "load-shed",
    feature = "retry"
))]
mod support;
use futures_util::pin_mut;
use std::{future::Ready, time::Duration};
use tokio_test::{assert_pending, assert_ready_err, assert_ready_ok, task};
use tower::builder::ServiceBuilder;
use tower::load_shed::error::Overloaded;
use tower::retry::Policy;
use tower::util::ServiceExt;
use tower_service::*;
use tower_test::{assert_request_eq, mock};

#[tokio::test(flavor = "current_thread")]
async fn builder_service() {
    let _t = support::trace_init();

    let (service, handle) = mock::pair();
    pin_mut!(handle);

    let policy = MockPolicy::<&'static str, bool>::default();
    let mut client = ServiceBuilder::new()
        .buffer(5)
        .concurrency_limit(5)
        .rate_limit(5, Duration::from_secs(5))
        .retry(policy)
        .map_response(|r: &'static str| r == "world")
        .map_request(|r: &'static str| r == "hello")
        .service(service);

    // allow a request through
    handle.allow(1);

    let fut = client.ready().await.unwrap().call("hello");
    assert_request_eq!(handle, true).send_response("world");
    assert!(fut.await.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn slow_readiness_is_not_admitted_into_stack() {
    let _t = support::trace_init();

    let (service, mut handle) = mock::pair();
    let mut first = ServiceBuilder::new()
        .load_shed()
        .concurrency_limit(1)
        .service(service);
    let mut second = first.clone();

    // The inner service is still warming up. `LoadShed` reports readiness so
    // the caller can make a decision immediately, but the request must be
    // rejected before it reaches the inner service.
    handle.allow(0);
    let ready = first.ready().await.unwrap();
    let mut response = task::spawn(ready.call("hello"));
    let err = assert_ready_err!(response.poll());
    assert!(err.is::<Overloaded>());
    assert_pending!(handle.poll_request());

    // Shedding the request must not consume the only concurrency permit, so a
    // second clone can proceed as soon as the inner service is ready.
    handle.allow(1);
    let ready = tokio::time::timeout(Duration::from_secs(1), second.ready())
        .await
        .expect("waiting for the second clone's readiness must be bounded")
        .unwrap();
    let mut response = task::spawn(ready.call("hello"));

    assert_request_eq!(handle, "hello").send_response("world");
    assert_eq!(assert_ready_ok!(response.poll()), "world");
}

#[derive(Debug, Clone, Default)]
struct MockPolicy<Req, Res> {
    _pd: std::marker::PhantomData<(Req, Res)>,
}

impl<Req, Res, E> Policy<Req, Res, E> for MockPolicy<Req, Res>
where
    Req: Clone,
    E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
{
    type Future = Ready<()>;

    fn retry(&mut self, _req: &mut Req, _result: &mut Result<Res, E>) -> Option<Self::Future> {
        None
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}
