//! The router: the request path from a pushed body to a chosen endpoint.
//!
//! [`service::KinferenceService`] is built once from the server's settings
//! and the fleet's handles; [`grpc`] exposes it as the `Picker` service. A
//! pick is five typed steps in `service::lifecycle`, and only its commit
//! writes state. The picker is pure; `grpc` holds no logic.
//!
//! - [`body_store`][]: request bodies held between `PushBody` and `PullBody`,
//!   with byte budgets and a TTL.
//! - [`error`][]: the router's error type.
//! - [`grpc`][]: the `Picker` handlers; maps proto types to service calls.
//! - [`metrics`][]: `RouterMetrics` and the tenant label policy.
//! - [`observations`][]: per-pick state kept until the caller reports, and the
//!   CSV sink.
//! - [`pick_body`][]: rewrites of the request body that `PullBody` serves.
//! - [`picker`][]: the wait-time predictor and the top-K picker.
//! - [`proto`][]: the generated tonic types for `kinference.v1`.
//! - [`service`][]: the service and its parts: scoring, the pick lifecycle,
//!   the policy driver, bodies and observations.
//! - [`tokenizer`][]: the `Tokenizer` trait and its backends: in-process,
//!   vLLM's `/render`, and the comparison of the two.

pub mod body_store;
pub mod error;
pub mod grpc;
pub mod metrics;
pub mod observations;
pub mod pick_body;
pub mod picker;
pub mod proto;
pub mod service;
pub mod tokenizer;

pub use error::Error;
