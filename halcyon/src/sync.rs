//! Synchronization-primitive shim so the pool can be model-checked with `loom`.
//!
//! Under `--cfg loom` these resolve to `loom`'s instrumented `Mutex`/`Condvar`/
//! `thread`; otherwise to `std`. `Arc` is deliberately NOT shimmed — the pool
//! keeps `std::sync::Arc` (its refcount is already correct); the interleavings
//! worth modelling live in the lock/condvar handoff. The shim compiles straight
//! to `std` in every normal build, so there is no runtime cost.

#[cfg(not(loom))]
pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
pub(crate) use std::thread;

#[cfg(loom)]
pub(crate) use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(loom)]
pub(crate) use loom::thread;
