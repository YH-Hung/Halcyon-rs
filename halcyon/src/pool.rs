//! Thread-safe connection pool with lazy growth, blocking acquire, transparent
//! reconnect, and background reaping.
//!
//! Ported from the C++ `ConnectionPool` (`include/halcyon/pool.hpp`), minus the
//! observability sinks and per-connection statement cache (Phases 5–6) and the
//! async executor (Phase 7). The C++ slot-pointer bookkeeping collapses in Rust
//! to ownership moves: an idle connection lives *in* the pool,
//! [`acquire`](ConnectionPool::acquire) moves it out into a [`PooledConnection`]
//! lease, and the lease's [`Drop`] moves it back (or discards it, if marked
//! broken). A lease holds an `Arc<PoolInner>`, so it may safely outlive the
//! [`ConnectionPool`] handle — the pool tears down only when the last of the
//! handle, the leases, and the maintenance thread is gone.
//!
//! Determinism: the C++ pool injects a fake clock and sleep for instant,
//! reproducible tests. Here the same coverage comes from boundary durations —
//! zero backoff (so `thread::sleep` returns at once) and zero / very-large
//! timeouts (so a reap is definite either way) — and by driving
//! [`maintain`](ConnectionPool::maintain) directly rather than racing the
//! background thread.

use std::collections::VecDeque;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::Connection;
use crate::driver::{CliDriver, ConnectionParams};
use crate::error::{Error, ErrorCode, Result};
use crate::retry::BackoffPolicy;
use crate::Isolation;
// The `thread` module alias is only used off-loom (the maintenance thread and the
// std-based unit tests); under `--cfg loom` only `JoinHandle` — the field type — is.
#[cfg(not(loom))]
use crate::sync::thread;
use crate::sync::thread::JoinHandle;
use crate::sync::{Condvar, Mutex, MutexGuard};

/// Configuration for a [`ConnectionPool`]: sizing, timeouts, validation,
/// reconnect backoff, and the per-connection statement cache. Plain `Clone`
/// data with reasonable defaults (1–8 connections, 5 s acquire, 60 s idle,
/// 30 min lifetime, 64 cached statements). Ported from the C++ `PoolConfig`;
/// the observability fields land in a later phase, and the injected
/// clock/sleep are gone (see the module docs).
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum warm connections kept alive; the pool refills up to this on
    /// maintenance and never reaps below it (except for lifetime expiry).
    pub min: usize,
    /// Hard ceiling on physical connections (idle + leased + in-flight connects).
    pub max: usize,
    /// How long [`acquire`](ConnectionPool::acquire) blocks for a connection
    /// before returning a [`ErrorCode::Pool`] timeout.
    pub acquire_timeout: Duration,
    /// Idle connections above `min` are reaped once idle this long.
    pub idle_timeout: Duration,
    /// Any connection older than this is reaped on maintenance, even at `min`.
    pub max_lifetime: Duration,
    /// Probe (and reconnect if dead) each connection as it is acquired.
    pub validate_on_acquire: bool,
    /// Backoff applied to reconnect attempts inside the pool.
    pub backoff: BackoffPolicy,
    /// How often the background maintenance thread reaps and refills.
    pub maintenance_interval: Duration,
    /// Spawn the background maintenance thread. Disable to drive
    /// [`maintain`](ConnectionPool::maintain) manually (tests do this).
    pub start_maintenance_thread: bool,
    /// Per-connection prepared-statement LRU capacity; 0 disables caching.
    /// Every pooled connection (including transparent reconnects) gets its own
    /// cache of this size, cleared when the physical connection is dropped.
    pub statement_cache_size: usize,
    /// Session isolation applied to every pooled connection at open time.
    /// `None` preserves the server default.
    pub isolation: Option<Isolation>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            min: 1,
            max: 8,
            acquire_timeout: Duration::from_millis(5000),
            idle_timeout: Duration::from_millis(60_000),
            max_lifetime: Duration::from_millis(1_800_000),
            validate_on_acquire: false,
            backoff: BackoffPolicy::default(),
            maintenance_interval: Duration::from_millis(1000),
            start_maintenance_thread: true,
            statement_cache_size: 64,
            isolation: None,
        }
    }
}

/// One idle physical connection plus the bookkeeping the reaper needs.
struct IdleConn {
    conn: Connection,
    /// When the physical connection was opened (reset on reconnect); drives
    /// `max_lifetime` expiry.
    created_at: Instant,
    /// When it was last returned to the pool; drives `idle_timeout` expiry.
    last_used_at: Instant,
}

/// Mutable pool state behind the mutex.
struct PoolState {
    idle: VecDeque<IdleConn>,
    /// Connections currently leased out (neither idle nor in-flight).
    leased: usize,
    /// In-flight connects reserved against `max` while the lock is dropped, so
    /// concurrent acquirers/refillers can't collectively exceed `max`.
    pending: usize,
    stopping: bool,
}

impl PoolState {
    /// Established physical connections (idle + leased), excluding in-flight
    /// connects — the C++ `slots_.size()`.
    fn established(&self) -> usize {
        self.idle.len() + self.leased
    }
}

/// Shared pool internals, reference-counted so leases and the maintenance thread
/// keep it alive independently of the [`ConnectionPool`] handle.
struct PoolInner {
    driver: Arc<dyn CliDriver>,
    params: ConnectionParams,
    config: PoolConfig,
    state: Mutex<PoolState>,
    /// Signals an acquirer that an idle connection is available or that reserved
    /// capacity was freed.
    available: Condvar,
    /// Wakes the maintenance thread promptly on shutdown.
    maint: Condvar,
}

fn acquire_timeout_error() -> Error {
    // Matches the C++ acquire timeout: a non-retriable pool-class error.
    Error::new(ErrorCode::Pool, "connection acquire timed out")
}

impl PoolInner {
    /// Locks the pool state, recovering a poisoned mutex rather than propagating
    /// the panic. Every critical section here is pure bookkeeping — the lock is
    /// never held across a connect, disconnect, or any user code — so a thread
    /// that panicked while holding it left the state structurally consistent, and
    /// re-entering is safe. Mirrors [`Db2CliDriver::lock`] and keeps the pool's
    /// public API panic-free (the repo's "no panicking public APIs" rule).
    fn lock(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Opens one physical connection with bounded exponential backoff. Called
    /// with the state lock **not** held, so a slow Db2 connect or backoff sleep
    /// never blocks other acquirers, releasers, or the reaper.
    fn make_connection(&self) -> Result<Connection> {
        let attempts = self.config.backoff.max_attempts.max(1);
        let mut last = Error::new(ErrorCode::Connection, "no connect attempt made");
        for attempt in 1..=attempts {
            match Connection::open_with_cache(
                Arc::clone(&self.driver),
                &self.params,
                self.config.statement_cache_size,
            ) {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    // Retry is driven by `Error.retriable` (the repo rule; spec
                    // Phase 4 "transparent reconnect … driven by Error.retriable").
                    // A non-retriable connect failure — bad credentials, unknown
                    // database — fails identically every attempt, so surface it at
                    // once rather than sleeping through the backoff.
                    let retriable = e.retriable;
                    last = e;
                    if !retriable || attempt == attempts {
                        break;
                    }
                    std::thread::sleep(self.config.backoff.delay_for(attempt));
                }
            }
        }
        Err(last)
    }

    /// Completes an acquire of a popped idle connection under
    /// `validate_on_acquire`: probes it (the lock is already dropped), and if dead
    /// reconnects in place — disconnecting the dead connection **outside** the
    /// lock, matching the `release`/`maintain` discipline so a slow disconnect
    /// can't stall other acquirers/releasers/the reaper. `leased` is already
    /// charged; on a reconnect failure it is un-charged and capacity freed. The
    /// C++ `ensure_alive` analog.
    fn validated_lease(
        self: &Arc<Self>,
        conn: Connection,
        created_at: Instant,
        #[cfg(feature = "observability")] pm: &mut crate::obs::PendingMetrics,
    ) -> Result<PooledConnection> {
        if self.driver.is_alive(conn.handle()).unwrap_or(false) {
            #[cfg(feature = "observability")]
            {
                let guard = self.lock();
                pm.set_gauges(guard.idle.len(), guard.leased);
            }
            return Ok(PooledConnection::new(Arc::clone(self), conn, created_at));
        }
        // Dead: disconnect it (unlocked), then reconnect in its place.
        drop(conn);
        match self.make_connection() {
            Ok(fresh) => {
                #[cfg(feature = "observability")]
                {
                    pm.reconnects += 1;
                    pm.emit_reconnect_span = true;
                    let guard = self.lock();
                    pm.set_gauges(guard.idle.len(), guard.leased);
                }
                Ok(PooledConnection::new(
                    Arc::clone(self),
                    fresh,
                    Instant::now(),
                ))
            }
            Err(e) => {
                let mut guard = self.lock();
                guard.leased -= 1;
                #[cfg(feature = "observability")]
                pm.set_gauges(guard.idle.len(), guard.leased);
                self.available.notify_one(); // capacity freed → wake a waiter
                Err(e)
            }
        }
    }

    /// Acquires a connection, blocking up to `acquire_timeout`. Reuses an idle
    /// connection if one exists, else grows the pool up to `max`, else waits for
    /// a release. Mirrors the C++ `ConnectionPool::acquire` loop.
    fn acquire(self: &Arc<Self>) -> Result<PooledConnection> {
        #[cfg(feature = "observability")]
        let span = {
            use tracing::subscriber::NoSubscriber;
            if tracing::dispatcher::get_default(|d| d.is::<NoSubscriber>()) {
                tracing::Span::none()
            } else {
                tracing::info_span!("halcyon.acquire", error.sqlstate = tracing::field::Empty,)
            }
        };
        #[cfg(feature = "observability")]
        let _span_guard = span.enter();
        #[cfg(feature = "observability")]
        let start = Instant::now();
        #[cfg(feature = "observability")]
        let mut pm = crate::obs::PendingMetrics::default();

        let result = self.acquire_loop(
            #[cfg(feature = "observability")]
            &mut pm,
        );

        #[cfg(feature = "observability")]
        {
            pm.acquire_wait_secs = Some(start.elapsed().as_secs_f64());
            if let Err(ref e) = result {
                // C++ `span.setStatusError` on timeout/failure — record the field
                // so OTel-style exporters see an errored acquire (empty SQLSTATE
                // is still a recorded failure marker).
                if !span.is_disabled() {
                    span.record("error.sqlstate", e.sqlstate.as_str());
                    tracing::error!(error.sqlstate = %e.sqlstate, "halcyon acquire failed");
                }
            }
            crate::obs::flush_pool_metrics(&pm);
        }
        result
    }

    fn acquire_loop(
        self: &Arc<Self>,
        #[cfg(feature = "observability")] pm: &mut crate::obs::PendingMetrics,
    ) -> Result<PooledConnection> {
        let mut guard = self.lock();
        let start = Instant::now();
        loop {
            if guard.stopping {
                return Err(Error::new(
                    ErrorCode::Pool,
                    "connection pool is shutting down",
                ));
            }

            // 1. Reuse an idle connection.
            if let Some(idle) = guard.idle.pop_front() {
                guard.leased += 1;
                let IdleConn {
                    conn, created_at, ..
                } = idle;
                if self.config.validate_on_acquire {
                    drop(guard);
                    return self.validated_lease(
                        conn,
                        created_at,
                        #[cfg(feature = "observability")]
                        pm,
                    );
                }
                #[cfg(feature = "observability")]
                pm.set_gauges(guard.idle.len(), guard.leased);
                return Ok(PooledConnection::new(Arc::clone(self), conn, created_at));
            }

            // 2. Grow if under `max`.
            if guard.established() + guard.pending < self.config.max {
                guard.pending += 1;
                drop(guard);
                let made = self.make_connection();
                guard = self.lock();
                guard.pending -= 1;
                match made {
                    Ok(conn) => {
                        guard.leased += 1;
                        #[cfg(feature = "observability")]
                        pm.set_gauges(guard.idle.len(), guard.leased);
                        return Ok(PooledConnection::new(
                            Arc::clone(self),
                            conn,
                            Instant::now(),
                        ));
                    }
                    Err(e) => {
                        self.available.notify_one();
                        return Err(e);
                    }
                }
            }

            // 3. At capacity: block until a release, or time out.
            let remaining = match self.config.acquire_timeout.checked_sub(start.elapsed()) {
                Some(r) if !r.is_zero() => r,
                _ => return Err(acquire_timeout_error()),
            };
            let (g, timeout) = self
                .available
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|p| p.into_inner());
            guard = g;
            if timeout.timed_out()
                && guard.idle.is_empty()
                && guard.established() + guard.pending >= self.config.max
            {
                return Err(acquire_timeout_error());
            }
        }
    }

    /// Returns a leased connection to the pool, or discards it (disconnecting)
    /// when broken or during shutdown. Mirrors the C++ `release_slot`.
    fn release(&self, conn: Connection, created_at: Instant, broken: bool) {
        let mut guard = self.lock();
        guard.leased -= 1;
        #[cfg(feature = "observability")]
        let mut pm = crate::obs::PendingMetrics::default();
        if broken || guard.stopping {
            #[cfg(feature = "observability")]
            pm.set_gauges(guard.idle.len(), guard.leased);
            self.available.notify_one(); // capacity freed → wake a waiter
            drop(guard);
            drop(conn); // Connection::drop disconnects, outside the lock
            #[cfg(feature = "observability")]
            crate::obs::flush_pool_metrics(&pm);
        } else {
            let now = Instant::now();
            guard.idle.push_back(IdleConn {
                conn,
                created_at,
                last_used_at: now,
            });
            #[cfg(feature = "observability")]
            pm.set_gauges(guard.idle.len(), guard.leased);
            self.available.notify_one();
            drop(guard);
            #[cfg(feature = "observability")]
            crate::obs::flush_pool_metrics(&pm);
        }
    }

    /// One maintenance pass: reap expired idle connections (respecting `min` for
    /// idle expiry, always for lifetime expiry) and refill back up to `min`.
    /// Ported from the C++ `ConnectionPool::maintain`.
    fn maintain(&self) {
        let mut discarded: Vec<Connection> = Vec::new();
        let mut guard = self.lock();
        let now = Instant::now();

        // Reap. `total` tracks established connections as we remove them so the
        // `min` floor is enforced against the shrinking count (C++ over_min).
        let idles = std::mem::take(&mut guard.idle);
        let mut total = idles.len() + guard.leased;
        let mut kept = VecDeque::with_capacity(idles.len());
        for ic in idles {
            let expired_life =
                now.saturating_duration_since(ic.created_at) >= self.config.max_lifetime;
            let expired_idle =
                now.saturating_duration_since(ic.last_used_at) >= self.config.idle_timeout;
            let over_min = total > self.config.min;
            if expired_life || (expired_idle && over_min) {
                total -= 1;
                discarded.push(ic.conn); // disconnect after the lock is released
            } else {
                kept.push_back(ic);
            }
        }
        guard.idle = kept;

        // Refill up to `min` (best effort; a failed connect retries next pass).
        // Connect without the lock, reserving via `pending` like acquire().
        loop {
            if guard.stopping || guard.established() + guard.pending >= self.config.min {
                break;
            }
            guard.pending += 1;
            drop(guard);
            let made = self.make_connection();
            guard = self.lock();
            guard.pending -= 1;
            let added = made.is_ok() && !guard.stopping;
            if let Ok(conn) = made {
                if guard.stopping {
                    discarded.push(conn);
                } else {
                    let t = Instant::now();
                    guard.idle.push_back(IdleConn {
                        conn,
                        created_at: t,
                        last_used_at: t,
                    });
                }
            }
            self.available.notify_one(); // capacity may be free → wake a waiter
            if !added {
                break;
            }
        }
        #[cfg(feature = "observability")]
        let pm = {
            let mut pm = crate::obs::PendingMetrics::default();
            pm.set_gauges(guard.idle.len(), guard.leased);
            pm
        };
        drop(guard);
        #[cfg(feature = "observability")]
        crate::obs::flush_pool_metrics(&pm);
        drop(discarded); // disconnect reaped/leftover connections outside the lock
    }
}

/// A thread-safe pool of Db2 connections. Create with [`ConnectionPool::create`];
/// [`Database`](crate::Database) manages one internally, so prefer `Database` for
/// application code. Cloneable handles are not provided — share via `Arc` if
/// needed; leases already keep the pool alive on their own.
pub struct ConnectionPool {
    inner: Arc<PoolInner>,
    maintenance: Option<JoinHandle<()>>,
}

impl ConnectionPool {
    /// Builds a pool over `driver`, warms it up to `config.min` connections, and
    /// (unless disabled) starts the background maintenance thread. A warmup
    /// connect failure aborts creation and returns the error.
    pub fn create(
        driver: Arc<dyn CliDriver>,
        params: ConnectionParams,
        config: PoolConfig,
    ) -> Result<ConnectionPool> {
        // Reject an unsatisfiable config before opening anything: `max` is the hard
        // ceiling, so a pool that can never hand out a connection (`max == 0`) or
        // whose warm-up floor already exceeds the ceiling (`min > max`) is a
        // configuration error, not a runtime one.
        if config.max == 0 {
            return Err(Error::new(
                ErrorCode::Pool,
                "PoolConfig.max must be at least 1",
            ));
        }
        if config.min > config.max {
            return Err(Error::new(
                ErrorCode::Pool,
                format!(
                    "PoolConfig.min ({}) must not exceed max ({})",
                    config.min, config.max
                ),
            ));
        }

        let inner = Arc::new(PoolInner {
            driver,
            params: ConnectionParams {
                isolation: config.isolation,
                ..params
            },
            config: config.clone(),
            state: Mutex::new(PoolState {
                idle: VecDeque::new(),
                leased: 0,
                pending: 0,
                stopping: false,
            }),
            available: Condvar::new(),
            maint: Condvar::new(),
        });

        // Warm up to `min`. Single-threaded here — nothing else observes the pool
        // yet — so connect directly and stash the results; a failure drops any
        // connections opened so far (disconnecting them) and returns the error.
        let mut idle = VecDeque::with_capacity(config.min);
        for _ in 0..config.min {
            let conn = inner.make_connection()?;
            let t = Instant::now();
            idle.push_back(IdleConn {
                conn,
                created_at: t,
                last_used_at: t,
            });
        }
        inner.lock().idle = idle;

        #[cfg(feature = "observability")]
        {
            let mut pm = crate::obs::PendingMetrics::default();
            pm.set_gauges(config.min, 0);
            crate::obs::flush_pool_metrics(&pm);
        }

        // `mut` is needed only off-loom, where `start_maintenance` takes `&mut self`.
        #[cfg_attr(loom, allow(unused_mut))]
        let mut pool = ConnectionPool {
            inner,
            maintenance: None,
        };
        #[cfg(not(loom))]
        if config.start_maintenance_thread {
            pool.start_maintenance();
        }
        Ok(pool)
    }

    #[cfg(not(loom))]
    fn start_maintenance(&mut self) {
        let inner = Arc::clone(&self.inner);
        let interval = inner.config.maintenance_interval;
        let handle = thread::spawn(move || loop {
            let guard = inner.lock();
            // Wait up to `interval`, or wake immediately on shutdown.
            let (guard, _) = inner
                .maint
                .wait_timeout_while(guard, interval, |s| !s.stopping)
                .unwrap_or_else(|p| p.into_inner());
            if guard.stopping {
                break;
            }
            drop(guard); // maintain() takes the lock itself
            inner.maintain();
        });
        self.maintenance = Some(handle);
    }

    /// Leases a connection, blocking up to `acquire_timeout`.
    pub fn acquire(&self) -> Result<PooledConnection> {
        self.inner.acquire()
    }

    /// Runs one maintenance pass (reap expired idle, refill to `min`). Public so
    /// callers — and tests — can drive it deterministically.
    pub fn maintain(&self) {
        self.inner.maintain();
    }

    /// Total established connections (idle + leased).
    pub fn total_count(&self) -> usize {
        self.inner.lock().established()
    }

    /// Idle (available) connections.
    pub fn idle_count(&self) -> usize {
        self.inner.lock().idle.len()
    }

    /// Leased (in-use) connections.
    pub fn active_count(&self) -> usize {
        self.inner.lock().leased
    }

    /// Configured hard ceiling on established + pending connections.
    pub fn max(&self) -> usize {
        self.inner.config.max
    }

    /// The reconnect backoff this pool was configured with — the facade derives
    /// its default per-call retry policy from it.
    pub fn backoff_policy(&self) -> BackoffPolicy {
        self.inner.config.backoff.clone()
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        {
            let mut guard = self.inner.lock();
            guard.stopping = true;
        }
        self.inner.maint.notify_all();
        self.inner.available.notify_all();
        if let Some(handle) = self.maintenance.take() {
            let _ = handle.join();
        }
        // Idle connections disconnect when the last `Arc<PoolInner>` drops — here,
        // unless outstanding leases still hold clones, in which case teardown
        // waits for the final lease.
    }
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.inner.lock();
        f.debug_struct("ConnectionPool")
            .field("min", &self.inner.config.min)
            .field("max", &self.inner.config.max)
            .field("idle", &guard.idle.len())
            .field("leased", &guard.leased)
            .finish_non_exhaustive()
    }
}

/// An RAII lease on a pooled connection. Derefs to [`Connection`], so it drives
/// queries/executes/transactions transparently. On drop it returns the
/// connection to the pool — or discards it if [`mark_broken`](Self::mark_broken)
/// was called. Move-only and `!Sync`, like the [`Connection`] it wraps.
pub struct PooledConnection {
    inner: Arc<PoolInner>,
    // `Some` until dropped; taken in `Drop` to hand the connection back.
    conn: Option<Connection>,
    created_at: Instant,
    broken: bool,
}

impl PooledConnection {
    fn new(inner: Arc<PoolInner>, conn: Connection, created_at: Instant) -> Self {
        PooledConnection {
            inner,
            conn: Some(conn),
            created_at,
            broken: false,
        }
    }

    /// Marks the leased connection unusable: on drop it is discarded and replaced
    /// rather than returned to the pool. The facade calls this after a
    /// connection-class error. Mirrors the C++ `PooledConnection::markBroken`.
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Deref for PooledConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("pooled connection used after drop")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let broken = self.broken || conn.is_unusable();
            self.inner.release(conn, self.created_at, broken);
        }
    }
}

impl fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledConnection")
            .field("broken", &self.broken)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::testing::MockCliDriver;
    use std::sync::mpsc;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
        }
    }

    /// A config with no background thread and zero backoff — the deterministic
    /// base every test tweaks. `min`/`max` are set per test.
    fn cfg(min: usize, max: usize) -> PoolConfig {
        PoolConfig {
            min,
            max,
            acquire_timeout: Duration::from_millis(200),
            idle_timeout: Duration::from_secs(3600),
            max_lifetime: Duration::from_secs(3600),
            validate_on_acquire: false,
            backoff: BackoffPolicy {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                max_attempts: 1,
            },
            maintenance_interval: Duration::from_millis(1000),
            start_maintenance_thread: false,
            statement_cache_size: 0,
            isolation: None,
        }
    }

    fn pool(mock: &Arc<MockCliDriver>, config: PoolConfig) -> ConnectionPool {
        ConnectionPool::create(mock.clone(), params(), config).unwrap()
    }

    fn retriable_conn(msg: &str) -> Error {
        let mut e = Error::new(ErrorCode::Connection, msg);
        e.retriable = true;
        e
    }

    #[test]
    fn default_config_enables_the_statement_cache() {
        assert_eq!(PoolConfig::default().statement_cache_size, 64);
    }

    #[test]
    fn pool_config_isolation_copied_into_params_at_connect() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 1);
        config.isolation = Some(Isolation::ReadStability);

        let _pool = ConnectionPool::create(mock.clone(), params(), config).unwrap();

        assert_eq!(
            mock.connect_params()[0].isolation,
            Some(Isolation::ReadStability)
        );
        assert_eq!(mock.isolation_calls(), vec![Isolation::ReadStability]);
    }

    #[test]
    fn pooled_connection_keeps_cached_statements_across_lease_cycles() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 1);
        config.statement_cache_size = 8;
        let p = pool(&mock, config);
        for _ in 0..2 {
            let lease = p.acquire().unwrap();
            lease.execute("UPDATE t SET x = 1", ()).unwrap();
        }
        // The prepared statement survived the release/re-acquire cycle.
        assert_eq!(mock.prepared_sql().len(), 1);
    }

    #[test]
    fn create_warms_up_min_connections() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(2, 4));
        assert_eq!(mock.connect_calls(), 2);
        assert_eq!(p.total_count(), 2);
        assert_eq!(p.idle_count(), 2);
        assert_eq!(p.active_count(), 0);
    }

    #[test]
    fn create_propagates_warmup_connect_error() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_connect_error(Error::new(ErrorCode::Connection, "refused"));
        let err = ConnectionPool::create(mock.clone(), params(), cfg(1, 4)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
    }

    #[test]
    fn pool_warmup_isolation_failure_does_not_leak_capacity() {
        let mock = Arc::new(MockCliDriver::new());
        mock.push_isolation_error(Error::new(ErrorCode::Connection, "set isolation failed"));
        let mut config = cfg(1, 1);
        config.isolation = Some(Isolation::UncommittedRead);

        let error = ConnectionPool::create(mock.clone(), params(), config).unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(mock.connect_calls(), 1);
        assert_eq!(mock.disconnect_calls(), 1);
        assert_eq!(mock.isolation_calls(), vec![Isolation::UncommittedRead]);
    }

    #[test]
    fn create_rejects_min_greater_than_max() {
        let mock = Arc::new(MockCliDriver::new());
        // min=5 > max=2 would warm 5 connections and blow past the advertised
        // hard ceiling before the pool ever started.
        let err = ConnectionPool::create(mock.clone(), params(), cfg(5, 2)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Pool);
        // An invalid config opens no connections.
        assert_eq!(mock.connect_calls(), 0);
    }

    #[test]
    fn create_rejects_zero_max() {
        let mock = Arc::new(MockCliDriver::new());
        // max=0 is a pool that can never hand out a connection.
        let err = ConnectionPool::create(mock.clone(), params(), cfg(0, 0)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Pool);
        assert_eq!(mock.connect_calls(), 0);
    }

    #[test]
    fn reconnect_retries_a_retriable_connect_error() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(0, 2);
        config.backoff.max_attempts = 3; // base_delay is ZERO → instant retries
        let p = pool(&mock, config);
        mock.push_connect_error(retriable_conn("temporarily unreachable"));
        // Attempt 1 fails (retriable) → backoff → attempt 2 connects.
        let _lease = p.acquire().unwrap();
        assert_eq!(mock.connect_calls(), 2);
    }

    #[test]
    fn reconnect_does_not_retry_a_non_retriable_connect_error() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(0, 2);
        config.backoff.max_attempts = 3;
        let p = pool(&mock, config);
        // Non-retriable (default): a bad-credentials-style failure fails the same
        // way every time, so retry is pointless — surface it after one attempt.
        mock.push_connect_error(Error::new(ErrorCode::Connection, "invalid credentials"));
        let err = p.acquire().unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
        assert_eq!(mock.connect_calls(), 1);
    }

    #[test]
    fn acquire_reuses_idle_and_returns_on_drop() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(1, 4));
        {
            let lease = p.acquire().unwrap();
            assert_eq!(lease.handle().0, 1); // the warmed-up connection
            assert_eq!(p.active_count(), 1);
            assert_eq!(p.idle_count(), 0);
        } // lease drops → returned
        assert_eq!(p.active_count(), 0);
        assert_eq!(p.idle_count(), 1);
        // A second acquire reuses it: no new physical connect.
        let _l2 = p.acquire().unwrap();
        assert_eq!(mock.connect_calls(), 1);
    }

    #[test]
    fn acquire_grows_lazily_up_to_max() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(0, 2));
        assert_eq!(mock.connect_calls(), 0); // min=0 → no warmup
        let a = p.acquire().unwrap();
        let b = p.acquire().unwrap();
        assert_eq!(mock.connect_calls(), 2);
        assert_eq!(p.total_count(), 2);
        // At max with both leased: the next acquire times out.
        let err = p.acquire().unwrap_err();
        assert_eq!(err.code, ErrorCode::Pool);
        drop(a);
        drop(b);
    }

    #[test]
    fn acquire_times_out_at_capacity() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 1);
        config.acquire_timeout = Duration::from_millis(60);
        let p = pool(&mock, config);
        let _held = p.acquire().unwrap();
        let start = Instant::now();
        let err = p.acquire().unwrap_err();
        assert_eq!(err.code, ErrorCode::Pool);
        assert!(!err.retriable);
        assert!(start.elapsed() >= Duration::from_millis(50)); // actually waited
    }

    #[test]
    fn acquire_connect_failure_frees_reserved_capacity() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(0, 2));
        mock.push_connect_error(Error::new(ErrorCode::Connection, "refused"));
        let err = p.acquire().unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
        // The reserved slot was released, so a later acquire can still grow.
        assert_eq!(p.total_count(), 0);
        let _ok = p.acquire().unwrap();
        assert_eq!(p.total_count(), 1);
    }

    #[test]
    fn broken_lease_is_discarded_on_return() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(1, 4));
        {
            let mut lease = p.acquire().unwrap();
            lease.mark_broken();
        }
        assert_eq!(p.total_count(), 0);
        assert_eq!(p.idle_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn direct_lease_failed_isolation_begin_discards_unusable_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(1, 1));
        let lease = p.acquire().unwrap();
        mock.push_txn_error(Error::new(
            ErrorCode::Connection,
            "autocommit disabled failed",
        ));

        let error = lease
            .begin_with_isolation(Isolation::UncommittedRead)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        drop(lease);
        assert_eq!(p.total_count(), 0);
        assert_eq!(p.idle_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn direct_lease_restore_failure_discards_unusable_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(1, 1));
        let lease = p.acquire().unwrap();
        mock.fail_isolation_on_call(
            2,
            Error::new(ErrorCode::Connection, "isolation restore failed"),
        );

        let mut tx = lease
            .begin_with_isolation(Isolation::UncommittedRead)
            .unwrap();
        let error = tx.commit().unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert!(tx.poisoned());
        drop(tx);
        drop(lease);
        assert_eq!(p.total_count(), 0);
        assert_eq!(p.idle_count(), 0);
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn validate_on_acquire_passes_live_connection_through() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 4);
        config.validate_on_acquire = true;
        let p = pool(&mock, config);
        let _lease = p.acquire().unwrap();
        assert_eq!(mock.alive_calls(), 1);
        assert_eq!(mock.connect_calls(), 1); // no reconnect — it was alive
    }

    #[test]
    fn validate_on_acquire_reconnects_a_dead_connection() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 4);
        config.validate_on_acquire = true;
        let p = pool(&mock, config);
        mock.push_alive_result(false); // the warmed-up connection is dead
        let lease = p.acquire().unwrap();
        assert_eq!(mock.alive_calls(), 1);
        assert_eq!(mock.connect_calls(), 2); // warmup + reconnect
        assert_eq!(mock.disconnect_calls(), 1); // the dead one was dropped
        assert_eq!(lease.handle().0, 2); // fresh handle
        assert_eq!(p.total_count(), 1); // reconnected in place
    }

    #[test]
    fn validate_reconnect_failure_frees_capacity_and_errors() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 4);
        config.validate_on_acquire = true;
        let p = pool(&mock, config);
        mock.push_alive_result(false);
        mock.push_connect_error(Error::new(ErrorCode::Connection, "still refused"));
        let err = p.acquire().unwrap_err();
        assert_eq!(err.code, ErrorCode::Connection);
        // The dead connection is gone and the slot was freed.
        assert_eq!(p.total_count(), 0);
        assert_eq!(p.idle_count(), 0);
    }

    #[test]
    fn pool_reconnect_isolation_failure_disconnects() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 1);
        config.validate_on_acquire = true;
        config.isolation = Some(Isolation::ReadStability);
        let p = pool(&mock, config);
        mock.push_alive_result(false);
        mock.push_isolation_error(Error::new(ErrorCode::Connection, "set isolation failed"));

        let error = p.acquire().unwrap_err();

        assert_eq!(error.code, ErrorCode::Connection);
        assert_eq!(mock.isolation_calls(), vec![Isolation::ReadStability; 2]);
        // The dead original and the failed replacement are both disconnected;
        // the leased slot was released rather than returning to idle.
        assert_eq!(mock.disconnect_calls(), 2);
        assert_eq!(p.total_count(), 0);
        assert_eq!(p.idle_count(), 0);
        assert_eq!(p.active_count(), 0);
    }

    #[test]
    fn maintain_reaps_idle_above_min() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 4);
        config.idle_timeout = Duration::ZERO; // every idle connection is expired
        let p = pool(&mock, config);
        // Grow to 3 leased, then release all → 3 idle.
        let a = p.acquire().unwrap();
        let b = p.acquire().unwrap();
        let c = p.acquire().unwrap();
        drop((a, b, c));
        assert_eq!(p.idle_count(), 3);
        p.maintain();
        // Reaped down to min=1.
        assert_eq!(p.idle_count(), 1);
        assert_eq!(p.total_count(), 1);
        assert_eq!(mock.disconnect_calls(), 2);
    }

    #[test]
    fn maintain_keeps_fresh_idle_and_honors_min_floor() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(2, 4);
        config.idle_timeout = Duration::ZERO; // expired by time...
        let p = pool(&mock, config); // ...but total == min, so nothing is reaped
        p.maintain();
        assert_eq!(p.idle_count(), 2);
        assert_eq!(mock.disconnect_calls(), 0);
    }

    #[test]
    fn maintain_reaps_lifetime_expired_below_min_then_refills() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 4);
        config.max_lifetime = Duration::ZERO; // the warmed-up connection is too old
        let p = pool(&mock, config);
        p.maintain();
        // The old connection was reaped (lifetime overrides the min floor) and a
        // fresh one connected to refill back to min.
        assert_eq!(p.idle_count(), 1);
        assert_eq!(p.total_count(), 1);
        assert_eq!(mock.disconnect_calls(), 1);
        assert_eq!(mock.connect_calls(), 2); // warmup + refill
    }

    #[test]
    fn maintain_refills_up_to_min() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(2, 4));
        // Drop below min by discarding one connection as broken.
        {
            let mut lease = p.acquire().unwrap();
            lease.mark_broken();
        }
        assert_eq!(p.total_count(), 1);
        p.maintain();
        assert_eq!(p.total_count(), 2);
        assert_eq!(p.idle_count(), 2);
    }

    #[test]
    fn acquire_blocks_until_a_release_hands_over() {
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(0, 1);
        config.acquire_timeout = Duration::from_secs(5);
        let p = Arc::new(pool(&mock, config));

        let held = p.acquire().unwrap(); // the only connection
        let (tx, rx) = mpsc::channel();
        let p2 = Arc::clone(&p);
        let waiter = thread::spawn(move || {
            let lease = p2.acquire().unwrap(); // blocks until `held` is released
            tx.send(lease.handle().0).unwrap();
        });

        // The waiter can't have a connection yet.
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        drop(held); // hand it over
        let handed = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(handed, 1); // reused the same physical connection
        waiter.join().unwrap();
        assert_eq!(mock.connect_calls(), 1);
    }

    #[test]
    fn dropping_the_pool_disconnects_idle_connections() {
        let mock = Arc::new(MockCliDriver::new());
        {
            let _p = pool(&mock, cfg(3, 4));
            assert_eq!(mock.connect_calls(), 3);
        } // pool drops here
        assert_eq!(mock.disconnect_calls(), 3);
    }

    #[test]
    fn maintenance_thread_starts_and_stops_cleanly() {
        // A live background thread with a short interval; creating and dropping
        // the pool must not hang, and the idle connection is disconnected.
        let mock = Arc::new(MockCliDriver::new());
        let mut config = cfg(1, 2);
        config.start_maintenance_thread = true;
        config.maintenance_interval = Duration::from_millis(5);
        {
            let _p = pool(&mock, config);
            thread::sleep(Duration::from_millis(20)); // let a few passes run
        }
        assert_eq!(mock.disconnect_calls(), 1);
    }

    #[test]
    fn poisoned_state_mutex_is_recovered_not_propagated() {
        let mock = Arc::new(MockCliDriver::new());
        let p = pool(&mock, cfg(1, 4));
        // Poison the state mutex: panic on another thread while holding it.
        let inner = Arc::clone(&p.inner);
        let poisoner = thread::spawn(move || {
            let _guard = inner.state.lock().unwrap();
            panic!("poison the pool state mutex");
        });
        assert!(poisoner.join().is_err()); // the thread panicked → mutex is poisoned
                                           // Public paths recover the poisoned lock instead of panicking (the repo's
                                           // "no panicking public APIs" rule); the warmed-up connection is intact.
        assert_eq!(p.idle_count(), 1);
        let lease = p.acquire().unwrap();
        assert_eq!(p.active_count(), 1);
        drop(lease);
        assert_eq!(p.idle_count(), 1);
    }

    #[test]
    fn pool_and_lease_thread_safety_markers() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}
        assert_send_sync::<ConnectionPool>();
        assert_send::<PooledConnection>(); // Send (movable to a worker) but !Sync
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::driver::{CliDriver, ConnectionHandle, ConnectionParams, StatementHandle};
    use crate::error::{Error, ErrorCode, Result};
    use crate::value::Value;
    use crate::Isolation;
    use loom::sync::Arc as LoomArc; // for spawning; pool internals use std::sync::Arc

    // Minimal stateless fake: everything trivially succeeds. `max = 1` plus the
    // pool's `pending` reservation means at most one connection exists at a time,
    // so a constant handle never collides. No atomics/locks of its own, so it adds
    // zero branches beyond the pool's own Mutex/Condvar for loom to explore.
    struct LoomFake;

    impl CliDriver for LoomFake {
        fn connect(&self, _p: &ConnectionParams) -> Result<ConnectionHandle> {
            Ok(ConnectionHandle(1))
        }
        fn disconnect(&self, _c: ConnectionHandle) -> Result<()> {
            Ok(())
        }
        fn is_alive(&self, _c: ConnectionHandle) -> Result<bool> {
            Ok(true)
        }
        fn prepare(&self, _c: ConnectionHandle, _s: &str) -> Result<StatementHandle> {
            Ok(StatementHandle(1))
        }
        fn bind_params(&self, _s: StatementHandle, _p: &[Value]) -> Result<()> {
            Ok(())
        }
        fn execute(&self, _s: StatementHandle) -> Result<i64> {
            Ok(0)
        }
        fn execute_batch(&self, _s: StatementHandle, _r: &[Vec<Value>]) -> Result<i64> {
            Ok(0)
        }
        fn column_count(&self, _s: StatementHandle) -> Result<usize> {
            Ok(0)
        }
        fn column_name(&self, _s: StatementHandle, _i: usize) -> Result<String> {
            Ok(String::new())
        }
        fn fetch(&self, _s: StatementHandle) -> Result<bool> {
            Ok(false)
        }
        fn get_column(&self, _s: StatementHandle, _i: usize) -> Result<Value> {
            Err(Error::new(ErrorCode::Mapping, "none"))
        }
        fn finalize(&self, _s: StatementHandle) -> Result<()> {
            Ok(())
        }
        fn close_cursor(&self, _s: StatementHandle) -> Result<()> {
            Ok(())
        }
        fn set_auto_commit(&self, _c: ConnectionHandle, _e: bool) -> Result<()> {
            Ok(())
        }
        fn set_isolation(&self, _c: ConnectionHandle, _level: Isolation) -> Result<()> {
            Ok(())
        }
        fn commit(&self, _c: ConnectionHandle) -> Result<()> {
            Ok(())
        }
        fn rollback(&self, _c: ConnectionHandle) -> Result<()> {
            Ok(())
        }
    }

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "loom".into(),
            isolation: None,
        }
    }

    // A deterministic base config: no maintenance thread, zero backoff, one slot.
    fn cfg(max: usize) -> PoolConfig {
        PoolConfig {
            min: 0,
            max,
            acquire_timeout: Duration::from_millis(1),
            idle_timeout: Duration::from_secs(3600),
            max_lifetime: Duration::from_secs(3600),
            validate_on_acquire: false,
            backoff: BackoffPolicy {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                max_attempts: 1,
            },
            maintenance_interval: Duration::from_secs(3600),
            start_maintenance_thread: false,
            statement_cache_size: 0,
            isolation: None,
        }
    }

    // Two threads contend for a single connection. Whichever acquires first
    // releases it; the other must eventually get it or time out — never a lost
    // wakeup, a double-hand-out, or a deadlock. loom explores every interleaving.
    #[test]
    fn acquire_release_handoff_has_no_lost_wakeup() {
        loom::model(|| {
            let driver = std::sync::Arc::new(LoomFake);
            let pool = LoomArc::new(ConnectionPool::create(driver, params(), cfg(1)).unwrap());

            let p2 = LoomArc::clone(&pool);
            let t = loom::thread::spawn(move || {
                if let Ok(lease) = p2.acquire() {
                    drop(lease); // hand it back
                }
            });

            if let Ok(lease) = pool.acquire() {
                drop(lease);
            }
            t.join().unwrap();

            // Both leases returned or timed out: at rest nothing is leased.
            assert_eq!(pool.active_count(), 0);
        });
    }

    // Acquire-timeout path: a waiter blocked at capacity must either time out with
    // `Err(Pool)` or receive the connection when the holder releases — never
    // deadlock, never leave the pool mis-accounted. loom explores both the timeout
    // and the hand-over branches of `Condvar::wait_timeout`.
    #[test]
    fn acquire_timeout_or_handover_keeps_accounting_consistent() {
        loom::model(|| {
            let driver = std::sync::Arc::new(LoomFake);
            let pool = LoomArc::new(ConnectionPool::create(driver, params(), cfg(1)).unwrap());
            let held = pool.acquire().unwrap(); // occupy the only slot

            let p2 = LoomArc::clone(&pool);
            let waiter = loom::thread::spawn(move || {
                if let Ok(lease) = p2.acquire() {
                    drop(lease);
                }
            });

            drop(held); // may hand over to the waiter, or it may have timed out
            waiter.join().unwrap();

            // Whichever branch each interleaving took, nothing is leased at rest.
            assert_eq!(pool.active_count(), 0);
        });
    }

    // Drop ordering: a leased connection returning (the `release` path) must race
    // the pool's own teardown (`stopping = true` + notify) in every interleaving
    // with no deadlock, lost wakeup, or leak. The lease is dropped on a *separate*
    // thread from the pool so loom explores both drop orders — unlike joining the
    // worker first, which would serialize them. The lease's `Arc<PoolInner>` keeps
    // the internals alive until whichever drops last.
    #[test]
    fn lease_drop_races_pool_drop_cleanly() {
        loom::model(|| {
            let driver = std::sync::Arc::new(LoomFake);
            let pool = ConnectionPool::create(driver, params(), cfg(1)).unwrap();
            let lease = pool.acquire().unwrap(); // main holds a lease

            let t = loom::thread::spawn(move || {
                drop(lease); // worker returns the lease (release path)
            });
            drop(pool); // main tears the pool down concurrently
            t.join().unwrap();
        });
    }
}
