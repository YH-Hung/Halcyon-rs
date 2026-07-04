//! Barrier-released workload runner. Spawns `threads` workers via
//! `std::thread::scope`, releases them together, loops the body until the stop
//! condition, times each call into a per-worker histogram, then joins and
//! merges. The first worker failure is surfaced in the report. Rust-native port
//! of `../Halcyon/tests/stress/support/workload_runner.*`.

use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use super::histogram::LatencyHistogram;

/// Stop after a fixed iteration count (deterministic; correctness) OR after a
/// wall-clock duration (steady-state; perf).
#[derive(Clone, Copy)]
pub enum Stop {
    Iters(u64),
    Duration(Duration),
}

pub struct RunConfig {
    pub threads: usize,
    pub stop: Stop,
    /// Optional discarded warmup phase before timing (perf).
    pub warmup: Option<Stop>,
    pub seed: u64,
}

/// Per-worker state passed to the body. Each worker owns its RNG and histogram —
/// no shared per-op state, so no false contention and no measurement skew.
pub struct WorkerCtx {
    pub thread_index: usize,
    rng: u64,
    hist: LatencyHistogram,
    failed: Option<String>,
    errors: u64,
}

impl WorkerCtx {
    /// splitmix64 — a seeded, dependency-free PRNG for workload choices.
    pub fn next_u64(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Record the first hard failure for this worker (stops its loop).
    pub fn fail(&mut self, msg: impl Into<String>) {
        if self.failed.is_none() {
            self.failed = Some(msg.into());
        }
    }

    /// Count a tolerated (non-fatal) error — a pool acquire-timeout or a
    /// genuinely-exhausted bounded retry. Drives the report's error column.
    pub fn note_error(&mut self) {
        self.errors += 1;
    }
}

pub struct RunReport {
    pub name: String,
    pub threads: usize,
    pub ops: u64,
    pub errors: u64,
    pub wall: Duration,
    pub hist: LatencyHistogram,
    pub failed: bool,
    pub first_error: Option<String>,
}

impl RunReport {
    pub fn throughput_per_sec(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs > 0.0 {
            self.ops as f64 / secs
        } else {
            0.0
        }
    }
    pub fn p50_us(&self) -> f64 {
        self.hist.percentile_us(0.50)
    }
    pub fn p95_us(&self) -> f64 {
        self.hist.percentile_us(0.95)
    }
    pub fn p99_us(&self) -> f64 {
        self.hist.percentile_us(0.99)
    }
    pub fn max_us(&self) -> f64 {
        self.hist.max_us()
    }
    pub fn error_rate(&self) -> f64 {
        if self.ops > 0 {
            self.errors as f64 / self.ops as f64
        } else {
            0.0
        }
    }
}

struct WorkerOut {
    ops: u64,
    errors: u64,
    hist: LatencyHistogram,
    failed: Option<String>,
    /// Wall-clock of this worker's *measured* phase only (excludes spawn + warmup).
    wall: Duration,
}

pub fn run_workload<F>(name: &str, cfg: &RunConfig, body: F) -> RunReport
where
    F: Fn(&mut WorkerCtx) + Sync,
{
    let threads = cfg.threads.max(1);
    let barrier = Barrier::new(threads);
    let body = &body;

    let outs: Vec<WorkerOut> = thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut ctx = WorkerCtx {
                        thread_index: i,
                        rng: cfg.seed ^ (i as u64).wrapping_mul(0x0100_0000_01B3),
                        hist: LatencyHistogram::new(),
                        failed: None,
                        errors: 0,
                    };

                    // Warmup (discarded): run, then reset the measured state.
                    if let Some(w) = cfg.warmup {
                        run_phase(&mut ctx, threads, w, body);
                        ctx.hist = LatencyHistogram::new();
                        ctx.errors = 0;
                        ctx.failed = None;
                    }

                    // Time only the measured phase: workers are released together
                    // by the barrier, so spawn and warmup are excluded from wall.
                    barrier.wait();
                    let started = Instant::now();
                    let ops = run_phase(&mut ctx, threads, cfg.stop, body);
                    let wall = started.elapsed();
                    WorkerOut {
                        ops,
                        errors: ctx.errors,
                        hist: ctx.hist,
                        failed: ctx.failed,
                        wall,
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut report = RunReport {
        name: name.to_string(),
        threads,
        ops: 0,
        errors: 0,
        // Makespan of the concurrent measured phase: workers start together at the
        // barrier, so the slowest to finish bounds throughput.
        wall: Duration::ZERO,
        hist: LatencyHistogram::new(),
        failed: false,
        first_error: None,
    };
    for out in outs {
        report.ops += out.ops;
        report.errors += out.errors;
        report.wall = report.wall.max(out.wall);
        report.hist.merge(&out.hist);
        if let Some(msg) = out.failed {
            if !report.failed {
                report.failed = true;
                report.first_error = Some(msg);
            }
        }
    }
    report
}

// Runs one phase (warmup or timed), returning this worker's op count. Stops early
// if the worker recorded a hard failure.
fn run_phase<F>(ctx: &mut WorkerCtx, threads: usize, stop: Stop, body: &F) -> u64
where
    F: Fn(&mut WorkerCtx) + Sync,
{
    let mut ops = 0u64;
    match stop {
        Stop::Iters(total) => {
            // Split iterations across threads; earlier threads take the remainder.
            let base = total / threads as u64;
            let rem = total % threads as u64;
            let quota = base
                + if (ctx.thread_index as u64) < rem {
                    1
                } else {
                    0
                };
            for _ in 0..quota {
                if ctx.failed.is_some() {
                    break;
                }
                let t = Instant::now();
                body(ctx);
                ctx.hist.record(t.elapsed().as_nanos() as u64);
                ops += 1;
            }
        }
        Stop::Duration(d) => {
            let deadline = Instant::now() + d;
            while Instant::now() < deadline {
                if ctx.failed.is_some() {
                    break;
                }
                let t = Instant::now();
                body(ctx);
                ctx.hist.record(t.elapsed().as_nanos() as u64);
                ops += 1;
            }
        }
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn runs_exactly_total_iterations_across_threads() {
        let ops = AtomicI64::new(0);
        let cfg = RunConfig {
            threads: 4,
            stop: Stop::Iters(4000),
            warmup: None,
            seed: 1,
        };
        let r = run_workload("count", &cfg, |_ctx| {
            ops.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(ops.load(Ordering::Relaxed), 4000);
        assert_eq!(r.ops, 4000);
        assert!(!r.failed);
        assert_eq!(r.hist.count(), 4000);
    }

    #[test]
    fn surfaces_worker_failure() {
        let cfg = RunConfig {
            threads: 2,
            stop: Stop::Iters(10),
            warmup: None,
            seed: 1,
        };
        let r = run_workload("boom", &cfg, |ctx| ctx.fail("kaboom"));
        assert!(r.failed);
        assert_eq!(r.first_error.as_deref(), Some("kaboom"));
    }

    #[test]
    fn duration_mode_runs_and_reports_throughput() {
        let cfg = RunConfig {
            threads: 2,
            stop: Stop::Duration(std::time::Duration::from_millis(50)),
            warmup: None,
            seed: 1,
        };
        let r = run_workload("spin", &cfg, |_ctx| {});
        assert!(r.ops > 0);
        assert!(r.throughput_per_sec() > 0.0);
    }
}
