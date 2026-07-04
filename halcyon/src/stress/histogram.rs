//! Fixed-bucket log2 histogram of nanosecond latencies. Bucket i holds samples
//! in [2^i, 2^(i+1)) ns. Allocation-free, trivially mergeable; percentiles
//! return the bucket's upper bound in microseconds (approximate by design —
//! cheap and distortion-free on the hot path). Port of
//! `../Halcyon/tests/stress/support/latency_histogram.hpp`.

const BUCKETS: usize = 64;

#[derive(Clone)]
pub struct LatencyHistogram {
    counts: [u64; BUCKETS],
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        LatencyHistogram {
            counts: [0; BUCKETS],
            total: 0,
        }
    }

    pub fn record(&mut self, ns: u64) {
        self.counts[bucket_for(ns)] += 1;
        self.total += 1;
    }

    pub fn merge(&mut self, other: &LatencyHistogram) {
        for i in 0..BUCKETS {
            self.counts[i] += other.counts[i];
        }
        self.total += other.total;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn percentile_us(&self, p: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        // At least rank 1: a non-empty histogram must never resolve to bucket 0
        // just because `p * total` floored below 1 (only bites tiny sample counts).
        let target = ((p * self.total as f64) as u64).max(1);
        let mut cum = 0u64;
        for i in 0..BUCKETS {
            cum += self.counts[i];
            if cum >= target {
                return bucket_upper_us(i);
            }
        }
        self.max_us()
    }

    pub fn max_us(&self) -> f64 {
        for i in (0..BUCKETS).rev() {
            if self.counts[i] != 0 {
                return bucket_upper_us(i);
            }
        }
        0.0
    }
}

fn bucket_for(mut ns: u64) -> usize {
    let mut b = 0usize;
    while b < BUCKETS - 1 {
        ns >>= 1;
        if ns == 0 {
            break;
        }
        b += 1;
    }
    b
}

// Upper bound of bucket i is 2^(i+1) ns -> /1000 us. `powi`/`ldexp` via f64 so
// the top bucket never triggers a shift-by-64.
fn bucket_upper_us(i: usize) -> f64 {
    2f64.powi(i as i32 + 1) / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_merges() {
        let mut a = LatencyHistogram::new();
        for _ in 0..100 {
            a.record(1_000);
        }
        let mut b = LatencyHistogram::new();
        for _ in 0..100 {
            b.record(1_000);
        }
        a.merge(&b);
        assert_eq!(a.count(), 200);
    }

    #[test]
    fn percentile_is_monotonic() {
        let mut h = LatencyHistogram::new();
        for _ in 0..1000 {
            h.record(1_000);
        } // ~1us
        for _ in 0..10 {
            h.record(1_000 * 1_000);
        } // ~1ms tail
        assert!(h.count() > 0);
        assert!(h.percentile_us(0.50) <= h.percentile_us(0.99));
        assert!(h.percentile_us(0.99) <= h.max_us());
    }

    #[test]
    fn empty_is_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile_us(0.5), 0.0);
        assert_eq!(h.max_us(), 0.0);
    }

    #[test]
    fn single_sample_reports_its_own_bucket() {
        // A one-sample histogram whose sample is in a high bucket must report that
        // bucket for any percentile — not bucket 0 because `p * total` floored to 0.
        let mut h = LatencyHistogram::new();
        h.record(1_000_000); // ~1ms, a high bucket
        assert_eq!(h.percentile_us(0.50), h.max_us());
        assert_eq!(h.percentile_us(0.99), h.max_us());
        assert!(h.percentile_us(0.50) > 1.0);
    }
}
