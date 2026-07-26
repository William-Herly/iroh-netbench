//! Deterministic duration statistics used by latency reports.

use std::time::Duration;

use crate::LatencyReport;

pub(crate) fn latency_report(samples: &[Duration]) -> LatencyReport {
    if samples.is_empty() {
        return LatencyReport {
            samples: 0,
            min: Duration::ZERO,
            mean: Duration::ZERO,
            p50: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
            max: Duration::ZERO,
            jitter_mean: Duration::ZERO,
            jitter_p95: Duration::ZERO,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let jitter: Vec<_> = samples
        .windows(2)
        .map(|pair| pair[0].abs_diff(pair[1]))
        .collect();
    let mut sorted_jitter = jitter.clone();
    sorted_jitter.sort_unstable();

    LatencyReport {
        samples: u32::try_from(samples.len()).unwrap_or(u32::MAX),
        min: sorted[0],
        mean: mean(samples),
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        max: *sorted.last().expect("non-empty"),
        jitter_mean: mean(&jitter),
        jitter_p95: percentile(&sorted_jitter, 95),
    }
}

fn mean(samples: &[Duration]) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let total: u128 = samples.iter().map(Duration::as_nanos).sum();
    duration_from_nanos(total / samples.len() as u128)
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let samples: Vec<_> = (1..=100).map(Duration::from_millis).collect();
        let report = latency_report(&samples);
        assert_eq!(report.p50, Duration::from_millis(50));
        assert_eq!(report.p95, Duration::from_millis(95));
        assert_eq!(report.p99, Duration::from_millis(99));
        assert_eq!(
            report.mean,
            Duration::from_millis(50) + Duration::from_micros(500)
        );
    }

    #[test]
    fn empty_report_is_zeroed() {
        assert_eq!(latency_report(&[]).samples, 0);
    }
}
