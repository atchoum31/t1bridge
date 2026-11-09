//! Shared conversion from an optional deadline wait to a native poll timeout.

use std::time::Duration;

/// Native timeout that blocks until a descriptor is ready.
pub(crate) const WAIT_FOREVER_MS: u32 = u32::MAX;

/// Rounds up so a wait never returns just before its deadline, which would
/// otherwise repeat zero-length polls until the deadline passes.
pub(crate) fn milliseconds(timeout: Option<Duration>) -> u32 {
    let Some(timeout) = timeout else {
        return WAIT_FOREVER_MS;
    };
    let bounded = u128::from(i32::MAX.unsigned_abs());
    let rounded = timeout.as_nanos().div_ceil(1_000_000).min(bounded);
    u32::try_from(rounded).unwrap_or(i32::MAX.unsigned_abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_waits_up_and_bounds_them_below_the_forever_sentinel() {
        assert_eq!(milliseconds(None), WAIT_FOREVER_MS);
        assert_eq!(milliseconds(Some(Duration::ZERO)), 0);
        assert_eq!(milliseconds(Some(Duration::from_nanos(1))), 1);
        assert_eq!(milliseconds(Some(Duration::from_micros(49_001))), 50);
        assert_eq!(milliseconds(Some(Duration::from_millis(50))), 50);
        assert_eq!(milliseconds(Some(Duration::MAX)), i32::MAX.unsigned_abs());
    }
}
