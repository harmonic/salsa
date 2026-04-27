//! x86_64 implementation: timestamps read directly from the CPU's time-stamp counter.

use {
    core::arch::x86_64::_rdtsc,
    std::{hint::black_box, sync::OnceLock, time::Duration},
};

/// How long `calibrate` samples the TSC against wall-clock to compute the scale factor.
const CALIBRATION_DURATION: Duration = Duration::from_millis(250);

/// `ms_per_cycle × 2^64`.
static MS_PER_CYCLE_Q64: OnceLock<u64> = OnceLock::new();

/// `us_per_cycle × 2^64`.
static US_PER_CYCLE_Q64: OnceLock<u64> = OnceLock::new();

/// `ns_per_cycle × 2^64`.
/// NOTE: Requires TSC > 1 GHz to avoid u64 overflow. Inaccurate if TSC < 1GHz.
static NS_PER_CYCLE_Q64: OnceLock<u64> = OnceLock::new();

/// Measure the TSC-to-time ratios. Busy-waits for ~250 ms.
/// Must be called once at startup before any `Instant::elapsed_*`.
pub fn calibrate() {
    MS_PER_CYCLE_Q64.get_or_init(|| {
        // Warm up vDSO/TLB/branch predictor to reduce jitter between paired reads.
        for _ in 0..5 {
            black_box(std::time::Instant::now());
            black_box(unsafe { _rdtsc() });
        }

        let t0 = std::time::Instant::now();
        let c0 = unsafe { _rdtsc() };
        while t0.elapsed() < CALIBRATION_DURATION {
            // Busy-wait to keep caches warm for the end reads.
            std::hint::spin_loop();
        }
        let t1 = std::time::Instant::now();
        let c1 = unsafe { _rdtsc() };

        let ns = t1.duration_since(t0).as_nanos() as f64;
        let cycles = c1.wrapping_sub(c0) as f64;
        assert!(cycles > 0.0, "TSC did not advance during calibration");
        let q64 = 2.0_f64.powi(64);

        // ns, us, and ms per cycle, each × 2^64.
        NS_PER_CYCLE_Q64.set((ns / cycles * q64) as u64).unwrap();
        US_PER_CYCLE_Q64
            .set((ns / cycles * 1e-3 * q64) as u64)
            .unwrap();
        (ns / cycles * 1e-6 * q64) as u64
    });
}

/// Low-overhead timestamp backed by RDTSC.
#[derive(Clone, Copy)]
pub struct Instant(u64);

impl Instant {
    /// Capture the current timestamp.
    #[inline(always)]
    pub fn now() -> Self {
        Self(unsafe { _rdtsc() })
    }

    /// Return milliseconds elapsed since `self`.
    #[inline(always)]
    pub fn elapsed_ms(self) -> u64 {
        self.elapsed_scaled(&MS_PER_CYCLE_Q64)
    }

    /// Return microseconds elapsed since `self`.
    #[inline(always)]
    pub fn elapsed_us(self) -> u64 {
        self.elapsed_scaled(&US_PER_CYCLE_Q64)
    }

    /// Return nanoseconds elapsed since `self`. Inaccurate on TSC < 1 GHz.
    #[inline(always)]
    pub fn elapsed_ns(self) -> u64 {
        self.elapsed_scaled(&NS_PER_CYCLE_Q64)
    }

    #[inline(always)]
    fn elapsed_scaled(self, scale: &OnceLock<u64>) -> u64 {
        let scale = *scale
            .get()
            .expect("rdtsc::calibrate() must be called before any Instant::elapsed_*");
        let delta = Self::now().0.wrapping_sub(self.0);
        // `(delta × scale) >> 64` avoids a float roundtrip — lowers to a
        // single `mul` on x86-64 (the high 64 bits of the 128-bit product).
        // u64 × u64 can never overflow u128, so the full-width multiply is
        // always exact
        (u128::from(delta).wrapping_mul(u128::from(scale)) >> 64) as u64
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::thread::sleep};

    #[test]
    fn elapsed_ms() {
        calibrate();
        let rdtsc_start = Instant::now();
        let std_start = std::time::Instant::now();

        for duration in [
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_secs(1),
        ] {
            sleep(duration);
            let rdtsc_ms = rdtsc_start.elapsed_ms();
            let std_ms = std_start.elapsed().as_millis() as u64;
            assert_eq!(
                rdtsc_ms, std_ms,
                "rdtsc={rdtsc_ms}ms std={std_ms}ms duration={duration:?}"
            );
        }
    }
}
