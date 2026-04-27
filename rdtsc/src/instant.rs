//! Non-x86_64 fallback: delegates to `std::time::Instant`.

/// No-op on non-x86_64 — `Instant` uses `std::time::Instant` directly.
pub fn calibrate() {}

/// Low-overhead timestamp backed by `std::time::Instant`.
#[derive(Clone, Copy)]
pub struct Instant(std::time::Instant);

impl Instant {
    /// Capture the current timestamp.
    #[inline(always)]
    pub fn now() -> Self {
        Self(std::time::Instant::now())
    }

    /// Return milliseconds elapsed since `self`.
    #[inline(always)]
    pub fn elapsed_ms(self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }

    /// Return microseconds elapsed since `self`.
    #[inline(always)]
    pub fn elapsed_us(self) -> u64 {
        self.0.elapsed().as_micros() as u64
    }

    /// Return nanoseconds elapsed since `self`.
    #[inline(always)]
    pub fn elapsed_ns(self) -> u64 {
        self.0.elapsed().as_nanos() as u64
    }
}
