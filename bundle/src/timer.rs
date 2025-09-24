use std::time::Instant;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use core::arch::x86_64::_rdtsc;

#[derive(Clone, Copy, Debug)]
pub enum Timer {
    RDTSC(u64),
    Instant(Instant),
}

impl Timer {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    pub fn new() -> Self {
        if check_cpu_supports_invariant_tsc() {
            Timer::RDTSC(unsafe { _rdtsc() })
        } else {
            Timer::Instant(Instant::now())
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    pub fn new() -> Self {
        Timer::Instant(Instant::now())
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    pub fn elapsed_us(&self) -> u64 {
        match self {
            Timer::RDTSC(rdtsc) => (unsafe { _rdtsc() - rdtsc } / ticks_per_us()),
            Timer::Instant(instant) => instant.elapsed().as_micros() as u64,
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    pub fn elapsed_us(&self) -> u64 {
        match self {
            Timer::RDTSC(_) => unreachable!("RDTSC not supported outside x86"),
            Timer::Instant(instant) => instant.elapsed().as_micros() as u64,
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn memoize_ticks_per_us_and_invariant_tsc_check() {
    check_cpu_supports_invariant_tsc();
    ticks_per_us();
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn check_cpu_supports_invariant_tsc() -> bool {
    use std::sync::OnceLock;
    static SUPPORTS_INVARIANT_TSC: OnceLock<bool> = OnceLock::new();

    *SUPPORTS_INVARIANT_TSC.get_or_init(|| {
        let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") else {
            return false;
        };

        let has_constant_tsc = cpuinfo.contains("constant_tsc");
        let has_nonstop_tsc = cpuinfo.contains("nonstop_tsc");

        has_constant_tsc && has_nonstop_tsc
    })
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn ticks_per_us() -> u64 {
    use std::{sync::OnceLock, time::Duration};

    static TICKS_PER_US: OnceLock<u64> = OnceLock::new();

    *TICKS_PER_US.get_or_init(|| {
        let warm_up_duration = Duration::from_millis(1000);
        let measurement_duration = Duration::from_millis(1000);

        // Warm up
        let warm_up_start = Instant::now();
        while warm_up_start.elapsed() < warm_up_duration {
            // Spin
        }

        let start = Instant::now();
        let start_tsc = unsafe { core::arch::x86_64::_rdtsc() };

        // Measure
        while start.elapsed() < measurement_duration {
            // Spin
        }

        let end_tsc = unsafe { core::arch::x86_64::_rdtsc() };
        let elapsed_tsc = end_tsc - start_tsc;

        let duration_us = measurement_duration.as_nanos() as u64 / 1000;
        let tsc_per_us = elapsed_tsc / duration_us;

        tsc_per_us
    })
}
