//! RDTSC-based `Instant` for low-overhead timing.
//! On non-x86_64 falls back to `std::time::Instant`.
//!
//! # Usage
//!
//! Call [`calibrate`] once at process startup, then capture timestamps with [`Instant::now`]
//! and read elapsed time via the `elapsed_ms` / `elapsed_us` / `elapsed_ns` methods.
//!
//! ```
//! rdtsc::calibrate(); // once, at startup
//!
//! let start = rdtsc::Instant::now();
//! // ... do work ...
//! let ms = start.elapsed_ms();
//! ```

#[cfg_attr(target_arch = "x86_64", path = "tsc.rs")]
#[cfg_attr(not(target_arch = "x86_64"), path = "instant.rs")]
mod imp;
pub use imp::{Instant, calibrate};
