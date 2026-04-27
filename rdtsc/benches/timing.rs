//! Benchmarks comparing `rdtsc::Instant` against `std::time::Instant`.

use {
    criterion::{Criterion, criterion_group, criterion_main},
    rdtsc::{Instant, calibrate},
    std::hint::black_box,
};

/// Benchmark rdtsc::Instant::now() against std::time::Instant::now()
fn bench_now(c: &mut Criterion) {
    calibrate();
    let mut group = c.benchmark_group("now");
    group.bench_function("rdtsc", |b| b.iter(|| black_box(Instant::now())));
    group.bench_function("std", |b| b.iter(|| black_box(std::time::Instant::now())));
    group.finish();
}

/// Benchmark rdtsc::Instant::elapsed_ms() against std::time::Instant::elapsed_ms()
fn bench_elapsed(c: &mut Criterion) {
    calibrate();
    let rdtsc_start = Instant::now();
    let std_start = std::time::Instant::now();
    let mut group = c.benchmark_group("elapsed");
    group.bench_function("rdtsc", |b| b.iter(|| black_box(rdtsc_start.elapsed_ms())));
    group.bench_function("std", |b| b.iter(|| black_box(std_start.elapsed())));
    group.finish();
}

criterion_group!(benches, bench_now, bench_elapsed);
criterion_main!(benches);
