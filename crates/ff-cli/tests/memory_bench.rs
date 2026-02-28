//! Memory and performance benchmarks for Featherflow.
//!
//! These are test-based benchmarks that measure peak memory usage and duration
//! for key operations: project loading, meta DB population, and static analysis.
//! Run with: `cargo test -p ff-cli --test memory_bench --release -- --nocapture`

use ff_core::Project;
use ff_meta::populate::populate_project_load;
use ff_meta::MetaDb;
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Global allocator wrapper that tracks peak memory usage.
struct TrackingAllocator {
    inner: System,
    current: AtomicUsize,
    peak: AtomicUsize,
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            let prev = self.current.fetch_add(layout.size(), Ordering::Relaxed);
            let new = prev + layout.size();
            // Update peak via CAS loop
            let mut old_peak = self.peak.load(Ordering::Relaxed);
            while new > old_peak {
                match self.peak.compare_exchange_weak(
                    old_peak,
                    new,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => old_peak = actual,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.current.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { self.inner.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator {
    inner: System,
    current: AtomicUsize::new(0),
    peak: AtomicUsize::new(0),
};

/// Reset tracking counters and return current values.
fn reset_tracking() -> (usize, usize) {
    let current = ALLOC.current.load(Ordering::Relaxed);
    let peak = ALLOC.peak.load(Ordering::Relaxed);
    // Reset peak to current so next measurement starts fresh
    ALLOC.peak.store(current, Ordering::Relaxed);
    (current, peak)
}

/// Format bytes as human-readable string.
fn fmt_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Benchmark result for a single operation.
struct BenchResult {
    name: String,
    duration_ms: f64,
    peak_memory: usize,
    baseline_memory: usize,
}

impl BenchResult {
    fn print(&self) {
        let delta = self.peak_memory.saturating_sub(self.baseline_memory);
        println!(
            "  {:<40} {:>8.1} ms  peak: {:>10}  delta: {:>10}",
            self.name,
            self.duration_ms,
            fmt_bytes(self.peak_memory),
            fmt_bytes(delta),
        );
    }
}

// ============================================================
// Benchmarks
// ============================================================

#[test]
fn bench_project_load_sample() {
    let fixture = Path::new("tests/fixtures/sample_project");
    if !fixture.exists() {
        eprintln!("Skipping bench: sample_project fixture not found");
        return;
    }

    println!("\n=== Featherflow Memory Benchmarks ===\n");

    let results = run_benchmarks(fixture);

    println!("\n--- Results ---");
    for r in &results {
        r.print();
    }
    println!();

    // Output as JSON for programmatic consumption
    println!("--- JSON ---");
    println!("[");
    for (i, r) in results.iter().enumerate() {
        let comma = if i < results.len() - 1 { "," } else { "" };
        println!(
            "  {{\"name\": \"{}\", \"duration_ms\": {:.1}, \"peak_bytes\": {}, \"delta_bytes\": {}}}{}",
            r.name,
            r.duration_ms,
            r.peak_memory,
            r.peak_memory.saturating_sub(r.baseline_memory),
            comma,
        );
    }
    println!("]");
}

fn run_benchmarks(fixture: &Path) -> Vec<BenchResult> {
    let mut results = Vec::new();

    // Benchmark 1: Project::load
    reset_tracking();
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let project = Project::load(fixture).unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: format!("Project::load ({} models)", project.models.len()),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    // Benchmark 2: Meta DB open + migrate
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let db = MetaDb::open_memory().unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: "MetaDb::open_memory (migrations)".to_string(),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    // Benchmark 3: Full meta population
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let _project_id = db
        .transaction(|conn| populate_project_load(conn, &project))
        .unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: format!(
            "populate_project_load ({} models, {} sources, {} seeds)",
            project.models.len(),
            project.sources.len(),
            project.seeds.len(),
        ),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    // Benchmark 4: Query all models view
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let result =
        ff_meta::query::execute_query(db.conn(), "SELECT * FROM ff_meta.v_models").unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: format!("query v_models ({} rows)", result.rows.len()),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    // Benchmark 5: Query all columns view
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let result =
        ff_meta::query::execute_query(db.conn(), "SELECT * FROM ff_meta.v_columns").unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: format!("query v_columns ({} rows)", result.rows.len()),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    // Benchmark 6: List tables
    let (baseline, _) = reset_tracking();
    let start = Instant::now();
    let tables = ff_meta::query::list_tables(db.conn()).unwrap();
    let elapsed = start.elapsed();
    let (_, peak) = reset_tracking();

    results.push(BenchResult {
        name: format!("list_tables ({} tables)", tables.len()),
        duration_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory: peak,
        baseline_memory: baseline,
    });

    results
}
