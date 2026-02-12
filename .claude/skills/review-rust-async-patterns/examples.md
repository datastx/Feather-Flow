# Review Rust Async Patterns - Extended Examples

Additional detailed examples for reference. The main SKILL.md provides the core rules; refer here for advanced scenarios and real-world patterns.

---

## Blocking Detection

### How to tell if something blocks

```rust
// If you're unsure whether a call blocks, check:
// 1. Does it return a Future / can you .await it? If not, it likely blocks.
// 2. Does the crate docs say "async" or "non-blocking"?
// 3. Is it a C FFI call? → blocks
// 4. Does it do file I/O, DNS resolution, or heavy computation? → blocks

// Common blocking calls that sneak into async code:
async fn sneaky_blockers() {
    std::thread::sleep(Duration::from_secs(1));   // blocks — use tokio::time::sleep
    std::fs::read_to_string("file.txt");           // blocks — use tokio::fs
    std::net::TcpStream::connect("host:80");       // blocks — use tokio::net
    dns_lookup::lookup_host("example.com");         // blocks — use trust-dns or hickory
    serde_json::to_string(&huge_struct);            // blocks if huge — use spawn_blocking
    image::open("photo.jpg");                       // blocks — use spawn_blocking
    bcrypt::hash(password, cost);                   // blocks (intentionally slow) — spawn_blocking
}
```

### Rayon integration for CPU parallelism

```rust
// GOOD — rayon for parallel CPU work, bridged through spawn_blocking
use rayon::prelude::*;

async fn process_images(images: Vec<RawImage>) -> Vec<ProcessedImage> {
    tokio::task::spawn_blocking(move || {
        images
            .par_iter()
            .map(|img| expensive_resize(img))
            .collect()
    })
    .await
    .expect("rayon task panicked")
}
```

---

## Advanced Mutex Patterns

### RwLock for read-heavy workloads

```rust
use tokio::sync::RwLock;

struct Cache {
    data: Arc<RwLock<HashMap<String, CachedItem>>>,
}

impl Cache {
    async fn get(&self, key: &str) -> Option<CachedItem> {
        let guard = self.data.read().await; // multiple readers OK
        guard.get(key).cloned()
    }

    async fn set(&self, key: String, value: CachedItem) {
        let mut guard = self.data.write().await; // exclusive access
        guard.insert(key, value);
    }

    async fn get_or_fetch(&self, key: &str) -> Result<CachedItem> {
        // Check cache with read lock (cheap)
        {
            let guard = self.data.read().await;
            if let Some(item) = guard.get(key) {
                return Ok(item.clone());
            }
        } // read lock dropped

        // Fetch outside any lock
        let item = fetch_from_source(key).await?;

        // Write lock only for the insert
        {
            let mut guard = self.data.write().await;
            guard.insert(key.to_string(), item.clone());
        }

        Ok(item)
    }
}
```

### Notify for async condition variable

```rust
use tokio::sync::Notify;

struct WorkQueue {
    items: std::sync::Mutex<VecDeque<Work>>,
    notify: Notify,
}

impl WorkQueue {
    fn push(&self, work: Work) {
        self.items.lock().unwrap().push_back(work);
        self.notify.notify_one();
    }

    async fn pop(&self) -> Work {
        loop {
            if let Some(work) = self.items.lock().unwrap().pop_front() {
                return work;
            }
            self.notify.notified().await;
        }
    }
}
```

---

## Advanced JoinSet Patterns

### Early termination on first error

```rust
async fn process_all_or_fail(items: Vec<Item>) -> Result<Vec<Output>> {
    let mut set = JoinSet::new();
    let mut results = Vec::with_capacity(items.len());

    for item in items {
        set.spawn(async move { process(item).await });
    }

    while let Some(res) = set.join_next().await {
        match res? {
            Ok(output) => results.push(output),
            Err(e) => {
                set.abort_all(); // cancel remaining tasks
                return Err(e);
            }
        }
    }

    Ok(results)
}
```

### Rate-limited batch processing with progress

```rust
use tokio::sync::Semaphore;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn process_with_progress(items: Vec<Item>) -> Result<Vec<Output>> {
    let total = items.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(20));
    let mut set = JoinSet::new();

    for item in items {
        let permit = semaphore.clone().acquire_owned().await?;
        let completed = completed.clone();

        set.spawn(async move {
            let result = process(item).await;
            drop(permit);
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            if done % 100 == 0 {
                info!("Progress: {done}/{total}");
            }
            result
        });
    }

    let mut results = Vec::with_capacity(total);
    while let Some(res) = set.join_next().await {
        results.push(res??);
    }
    Ok(results)
}
```

---

## Advanced Cancellation Patterns

### Cancellation-safe wrapper

```rust
/// Runs a future to completion, ignoring cancellation.
/// Use sparingly — this defeats structured cancellation.
async fn run_to_completion<F, T>(f: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handle = tokio::spawn(f);
    handle.await.expect("task panicked")
}

// Usage: when you absolutely cannot tolerate cancellation
tokio::select! {
    result = run_to_completion(critical_operation()) => { /* ... */ }
    _ = shutdown.cancelled() => {
        // critical_operation continues running in its spawned task
    }
}
```

### Drop guard for cancellation logging

```rust
struct CancellationGuard {
    name: &'static str,
    completed: bool,
}

impl CancellationGuard {
    fn new(name: &'static str) -> Self {
        Self { name, completed: false }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if !self.completed {
            warn!("Operation '{}' was cancelled before completion", self.name);
        }
    }
}

async fn important_operation() -> Result<()> {
    let mut guard = CancellationGuard::new("important_operation");

    step_one().await?;
    step_two().await?;
    step_three().await?;

    guard.complete();
    Ok(())
}
```

---

## Actor Pattern

A common pattern for managing shared mutable state without mutexes:

```rust
struct CacheActor {
    data: HashMap<String, String>,
    rx: mpsc::Receiver<CacheCommand>,
}

enum CacheCommand {
    Get {
        key: String,
        reply: oneshot::Sender<Option<String>>,
    },
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
}

impl CacheActor {
    fn new(rx: mpsc::Receiver<CacheCommand>) -> Self {
        Self {
            data: HashMap::new(),
            rx,
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                CacheCommand::Get { key, reply } => {
                    let _ = reply.send(self.data.get(&key).cloned());
                }
                CacheCommand::Set { key, value } => {
                    self.data.insert(key, value);
                }
                CacheCommand::Delete { key } => {
                    self.data.remove(&key);
                }
            }
        }
    }
}

#[derive(Clone)]
struct CacheHandle {
    tx: mpsc::Sender<CacheCommand>,
}

impl CacheHandle {
    fn new(buffer: usize) -> (Self, CacheActor) {
        let (tx, rx) = mpsc::channel(buffer);
        (Self { tx }, CacheActor::new(rx))
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CacheCommand::Get {
                key: key.to_string(),
                reply: reply_tx,
            })
            .await?;
        Ok(reply_rx.await?)
    }

    async fn set(&self, key: String, value: String) -> Result<()> {
        self.tx
            .send(CacheCommand::Set { key, value })
            .await?;
        Ok(())
    }
}

// Usage
async fn run() {
    let (handle, actor) = CacheHandle::new(64);
    tokio::spawn(actor.run());

    handle.set("key".into(), "value".into()).await.unwrap();
    let val = handle.get("key").await.unwrap();
    assert_eq!(val, Some("value".to_string()));
}
```

---

## Retry with Exponential Backoff

```rust
use tokio::time::{sleep, Duration};

async fn retry_with_backoff<F, Fut, T, E>(
    f: F,
    max_attempts: u32,
    initial_delay: Duration,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = initial_delay;

    for attempt in 1..=max_attempts {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt == max_attempts => return Err(e),
            Err(e) => {
                warn!(
                    attempt,
                    max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "Attempt failed, retrying"
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2).min(Duration::from_secs(60));
            }
        }
    }

    unreachable!()
}

// Usage
let result = retry_with_backoff(
    || async { call_external_service().await },
    5,
    Duration::from_millis(100),
).await?;
```

---

## Testing Async Code

### Testing with `#[tokio::test]`

```rust
#[tokio::test]
async fn test_service() {
    let (handle, actor) = CacheHandle::new(16);
    let actor_handle = tokio::spawn(actor.run());

    handle.set("key".into(), "value".into()).await.unwrap();
    let result = handle.get("key").await.unwrap();
    assert_eq!(result, Some("value".to_string()));

    drop(handle); // drop sender → actor exits
    actor_handle.await.unwrap();
}
```

### Testing timeouts

```rust
#[tokio::test]
async fn test_timeout_behavior() {
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            "should not reach here"
        },
    )
    .await;

    assert!(result.is_err()); // elapsed
}
```

### Testing with time control

```rust
#[tokio::test]
async fn test_with_time_control() {
    tokio::time::pause(); // freeze time

    let start = tokio::time::Instant::now();

    tokio::time::advance(Duration::from_secs(60)).await;

    assert_eq!(start.elapsed(), Duration::from_secs(60));
    // No actual 60 seconds waited — test runs instantly
}
```

### Asserting futures are Send

```rust
fn assert_send<T: Send>(_: &T) {}

#[test]
fn key_futures_are_send() {
    let handle = CacheHandle { tx: todo!() };
    assert_send(&handle.get("key"));
    assert_send(&handle.set("k".into(), "v".into()));
}
```

---

## Full Graceful Shutdown Example

```rust
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

struct App {
    token: CancellationToken,
    tracker: TaskTracker,
}

impl App {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
    }

    fn spawn_worker<F>(&self, name: &'static str, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let token = self.token.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("{name}: shutdown signal received");
                }
                _ = f => {
                    info!("{name}: completed naturally");
                }
            }
        });
    }

    async fn run(self) {
        // Spawn subsystem workers
        self.spawn_worker("http_server", run_http_server());
        self.spawn_worker("event_processor", run_event_processor());
        self.spawn_worker("metrics_reporter", run_metrics_reporter());

        // Wait for shutdown signal
        shutdown_signal().await;
        info!("Initiating graceful shutdown");

        // Signal all workers
        self.token.cancel();
        self.tracker.close();

        // Wait with timeout
        match tokio::time::timeout(Duration::from_secs(30), self.tracker.wait()).await {
            Ok(()) => info!("All workers shut down cleanly"),
            Err(_) => warn!("Shutdown timed out, some workers did not finish"),
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )
    .expect("failed to install SIGTERM handler");

    tokio::select! {
        _ = ctrl_c => {}
        #[cfg(unix)]
        _ = sigterm.recv() => {}
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    App::new().run().await;
}
```

---

## Connection Pool Pattern

```rust
use tokio::sync::Semaphore;

struct Pool<T> {
    connections: tokio::sync::Mutex<Vec<T>>,
    semaphore: Semaphore,
    factory: Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<T>>>> + Send + Sync>,
}

impl<T: Send> Pool<T> {
    fn new(
        max_size: usize,
        factory: impl Fn() -> Pin<Box<dyn Future<Output = Result<T>>>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            connections: tokio::sync::Mutex::new(Vec::new()),
            semaphore: Semaphore::new(max_size),
            factory: Box::new(factory),
        }
    }

    async fn acquire(&self) -> Result<PoolGuard<'_, T>> {
        let permit = self.semaphore.acquire().await?;

        let conn = {
            let mut pool = self.connections.lock().await;
            pool.pop()
        };

        let conn = match conn {
            Some(c) => c,
            None => (self.factory)().await?,
        };

        Ok(PoolGuard {
            pool: self,
            conn: Some(conn),
            _permit: permit,
        })
    }
}

struct PoolGuard<'a, T> {
    pool: &'a Pool<T>,
    conn: Option<T>,
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl<T: Send> Drop for PoolGuard<'_, T> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool;
            // Return connection to pool synchronously
            // (we can't .await in Drop, but we can lock std::sync::Mutex)
            // For a real pool, use a channel to return connections
            tokio::spawn({
                let connections = pool.connections.clone();
                async move {
                    connections.lock().await.push(conn);
                }
            });
        }
    }
}
```

This examples file provides deeper patterns for complex async scenarios. Refer to the main SKILL.md for the core rules and review checklist.
