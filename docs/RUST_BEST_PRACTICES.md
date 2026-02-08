# Rust Best Practices: Design Standards the Compiler Won't Enforce

A set of conventions focused on design decisions that prevent bugs, reduce cognitive load, and keep code maintainable — things `cargo check` won't flag but code review should.

---

## 1. Prefer Stateless Functions Over Stateful Structs

If a struct exists only to carry data between method calls, it's probably just function arguments in disguise.

```rust
// avoid: struct exists just to thread state between methods
struct Processor {
    config: Config,
    intermediate: Option<Vec<Row>>,
}

impl Processor {
    fn load(&mut self) { /* sets self.intermediate */ }
    fn transform(&mut self) { /* reads self.intermediate */ }
}

// prefer: data flows through function arguments
fn load(config: &Config) -> Vec<Row> { ... }
fn transform(rows: Vec<Row>, config: &Config) -> Vec<Row> { ... }
```

Reserve structs with `&mut self` methods for when you genuinely have a resource with a lifecycle (a connection pool, a file handle, a cache).

---

## 2. Never Nest — Early Return and Extract

Flat code is readable code. Two concrete rules:

### Early return on errors/edge cases

Instead of wrapping the happy path in conditionals:

```rust
// avoid
fn process(input: Option<&str>) -> Result<Output> {
    if let Some(val) = input {
        if val.len() > 0 {
            // ... 40 lines deep
        } else {
            Err(anyhow!("empty"))
        }
    } else {
        Err(anyhow!("missing"))
    }
}

// prefer
fn process(input: Option<&str>) -> Result<Output> {
    let val = input.ok_or_else(|| anyhow!("missing"))?;
    if val.is_empty() {
        return Err(anyhow!("empty"));
    }
    // happy path at top indentation level
}
```

### Extract a function

Any time you're more than 2 levels of indentation deep, extract a function. The function name becomes documentation.

---

## 3. Make Invalid States Unrepresentable

The compiler enforces type safety, but *you* choose the types. Push invariants into the type system so bad states can't exist at all.

```rust
// avoid: stringly-typed or flag-driven logic
struct Job {
    status: String,          // "pending", "running", "done"
    result: Option<Vec<u8>>, // only valid when status == "done"
    error: Option<String>,   // only valid when status == "failed"
}

// prefer: enums that carry only the data valid for that state
enum Job {
    Pending,
    Running { started_at: Instant },
    Done { result: Vec<u8> },
    Failed { error: String },
}
```

This is the single highest-leverage Rust pattern. Encode business rules in types rather than runtime checks.

---

## 4. Newtype Pattern for Domain Concepts

Wrap primitives so the compiler prevents you from mixing them up. The compiler won't stop you from passing a `user_id: i64` where an `order_id: i64` is expected — but it will if they're different types.

```rust
struct UserId(i64);
struct OrderId(i64);

fn get_order(user_id: UserId, order_id: OrderId) -> Order { ... }
// now it's impossible to accidentally swap the arguments
```

This is free at runtime (zero-cost abstraction) and catches real bugs, especially in data pipeline code where you're juggling lots of IDs and counts.

---

## 5. Keep `unwrap()` Out of Library/Production Code

`unwrap()` and `expect()` are fine in tests and throwaway scripts. In production paths, propagate errors with `?` or handle them explicitly. Grep your codebase for `unwrap()` as a code smell.

```rust
// avoid in production paths
let val = map.get("key").unwrap();

// prefer
let val = map.get("key").ok_or_else(|| anyhow!("missing key"))?;
```

If you *truly* know a value is present, use `expect("reason this is safe")` so the invariant is documented, but treat it as a sign you might want to restructure.

---

## 6. Minimize Visibility — `pub` Is a Commitment

Default everything to private. Only make things `pub` when another module actually needs them. Use `pub(crate)` when something needs to be shared internally but isn't part of your public API.

---

## 7. Prefer Borrowed Data in Function Signatures

Accept `&str` not `String`, `&[T]` not `Vec<T>`, `&Path` not `PathBuf` — unless you need ownership. This makes functions composable and avoids unnecessary cloning.

```rust
// avoid: forces caller to allocate
fn process(name: String, items: Vec<Item>) { ... }

// prefer: borrows, caller decides about allocation
fn process(name: &str, items: &[Item]) { ... }
```

---

## 8. Use `#[must_use]` on Functions With Important Return Values

The compiler warns on unused `Result`, but not on your custom types. If ignoring a return value is always a bug, annotate it.

```rust
#[must_use = "dropping this means the work was wasted"]
fn compute_checksum(data: &[u8]) -> Checksum { ... }
```

---

## 9. Constructors Validate — Don't Allow Invalid Instances

If a type has invariants, enforce them at creation time and make the fields private.

```rust
pub struct PortNumber(u16);

impl PortNumber {
    pub fn new(port: u16) -> Result<Self> {
        if port == 0 {
            return Err(anyhow!("port 0 is not valid"));
        }
        Ok(Self(port))
    }
}
```

Now every `PortNumber` in your program is guaranteed valid. No defensive checks needed downstream.

---

## 10. Errors Should Be Informative and Structured

Don't just propagate `?` blindly — add context. Use `anyhow` for applications and `thiserror` for libraries.

```rust
// avoid: raw propagation loses context
let file = File::open(path)?;

// prefer: context tells you what went wrong at a business level
let file = File::open(path)
    .with_context(|| format!("failed to open config file at {}", path.display()))?;
```

---

## 11. Small Modules, One Responsibility

A file with 500+ lines is a code smell. Split by responsibility. Think of it like keeping each task focused — same principle at the module level.

---

## 12. Avoid `clone()` as a Reflex

When you hit a borrow checker issue, `clone()` makes it compile but often hides a design problem. Before cloning, ask: can I restructure ownership? Can I borrow instead? Treat each `clone()` as a deliberate decision, not an escape hatch.

---

## Quick Reference

| Principle | One-liner |
|---|---|
| Stateless by default | Struct only if there's a real lifecycle |
| Never nest | Early return + extract functions |
| Invalid states unrepresentable | Enums carry state-specific data |
| Newtype wrappers | Distinct types for distinct domain concepts |
| No `unwrap()` in prod | Propagate or handle, always |
| Minimal visibility | Private by default, `pub(crate)` before `pub` |
| Borrow in signatures | `&str`, `&[T]`, `&Path` |
| `#[must_use]` | If ignoring the return is a bug, say so |
| Validate at construction | Private fields + fallible constructors |
| Contextual errors | `.with_context()` on every `?` that crosses a boundary |
| Small modules | One responsibility per file |
| Intentional cloning | Every `.clone()` should be a conscious choice |