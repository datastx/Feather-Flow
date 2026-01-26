---
name: review-rust-code
description: Review and enforce Rust best practices, clean code principles, and idiomatic patterns. Use when reviewing code, writing new Rust code, or refactoring existing implementations. Covers type-driven design, error handling, async patterns, testing, and project-specific standards.
---

# Review Rust Code - Best Practices & Standards

Apply these standards when working with Rust code in this project. These patterns leverage Rust's type system, ownership model, and trait system to make illegal states unrepresentable and enforce clean code at compile time.

## Core Principles

1. **Type-driven design**: Use the type system to make invalid states unrepresentable
2. **Early returns**: Keep code flat with `?` operator and `let-else` statements
3. **Trait-based abstractions**: Zero-cost dependency injection through traits
4. **Parse, don't validate**: Enforce invariants at construction time
5. **Minimal visibility**: Start private, expose deliberately
6. **AST over regex**: Use proper parsers and abstract syntax trees instead of string manipulation (especially for SQL and code generation)
7. **Meaningful comments only**: Comments explain "why", not "what"—the code itself should be self-explanatory

---

## Early Returns and Guard Clauses

### Use `?` operator for error propagation

The `?` operator automatically converts error types via the `From` trait. When a function returns `Result<T, AppError>`, any internal error type that implements `From<InternalError>` for `AppError` can use `?` directly.

```rust
// GOOD: Flat, readable, early returns
fn process_user(id: u32) -> Result<UserProfile, AppError> {
    let user = find_user(id)?;           // Returns early on Err
    let profile = load_profile(&user)?;   // Automatic error conversion
    let settings = get_settings(&user)?;  
    Ok(UserProfile { user, profile, settings })
}

// BAD: Nested match statements
fn process_user_bad(id: u32) -> Result<UserProfile, AppError> {
    match find_user(id) {
        Ok(user) => {
            match load_profile(&user) {
                Ok(profile) => {
                    match get_settings(&user) {
                        Ok(settings) => Ok(UserProfile { user, profile, settings }),
                        Err(e) => Err(e.into()),
                    }
                }
                Err(e) => Err(e.into()),
            }
        }
        Err(e) => Err(e.into()),
    }
}
```

### Use `let-else` for guard clauses

Use `let-else` (stable since Rust 1.65) to clearly highlight the happy path while handling exceptions early.

```rust
// GOOD: Clear happy path with let-else
fn get_user_name(user_id: u32) -> Result<String, AppError> {
    let Some(user) = find_user(user_id) else {
        return Err(AppError::NotFound("User not found"));
    };
    
    let Some(name) = user.name.clone() else {
        return Err(AppError::InvalidData("Name missing"));
    };
    
    Ok(name)
}
```

### Prefer `ok_or_else` over `ok_or`

Use lazy evaluation to avoid unnecessary error construction:

```rust
// GOOD: Lazy evaluation
get_user().ok_or_else(|| AppError::NotFound("User not found".to_string()))?

// BAD: Eager evaluation (constructs error even if not needed)
get_user().ok_or(AppError::NotFound("User not found".to_string()))?
```

---

## SOLID Principles Through Traits

### Single Responsibility Principle

Map to Rust's module system with visibility controls:

```rust
// GOOD: Small, focused modules with clear boundaries
pub(crate) mod user_service {
    use super::User;
    
    pub(crate) fn create_user(email: &str) -> Result<User, Error> {
        // Single responsibility: user creation
    }
}

// Use pub, pub(crate), pub(super) to enforce boundaries
```

### Open/Closed Principle

Use traits and generics for extension without modification:

```rust
// GOOD: Open for extension through trait implementations
trait Shape {
    fn area(&self) -> f64;
}

fn calculate_total_area(shapes: &[impl Shape]) -> f64 {
    shapes.iter().map(|s| s.area()).sum()
}

// Adding new shapes doesn't require modifying existing code
struct Circle { radius: f64 }
impl Shape for Circle {
    fn area(&self) -> f64 { std::f64::consts::PI * self.radius * self.radius }
}
```

### Liskov Substitution

Design traits with compile-time guarantees:

```rust
// GOOD: Separate traits for different capabilities
trait Bird {
    fn name(&self) -> &str;
}

trait FlyingBird: Bird {
    fn fly(&self);
}

trait SwimmingBird: Bird {
    fn swim(&self);
}

// BAD: Penguin implementing Bird::fly() by panicking
// This is prevented by the trait design above
```

### Interface Segregation

Use small, focused traits that types implement only when needed:

```rust
// GOOD: Small, focused traits
trait Printer {
    fn print(&self, doc: &Document);
}

trait Scanner {
    fn scan(&self) -> Document;
}

trait Fax {
    fn fax(&self, doc: &Document, number: &str);
}

// Types implement only what they need
struct SimplePrinter;
impl Printer for SimplePrinter {
    fn print(&self, doc: &Document) { /* ... */ }
}

// BAD: Monolithic trait
trait Machine {
    fn print(&self, doc: &Document);
    fn scan(&self) -> Document;
    fn fax(&self, doc: &Document, number: &str);
}
```

### Dependency Inversion

Use trait-based dependency injection:

```rust
// GOOD: High-level module depends on abstraction
trait Messenger {
    fn send(&self, user: &str, message: &str);
}

struct NotificationService<M: Messenger> {
    messenger: M,
}

impl<M: Messenger> NotificationService<M> {
    fn notify(&self, user: &str, message: &str) {
        self.messenger.send(user, message);
    }
}

// Decision matrix:
// - Use generics (impl Trait, <T: Trait>) for compile-time DI (zero cost)
// - Use trait objects (Box<dyn Trait + Send + Sync>) for runtime polymorphism
```

---

## Type-Driven Design

### Newtype pattern for domain types

Wrap primitive types to prevent entire categories of bugs:

```rust
// GOOD: Distinct types prevent mixing
struct UserId(Uuid);
struct ProductId(Uuid);

fn get_user(id: UserId) -> Option<User> { /* ... */ }

let product_id = ProductId(Uuid::new_v4());
// get_user(product_id);  // COMPILE ERROR: types don't match

// GOOD: Enforce invariants at construction
pub struct Email(String);

impl Email {
    pub fn parse(s: String) -> Result<Self, ValidationError> {
        if is_valid_email(&s) {
            Ok(Email(s))
        } else {
            Err(ValidationError::InvalidEmail)
        }
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// If an Email exists, it's guaranteed to be valid
```

### Parse, don't validate

Enforce invariants at construction with validation:

```rust
// GOOD: Validation happens once at deserialization
use serde::{Deserialize, Deserializer};

impl<'de> Deserialize<'de> for Email {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Email::parse(s).map_err(serde::de::Error::custom)
    }
}

// Invalid data never enters the domain model
```

### Typestate pattern for builders

Use the type system to enforce required fields at compile time:

```rust
// GOOD: Typestate builder (compile-time guarantees)
struct RequestBuilder<State> {
    url: Option<String>,
    method: Option<String>,
    _state: PhantomData<State>,
}

struct NoUrl;
struct HasUrl;

impl RequestBuilder<NoUrl> {
    fn url(self, url: String) -> RequestBuilder<HasUrl> {
        RequestBuilder {
            url: Some(url),
            method: self.method,
            _state: PhantomData,
        }
    }
}

impl RequestBuilder<HasUrl> {
    fn build(self) -> Request {
        // This method only exists when URL is set
        Request {
            url: self.url.unwrap(),
            method: self.method.unwrap_or_else(|| "GET".to_string()),
        }
    }
}

// ACCEPTABLE: Runtime validation for dynamic cases
#[derive(Default)]
struct FlexibleBuilder {
    url: Option<String>,
    method: Option<String>,
}

impl FlexibleBuilder {
    fn build(self) -> Result<Request, BuildError> {
        Ok(Request {
            url: self.url.ok_or(BuildError::MissingUrl)?,
            method: self.method.unwrap_or_else(|| "GET".to_string()),
        })
    }
}
```

### State machines with enums

Make state transitions explicit and exhaustive:

```rust
// GOOD: Each state carries exactly the data it needs
enum Order {
    Draft { items: Vec<Item> },
    Submitted { items: Vec<Item>, at: DateTime<Utc> },
    Paid { items: Vec<Item>, at: DateTime<Utc>, payment: Payment },
    Shipped { items: Vec<Item>, at: DateTime<Utc>, payment: Payment, tracking: TrackingInfo },
}

impl Order {
    fn submit(self) -> Result<Order, OrderError> {
        match self {
            Order::Draft { items } => Ok(Order::Submitted {
                items,
                at: Utc::now(),
            }),
            _ => Err(OrderError::InvalidTransition),
        }
    }
    
    fn ship(self, tracking: TrackingInfo) -> Result<Order, OrderError> {
        match self {
            Order::Paid { items, at, payment } => Ok(Order::Shipped {
                items,
                at,
                payment,
                tracking,
            }),
            _ => Err(OrderError::CannotShipUnpaidOrder),
        }
    }
}

// BAD: Boolean flags and nullable fields
struct OrderBad {
    items: Vec<Item>,
    submitted: bool,
    paid: bool,
    shipped: bool,
    payment: Option<Payment>,
    tracking: Option<TrackingInfo>,
}
```

---

## Error Handling

### Use `thiserror` for libraries, `anyhow` for applications

```rust
// GOOD: Library code with thiserror
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
    
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// GOOD: Application code with anyhow
use anyhow::{Context, Result};

fn run_app() -> Result<()> {
    let config = load_config()
        .context("Failed to load config")?;
    
    let db = connect_db(&config.db_url)
        .context("Failed to connect to database")?;
    
    Ok(())
}
```

### Always include error context

```rust
// GOOD: Rich error context
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Failed to process user {user_id}")]
    UserProcessing {
        user_id: u32,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    
    #[error("Database error")]
    Database(#[from] sqlx::Error),
}
```

---

## Trait Design

### Static vs. dynamic dispatch

```rust
// GOOD: Static dispatch (zero cost, but increases binary size)
fn process_shapes_static(shapes: &[impl Shape]) -> f64 {
    shapes.iter().map(|s| s.area()).sum()
}

// GOOD: Dynamic dispatch (runtime polymorphism, vtable overhead)
fn process_shapes_dynamic(shapes: &[Box<dyn Shape + Send + Sync>]) -> f64 {
    shapes.iter().map(|s| s.area()).sum()
}

// Decision matrix:
// - Generics: Type known at compile time, performance critical
// - Trait objects: Heterogeneous collections, runtime type selection, plugin systems
```

### Associated types vs. generic parameters

```rust
// GOOD: Associated types (one logical implementation per type)
trait Iterator {
    type Item;
    fn next(&mut self) -> Option<Self::Item>;
}

// GOOD: Generic parameters (multiple implementations make sense)
trait From<T> {
    fn from(value: T) -> Self;
}

impl From<String> for MyType { /* ... */ }
impl From<&str> for MyType { /* ... */ }
impl From<i32> for MyType { /* ... */ }
```

### Object safety

```rust
// GOOD: Make methods with generics or Self return types opt-in for object safety
trait Cloneable {
    fn clone_box(&self) -> Box<dyn Cloneable>;
    
    // This method won't be available on dyn Cloneable
    fn to_owned(&self) -> Self
    where
        Self: Sized + Clone,
    {
        self.clone()
    }
}
```

### Extension traits

```rust
// GOOD: Extension traits for adding methods to types you don't control
pub trait IteratorExt: Iterator {
    fn collect_vec(self) -> Vec<Self::Item>
    where
        Self: Sized,
    {
        self.collect()
    }
}

// Blanket implementation
impl<T: Iterator> IteratorExt for T {}
```

### Sealed traits

```rust
// GOOD: Sealed traits prevent external implementations
mod sealed {
    pub trait Sealed {}
}

pub trait MyTrait: sealed::Sealed {
    fn method(&self);
}

impl sealed::Sealed for MyType {}
impl MyTrait for MyType {
    fn method(&self) { /* ... */ }
}

// External crates cannot implement MyTrait
```

---

## Ownership Patterns

### Use `Cow` for flexible borrowing

```rust
use std::borrow::Cow;

// GOOD: Cow defers allocation to the point of necessity
fn process_text(input: &str) -> Cow<str> {
    if input.contains("old") {
        Cow::Owned(input.replace("old", "new"))
    } else {
        Cow::Borrowed(input)
    }
}
```

### Multi-threaded patterns

```rust
use std::sync::{Arc, RwLock, Mutex};

// GOOD: Arc<T> for immutable shared data
let shared_config = Arc::new(Config::load());

// GOOD: Arc<RwLock<T>> for read-heavy mutable data
let cache = Arc::new(RwLock::new(HashMap::new()));

// ACCEPTABLE: Arc<Mutex<T>> for write-heavy data or when RwLock overhead isn't justified
let counter = Arc::new(Mutex::new(0));

// Keep critical sections minimal
{
    let mut data = cache.write().unwrap();
    data.insert(key, value);
} // Lock released here
```

---

## Module Organization

### Minimal visibility

```rust
// GOOD: Start everything private, expose deliberately
mod internal {
    pub(crate) struct Helper; // Visible within crate
    
    impl Helper {
        pub(super) fn assist(&self) { /* ... */ } // Visible to parent module
        fn private_method(&self) { /* ... */ } // Private
    }
}

// lib.rs: Curated public API
pub use crate::internal::Helper;
pub use crate::other_module::PublicType;
```

### Prelude pattern

```rust
// GOOD: Prelude for common imports
pub mod prelude {
    pub use crate::{Error, Result};
    pub use crate::traits::{Process, Validate};
    pub use crate::types::{UserId, Email};
}

// Users can then: use mycrate::prelude::*;
```

---

## Comments and Documentation

**Project standard**: Write self-explanatory code. Only add comments when they explain "why" something is done, not "what" is being done. The code itself should be clear enough to understand what it does.

### When to use comments

```rust
// GOOD: Explains WHY, provides context that isn't obvious
pub fn process_payment(amount: Decimal) -> Result<Payment> {
    // Stripe requires amounts in cents, not dollars
    let amount_cents = (amount * 100).round() as i64;
    
    // We retry up to 3 times because Stripe occasionally returns transient 500 errors
    // during high load periods (see incident #1234)
    let payment = retry_with_backoff(|| stripe::charge(amount_cents), 3)?;
    
    Ok(payment)
}

// GOOD: Complex algorithm that benefits from explanation
fn calculate_priority_score(user: &User, task: &Task) -> f64 {
    // Priority scoring uses the Eisenhower Matrix:
    // urgent + important = 4.0
    // urgent + not important = 3.0  
    // not urgent + important = 2.0
    // not urgent + not important = 1.0
    let urgency = if task.due_date < Utc::now() + Duration::days(1) { 2.0 } else { 1.0 };
    let importance = if task.impact == Impact::High { 2.0 } else { 1.0 };
    urgency * importance
}

// GOOD: Documents non-obvious business logic
pub fn can_refund(order: &Order) -> bool {
    // Per company policy, refunds are only allowed within 30 days
    // unless the order was marked as defective by QA
    order.created_at > Utc::now() - Duration::days(30) || order.is_defective
}

// BAD: Comments state the obvious
pub fn process_payment(amount: Decimal) -> Result<Payment> {
    // Convert amount to cents
    let amount_cents = (amount * 100).round() as i64;
    
    // Call stripe charge function
    let payment = stripe::charge(amount_cents)?;
    
    // Return the payment
    Ok(payment)
}

// BAD: Code is unclear, comment tries to compensate
fn calc_score(u: &User, t: &Task) -> f64 {
    // Calculate the priority score based on task urgency and importance
    let x = if t.d < Utc::now() + Duration::days(1) { 2.0 } else { 1.0 };
    let y = if t.i == Impact::High { 2.0 } else { 1.0 };
    x * y
}
// Problem: Instead of commenting unclear code, make the code clear:
fn calculate_priority_score(user: &User, task: &Task) -> f64 {
    let urgency_multiplier = if task.due_date < Utc::now() + Duration::days(1) { 2.0 } else { 1.0 };
    let importance_multiplier = if task.impact == Impact::High { 2.0 } else { 1.0 };
    urgency_multiplier * importance_multiplier
}

// ACCEPTABLE: TODO comments with ticket references
pub fn legacy_import(data: &OldFormat) -> Result<NewFormat> {
    // TODO(#1842): Remove this legacy converter after migration completes (Q2 2026)
    convert_legacy_format(data)
}
```

### Use doc comments for public APIs

```rust
// GOOD: Doc comments for public items
/// Processes a user payment through the payment provider.
///
/// # Arguments
///
/// * `amount` - The payment amount in dollars
/// * `method` - The payment method to use
///
/// # Returns
///
/// Returns the completed payment transaction or an error if the payment fails.
///
/// # Errors
///
/// Returns `PaymentError::InsufficientFunds` if the account balance is too low.
/// Returns `PaymentError::NetworkError` if unable to reach the payment provider.
///
/// # Examples
///
/// ```
/// let payment = process_payment(Decimal::new(2999, 2), PaymentMethod::Card)?;
/// assert_eq!(payment.amount, Decimal::new(2999, 2));
/// ```
pub fn process_payment(amount: Decimal, method: PaymentMethod) -> Result<Payment, PaymentError> {
    // Implementation
}

// GOOD: Module-level documentation
//! Payment processing module.
//!
//! This module handles all payment transactions through our payment provider.
//! It supports multiple payment methods and includes retry logic for transient failures.

// BAD: Missing doc comments on public API
pub fn process_payment(amount: Decimal, method: PaymentMethod) -> Result<Payment, PaymentError> {
    // No documentation - users don't know what this does
}
```

### Avoid commented-out code

```rust
// BAD: Commented-out code should be deleted
pub fn calculate_total(items: &[Item]) -> Decimal {
    // let total = items.iter().map(|i| i.price).sum();
    // total * Decimal::new(109, 2) // Old tax rate
    
    items.iter()
        .map(|i| i.price * Decimal::new(108, 2))
        .sum()
}

// GOOD: Remove dead code (use version control to recover if needed)
pub fn calculate_total(items: &[Item]) -> Decimal {
    items.iter()
        .map(|i| i.price * Decimal::new(108, 2))
        .sum()
}
```

### When in doubt, prefer better naming over comments

```rust
// BAD: Comment needed because of unclear name
fn proc_usr(u: &User) -> Result<()> {
    // Validate user email and send confirmation
    validate_email(&u.email)?;
    send_confirmation(&u.email)?;
    Ok(())
}

// GOOD: Clear function name eliminates need for comment
fn validate_and_send_confirmation_email(user: &User) -> Result<()> {
    validate_email(&user.email)?;
    send_confirmation(&user.email)?;
    Ok(())
}
```

---

## AST Parsing Over String Manipulation

**Project-specific standard**: When working with SQL queries, templates, or code generation, always use the parser's abstract syntax tree (AST) rather than regex or string manipulation.

### Use AST parsers for structural transformations

```rust
// GOOD: Using the SQL parser's AST
use crate::parser::Parser;
use crate::extractor::DependencyExtractor;

fn extract_table_references(sql: &str) -> Result<Vec<String>> {
    let ast = Parser::parse_sql(sql)?;
    let extractor = DependencyExtractor::new();
    Ok(extractor.extract_tables(&ast))
}

fn rewrite_table_name(sql: &str, old: &str, new: &str) -> Result<String> {
    let mut ast = Parser::parse_sql(sql)?;
    
    // Walk the AST and transform nodes
    ast.visit_mut(&mut |node| {
        if let Node::TableRef(ref mut name) = node {
            if name == old {
                *name = new.to_string();
            }
        }
    });
    
    Ok(ast.to_string())
}

// BAD: Using regex for SQL manipulation
fn extract_table_references_bad(sql: &str) -> Vec<String> {
    let re = Regex::new(r"FROM\s+(\w+)").unwrap();
    re.captures_iter(sql)
        .map(|cap| cap[1].to_string())
        .collect()
    // Problems:
    // - Misses JOINs, subqueries, CTEs
    // - Breaks on qualified names (schema.table)
    // - Can't handle comments, strings containing FROM
    // - Fragile to SQL variations
}

fn rewrite_table_name_bad(sql: &str, old: &str, new: &str) -> String {
    sql.replace(old, new)
    // Problems:
    // - Replaces table name in comments, strings
    // - Can replace partial matches
    // - Doesn't understand SQL context
}
```

### Leverage AST for validation and analysis

```rust
// GOOD: AST-based validation
use crate::validator::SqlValidator;

fn validate_query(sql: &str) -> Result<Vec<ValidationError>> {
    let ast = Parser::parse_sql(sql)?;
    let validator = SqlValidator::new();
    
    validator.validate(&ast)
        .check_table_exists()
        .check_column_references()
        .check_type_compatibility()
        .errors()
}

// GOOD: Type-safe AST transformations
fn add_row_filter(ast: &mut SelectStmt, filter: WhereClause) -> Result<()> {
    match &mut ast.where_clause {
        Some(existing) => {
            // Combine with existing WHERE
            *existing = WhereClause::And(vec![existing.clone(), filter]);
        }
        None => {
            ast.where_clause = Some(filter);
        }
    }
    Ok(())
}

// BAD: String concatenation
fn add_row_filter_bad(sql: &str, filter: &str) -> String {
    if sql.to_uppercase().contains("WHERE") {
        sql.replace("WHERE", &format!("WHERE {} AND", filter))
    } else {
        format!("{} WHERE {}", sql, filter)
    }
    // Problems:
    // - Doesn't handle subqueries
    // - Breaks on WHERE in strings/comments
    // - Incorrect precedence
    // - SQL injection risk if filter isn't sanitized
}
```

### When regex is acceptable

Regex is appropriate for:
- Simple pattern matching that doesn't require understanding structure
- Preprocessing before parsing (e.g., removing comments for display)
- Non-critical string operations where context doesn't matter

```rust
// ACCEPTABLE: Simple pattern matching
fn extract_placeholders(template: &str) -> Vec<String> {
    let re = Regex::new(r"\{\{\s*(\w+)\s*\}\}").unwrap();
    re.captures_iter(template)
        .map(|cap| cap[1].to_string())
        .collect()
}

// ACCEPTABLE: Non-structural formatting
fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}
```

### Benefits of AST-based approaches

1. **Correctness**: Understands SQL structure, won't break on edge cases
2. **Maintainability**: Clear intent, easier to extend
3. **Type safety**: Compile-time guarantees about transformations
4. **Composability**: Can combine AST transformations reliably
5. **Performance**: Parse once, perform multiple operations on AST
6. **Security**: Avoids SQL injection through structured manipulation

---

## Async Patterns

### Never block the async runtime

```rust
// GOOD: Use spawn_blocking for blocking operations
use tokio::task;

async fn process_data(data: Vec<u8>) -> Result<String> {
    // CPU-intensive or blocking operation
    let result = task::spawn_blocking(move || {
        expensive_computation(data)
    }).await??;
    
    Ok(result)
}

// GOOD: Use Rayon for CPU-bound parallel work
use rayon::prelude::*;

async fn parallel_process(items: Vec<Item>) -> Vec<Result> {
    task::spawn_blocking(move || {
        items.par_iter()
            .map(|item| process_item(item))
            .collect()
    }).await.unwrap()
}

// BAD: Don't hold std::sync::Mutex across .await
// Use tokio::sync::Mutex instead for async code
```

### Cancellation safety

```rust
// GOOD: Keep mutable state outside futures
use tokio::select;

async fn select_example() {
    let mut state = State::new();
    
    loop {
        select! {
            result = operation1() => {
                state.update(result);
            }
            result = operation2() => {
                state.update(result);
            }
        }
    }
}

// BAD: State inside future can be lost when other branch wins
```

### Graceful shutdown

```rust
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// GOOD: Graceful shutdown pattern
async fn run_server(token: CancellationToken, tracker: TaskTracker) -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    
    loop {
        select! {
            _ = token.cancelled() => {
                // Shutdown signal received
                tracker.close();
                tracker.wait().await;
                break;
            }
            Ok((stream, _)) = listener.accept() => {
                let token = token.clone();
                tracker.spawn(async move {
                    handle_connection(stream, token).await
                });
            }
        }
    }
    
    Ok(())
}
```

### Async traits

```rust
// GOOD: Native async fn in traits (Rust 1.75+, not dyn-compatible)
trait Repository {
    async fn get(&self, id: u64) -> Result<Item>;
}

// GOOD: Use async-trait for trait objects
use async_trait::async_trait;

#[async_trait]
trait DynRepository: Send + Sync {
    async fn get(&self, id: u64) -> Result<Item>;
}

// Now works with Box<dyn DynRepository>
```

---

## Testing

### Unit tests with async support

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_async_operation() {
        let result = async_function().await;
        assert!(result.is_ok());
    }
}
```

### Table-based testing for parameterization

Use table-based testing to avoid duplicating test logic for different inputs:

```rust
// GOOD: Table-based testing with rstest
use rstest::rstest;

#[rstest]
#[case("valid@email.com", true)]
#[case("invalid.email", false)]
#[case("another@valid.email.co.uk", true)]
#[case("@invalid.com", false)]
#[case("", false)]
fn test_email_validation(#[case] email: &str, #[case] expected_valid: bool) {
    let result = Email::parse(email.to_string());
    assert_eq!(result.is_ok(), expected_valid);
}

// GOOD: Table-based with fixtures
#[rstest]
#[case(StatusCode::OK, true)]
#[case(StatusCode::CREATED, true)]
#[case(StatusCode::BAD_REQUEST, false)]
#[case(StatusCode::INTERNAL_SERVER_ERROR, false)]
fn test_is_success_status(#[case] status: StatusCode, #[case] expected: bool) {
    assert_eq!(is_success(status), expected);
}

// GOOD: Manual table-based testing without rstest
#[test]
fn test_parse_duration() {
    let test_cases = vec![
        ("1s", Ok(Duration::from_secs(1))),
        ("5m", Ok(Duration::from_secs(300))),
        ("2h", Ok(Duration::from_secs(7200))),
        ("invalid", Err(ParseError::InvalidFormat)),
        ("", Err(ParseError::Empty)),
    ];
    
    for (input, expected) in test_cases {
        let result = parse_duration(input);
        assert_eq!(result, expected, "Failed for input: {}", input);
    }
}

// BAD: Duplicated test logic
#[test]
fn test_valid_email_1() {
    let result = Email::parse("valid@email.com".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_valid_email_2() {
    let result = Email::parse("another@valid.email.co.uk".to_string());
    assert!(result.is_ok());
}

#[test]
fn test_invalid_email_1() {
    let result = Email::parse("invalid.email".to_string());
    assert!(result.is_err());
}
// ... more duplicated tests
```

### Mocking with mockall

```rust
use mockall::{automock, predicate::*};

#[automock]
trait Database {
    fn get_user(&self, id: u32) -> Result<User>;
    fn save_user(&mut self, user: User) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_with_mock() {
        let mut mock = MockDatabase::new();
        
        mock.expect_get_user()
            .with(eq(123))
            .times(1)
            .returning(|_| Ok(User::default()));
        
        let service = UserService::new(mock);
        let result = service.get_user(123);
        
        assert!(result.is_ok());
    }
}
```

### Property-based testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_email_parsing(s in "\\PC*") {
        // Test that parsing never panics
        let _ = Email::parse(s);
    }
    
    #[test]
    fn test_reversible_encoding(data: Vec<u8>) {
        let encoded = encode(&data);
        let decoded = decode(&encoded);
        prop_assert_eq!(data, decoded);
    }
}
```

---

## Structured Logging

### Use tracing for structured logging

```rust
use tracing::{info, warn, error, instrument};

#[instrument(skip(password))] // Skip sensitive data
async fn login(username: &str, password: &str) -> Result<Session> {
    info!(username = %username, "Login attempt");
    
    let user = authenticate(username, password).await?;
    
    info!(
        user_id = %user.id,
        username = %username,
        "Login successful"
    );
    
    Ok(create_session(user))
}

// GOOD: Structured fields for filtering and aggregation
info!(
    user_id = %id,
    action = "created",
    ip_address = %addr,
    "User created"
);

// BAD: String interpolation loses structure
info!("User {} created from {}", id, addr);
```

### Instrument futures for async context

```rust
use tracing::Instrument;

async fn process_request(request_id: String) {
    let span = tracing::info_span!("process_request", request_id = %request_id);
    
    async {
        // Processing logic
        handle_request().await
    }
    .instrument(span)
    .await
}
```

---

## Code Review Checklist

When reviewing Rust code, ensure:

- **Type safety**: Are domain types using newtypes? Are invalid states unrepresentable?
- **Error handling**: Using `thiserror` for libraries, `anyhow` for applications? Errors have context?
- **Early returns**: Functions use `?` and `let-else` instead of nesting?
- **Trait design**: Abstractions use traits appropriately (static vs dynamic dispatch)?
- **Module visibility**: Everything starts private, exposed deliberately with `pub(crate)` where appropriate?
- **AST parsing**: SQL/template manipulation uses AST parsers instead of regex?
- **Async patterns**: No blocking in async code? Cancellation-safe? Proper shutdown handling?
- **Testing**: Using table-based tests for parameterization? Tests are clean and maintainable?
- **Logging**: Structured logging with `tracing`? Context propagated through spans?
- **Comments**: Only meaningful comments that explain "why"? No obvious comments or commented-out code?

---

## Summary

These patterns make clean code the path of least resistance in Rust:

- **Early returns with `?` and `let-else`** are more ergonomic than nested conditionals
- **Traits naturally encourage** interface segregation and dependency inversion
- **The type system makes illegal states unrepresentable** with newtypes and state-machine enums
- **Ownership semantics force explicit decisions** about data flow
- **Async code remains cancellation-safe** by design
- **AST parsing provides correctness** that regex cannot guarantee

When writing or reviewing Rust code:

1. Start with the domain model and type-driven design
2. Use traits for abstractions, generics for performance
3. Make error handling explicit with `Result` and rich error types
4. Keep functions flat with early returns
5. Use the module system to enforce boundaries
6. Prefer AST parsers over regex for structural transformations
7. Test with table-based and property-based tests
8. Add structured logging with context propagation

For detailed examples and supporting documentation, see [examples.md](examples.md).
