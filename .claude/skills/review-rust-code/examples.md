# Review Rust Code - Extended Examples

This file contains additional detailed examples for reference. The main SKILL.md provides the core patterns; refer to this file when you need more context or advanced scenarios.

## Never Nester: Real-World Refactoring Examples

### Before/After: File processing pipeline

```rust
// BAD: 5 levels deep
fn process_files(dir: &Path) -> Result<Vec<Output>> {
    let mut results = Vec::new();
    if dir.exists() {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                if let Ok(entry) = entry {
                    if entry.path().extension() == Some("sql".as_ref()) {
                        match process_file(&entry.path()) {
                            Ok(output) => results.push(output),
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        } else {
            return Err(AppError::NotADirectory);
        }
    } else {
        return Err(AppError::DirNotFound);
    }
    Ok(results)
}

// GOOD: Max 2 levels — inversion + extraction + iterators
fn process_files(dir: &Path) -> Result<Vec<Output>> {
    if !dir.exists() {
        return Err(AppError::DirNotFound);
    }
    if !dir.is_dir() {
        return Err(AppError::NotADirectory);
    }

    fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension() == Some("sql".as_ref()))
        .map(|entry| process_file(&entry.path()))
        .collect()
}
```

### Before/After: Config loading with fallbacks

```rust
// BAD: Nested option checks
fn load_database_url(config: &Config, env: &Env) -> Result<String> {
    if let Some(db) = &config.database {
        if let Some(url) = &db.url {
            Ok(url.clone())
        } else {
            if let Ok(env_url) = env.get("DATABASE_URL") {
                Ok(env_url)
            } else {
                Err(AppError::NoDatabaseUrl)
            }
        }
    } else {
        if let Ok(env_url) = env.get("DATABASE_URL") {
            Ok(env_url)
        } else {
            Err(AppError::NoDatabaseUrl)
        }
    }
}

// GOOD: Combinator chain with fallback
fn load_database_url(config: &Config, env: &Env) -> Result<String> {
    config.database
        .as_ref()
        .and_then(|db| db.url.clone())
        .or_else(|| env.get("DATABASE_URL").ok())
        .ok_or(AppError::NoDatabaseUrl)
}
```

### Before/After: Match arm extraction

```rust
// BAD: Complex logic inside match arms
fn handle_message(msg: Message, state: &mut AppState) -> Result<Response> {
    match msg {
        Message::Login { username, password } => {
            let user = state.db.find_user(&username)?;
            let Some(user) = user else {
                return Ok(Response::error("User not found"));
            };
            if !verify_password(&password, &user.password_hash)? {
                state.failed_attempts.entry(username.clone()).and_modify(|c| *c += 1).or_insert(1);
                if state.failed_attempts[&username] > 3 {
                    state.db.lock_account(&username)?;
                    return Ok(Response::error("Account locked"));
                }
                return Ok(Response::error("Invalid password"));
            }
            state.failed_attempts.remove(&username);
            let session = state.sessions.create(&user)?;
            Ok(Response::ok(session))
        }
        Message::Logout { session_id } => {
            // ... similarly complex
        }
    }
}

// GOOD: Each arm delegates to a focused function
fn handle_message(msg: Message, state: &mut AppState) -> Result<Response> {
    match msg {
        Message::Login { username, password } => handle_login(state, &username, &password),
        Message::Logout { session_id } => handle_logout(state, &session_id),
    }
}

fn handle_login(state: &mut AppState, username: &str, password: &str) -> Result<Response> {
    let Some(user) = state.db.find_user(username)? else {
        return Ok(Response::error("User not found"));
    };
    if !verify_password(password, &user.password_hash)? {
        return handle_failed_login(state, username);
    }
    state.failed_attempts.remove(username);
    let session = state.sessions.create(&user)?;
    Ok(Response::ok(session))
}

fn handle_failed_login(state: &mut AppState, username: &str) -> Result<Response> {
    let count = state.failed_attempts.entry(username.to_string()).and_modify(|c| *c += 1).or_insert(1);
    if *count > 3 {
        state.db.lock_account(username)?;
        return Ok(Response::error("Account locked"));
    }
    Ok(Response::error("Invalid password"))
}
```

---

## Iterator Chains vs Manual Loops

### Transforming collections

```rust
// BAD: Manual loop
fn summarize_orders(orders: &[Order]) -> Summary {
    let mut total = Decimal::ZERO;
    let mut count = 0;
    let mut max_amount = Decimal::ZERO;
    for order in orders {
        if order.status == Status::Completed {
            total += order.amount;
            count += 1;
            if order.amount > max_amount {
                max_amount = order.amount;
            }
        }
    }
    Summary { total, count, max_amount }
}

// GOOD: Iterator methods
fn summarize_orders(orders: &[Order]) -> Summary {
    let completed: Vec<_> = orders.iter()
        .filter(|o| o.status == Status::Completed)
        .collect();

    Summary {
        total: completed.iter().map(|o| o.amount).sum(),
        count: completed.len(),
        max_amount: completed.iter().map(|o| o.amount).max().unwrap_or(Decimal::ZERO),
    }
}

// GOOD: fold for single-pass when performance matters
fn summarize_orders(orders: &[Order]) -> Summary {
    orders.iter()
        .filter(|o| o.status == Status::Completed)
        .fold(Summary::default(), |mut acc, order| {
            acc.total += order.amount;
            acc.count += 1;
            acc.max_amount = acc.max_amount.max(order.amount);
            acc
        })
}
```

### Building maps and grouped data

```rust
// BAD: Manual grouping
fn group_by_status(orders: &[Order]) -> HashMap<Status, Vec<&Order>> {
    let mut groups: HashMap<Status, Vec<&Order>> = HashMap::new();
    for order in orders {
        groups.entry(order.status).or_default().push(order);
    }
    groups
}

// GOOD: itertools or fold
use itertools::Itertools;

fn group_by_status(orders: &[Order]) -> HashMap<Status, Vec<&Order>> {
    orders.iter()
        .into_group_map_by(|o| o.status)
}
```

---

## AST Parsing Examples

### Complex SQL transformation using AST

```rust
use crate::parser::{Parser, Visitor, VisitorMut};
use crate::ast::{Statement, SelectStmt, Expr, TableRef};

struct TablePrefixer {
    prefix: String,
}

impl VisitorMut for TablePrefixer {
    fn visit_table_ref(&mut self, table: &mut TableRef) {
        if !table.name.contains('.') {
            table.name = format!("{}.{}", self.prefix, table.name);
        }
    }
}

fn add_schema_prefix(sql: &str, schema: &str) -> Result<String> {
    let mut ast = Parser::parse_sql(sql)?;
    let mut prefixer = TablePrefixer {
        prefix: schema.to_string(),
    };
    ast.visit_mut(&mut prefixer);
    Ok(ast.to_string())
}
```

### Column dependency extraction via AST

```rust
struct ColumnExtractor {
    columns: Vec<String>,
    current_table: Option<String>,
}

impl Visitor for ColumnExtractor {
    fn visit_table_ref(&mut self, table: &TableRef) {
        self.current_table = Some(table.name.clone());
    }

    fn visit_column_ref(&mut self, column: &ColumnRef) {
        let full_name = match (&column.table, &self.current_table) {
            (Some(table), _) => format!("{}.{}", table, column.name),
            (None, Some(table)) => format!("{}.{}", table, column.name),
            (None, None) => column.name.clone(),
        };
        self.columns.push(full_name);
    }
}

fn extract_column_dependencies(sql: &str) -> Result<Vec<String>> {
    let ast = Parser::parse_sql(sql)?;
    let mut extractor = ColumnExtractor {
        columns: Vec::new(),
        current_table: None,
    };
    ast.visit(&mut extractor);
    Ok(extractor.columns)
}
```

---

## Advanced Error Handling Patterns

### Error hierarchies with HTTP status mapping

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UserError {
    #[error("user not found: {0}")]
    NotFound(String),

    #[error("user already exists: {0}")]
    AlreadyExists(String),

    #[error("invalid user data")]
    Invalid(#[from] ValidationError),
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("user operation failed")]
    User(#[from] UserError),

    #[error("database error")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<AppError> for axum::http::StatusCode {
    fn from(err: AppError) -> Self {
        match err {
            AppError::User(UserError::NotFound(_)) => StatusCode::NOT_FOUND,
            AppError::User(UserError::AlreadyExists(_)) => StatusCode::CONFLICT,
            AppError::User(UserError::Invalid(_)) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
```

---

## Test File Organization Examples

### Separate test file pattern

```
crates/ff-core/src/
  model/
    mod.rs            # Production code
    freshness.rs      # Production code
    schema.rs         # Production code
    testing.rs        # Production code
  model_test.rs       # All unit tests for model module
  project/
    mod.rs
    loading.rs
  project_test.rs     # All unit tests for project module
```

```rust
// crates/ff-core/src/model_test.rs

#[cfg(test)]
mod tests {
    use super::model::*;
    use tempfile::TempDir;

    #[test]
    fn from_file_requires_matching_yaml() {
        let dir = TempDir::new().unwrap();
        let model_dir = dir.path().join("models/test_model");
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(model_dir.join("test_model.sql"), "SELECT 1").unwrap();

        let result = Model::from_file(&model_dir.join("test_model.sql"));
        assert!(matches!(result, Err(CoreError::MissingSchemaFile { .. })));
    }

    #[test]
    fn from_file_succeeds_with_yaml() {
        let dir = TempDir::new().unwrap();
        let model_dir = dir.path().join("models/test_model");
        fs::create_dir_all(&model_dir).unwrap();
        fs::write(model_dir.join("test_model.sql"), "SELECT 1").unwrap();
        fs::write(model_dir.join("test_model.yml"), "name: test_model\ncolumns: []").unwrap();

        let result = Model::from_file(&model_dir.join("test_model.sql"));
        assert!(result.is_ok());
    }
}
```

---

## Table-Based Testing Patterns

### Using `rstest` for parameterized tests

```rust
use rstest::rstest;

#[rstest]
#[case::empty_string("", true)]
#[case::whitespace("   ", true)]
#[case::with_text("hello", false)]
fn test_is_blank(#[case] input: &str, #[case] expected: bool) {
    assert_eq!(is_blank(input), expected);
}

#[rstest]
#[case::negative(-5, Err(ValidationError::Negative))]
#[case::zero(0, Err(ValidationError::Zero))]
#[case::too_large(1000, Err(ValidationError::TooLarge))]
#[case::valid(50, Ok(50))]
fn test_validate_age(#[case] age: i32, #[case] expected: Result<i32, ValidationError>) {
    assert_eq!(validate_age(age), expected);
}
```

### Manual table-based tests with structs

```rust
#[derive(Debug)]
struct ParseTestCase {
    name: &'static str,
    input: &'static str,
    expected: Result<User, JsonError>,
}

#[test]
fn test_json_parsing() {
    let test_cases = vec![
        ParseTestCase {
            name: "valid user",
            input: r#"{"id":1,"name":"Alice"}"#,
            expected: Ok(User { id: 1, name: "Alice".to_string() }),
        },
        ParseTestCase {
            name: "missing field",
            input: r#"{"id":1}"#,
            expected: Err(JsonError::MissingField("name")),
        },
    ];

    for case in test_cases {
        let result = parse_user(case.input);
        assert_eq!(
            result, case.expected,
            "Test case '{}' failed",
            case.name
        );
    }
}
```

### Async table-based tests

```rust
#[rstest]
#[case("GET", "/users", StatusCode::OK)]
#[case("POST", "/users", StatusCode::CREATED)]
#[case("GET", "/invalid", StatusCode::NOT_FOUND)]
#[tokio::test]
async fn test_api_endpoints(
    #[case] method: &str,
    #[case] path: &str,
    #[case] expected_status: StatusCode,
) {
    let app = create_test_app().await;
    let response = app.request(method, path).await.unwrap();
    assert_eq!(response.status(), expected_status);
}
```

### Permission matrix testing

```rust
#[test]
fn test_permission_matrix() {
    let expected = [
        ((Role::Admin, Resource::User, Action::Read), true),
        ((Role::Admin, Resource::User, Action::Write), true),
        ((Role::Admin, Resource::User, Action::Delete), true),
        ((Role::User, Resource::Post, Action::Read), true),
        ((Role::User, Resource::Post, Action::Write), true),
        ((Role::User, Resource::Post, Action::Delete), false),
        ((Role::Guest, Resource::Post, Action::Read), true),
        ((Role::Guest, Resource::Post, Action::Write), false),
        ((Role::Guest, Resource::Post, Action::Delete), false),
    ];

    for ((role, resource, action), expected_allowed) in expected {
        let result = check_permission(role, resource, action);
        assert_eq!(
            result, expected_allowed,
            "Permission check failed: {:?} {:?} on {:?}",
            role, action, resource
        );
    }
}
```

---

## Advanced Typestate Patterns

### Multi-step builder with validation

```rust
use std::marker::PhantomData;

struct NeedsUrl;
struct NeedsMethod;
struct Ready;

struct HttpRequest<State> {
    url: Option<String>,
    method: Option<String>,
    headers: Option<HashMap<String, String>>,
    body: Option<Vec<u8>>,
    _state: PhantomData<State>,
}

impl HttpRequest<NeedsUrl> {
    pub fn new() -> Self {
        Self {
            url: None,
            method: None,
            headers: None,
            body: None,
            _state: PhantomData,
        }
    }

    pub fn url(self, url: impl Into<String>) -> HttpRequest<NeedsMethod> {
        HttpRequest {
            url: Some(url.into()),
            method: self.method,
            headers: self.headers,
            body: self.body,
            _state: PhantomData,
        }
    }
}

impl HttpRequest<NeedsMethod> {
    pub fn method(self, method: impl Into<String>) -> HttpRequest<Ready> {
        HttpRequest {
            url: self.url,
            method: Some(method.into()),
            headers: self.headers,
            body: self.body,
            _state: PhantomData,
        }
    }
}

impl HttpRequest<Ready> {
    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }

    pub fn build(self) -> Request {
        Request {
            url: self.url.unwrap(),
            method: self.method.unwrap(),
            headers: self.headers.unwrap_or_default(),
            body: self.body,
        }
    }
}
```

---

## Advanced Async Patterns

### Async task spawning with JoinSet

```rust
use tokio::task::JoinSet;
use tracing::Instrument;

async fn process_batch(items: Vec<Item>) -> Result<Vec<Result<Output>>> {
    let mut set = JoinSet::new();

    for item in items {
        let span = tracing::info_span!("process_item", item_id = %item.id);
        set.spawn(
            async move { process_item(item).await }.instrument(span)
        );
    }

    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(output) => results.push(output),
            Err(e) => {
                tracing::error!("Task panicked: {}", e);
                results.push(Err(ProcessError::TaskPanic));
            }
        }
    }

    Ok(results)
}
```

### Rate limiting with semaphore

```rust
use tokio::sync::Semaphore;
use std::sync::Arc;

struct RateLimiter {
    semaphore: Arc<Semaphore>,
}

impl RateLimiter {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub async fn execute<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let _permit = self.semaphore.acquire().await.expect("semaphore closed");
        f.await
    }
}
```

---

## Dependency Injection Patterns

### Trait-based storage abstraction

```rust
#[async_trait]
trait Storage: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
}

struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.data.write().await.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.data.write().await.remove(key);
        Ok(())
    }
}

fn create_storage(config: &Config) -> Arc<dyn Storage> {
    match config.storage_type.as_str() {
        "memory" => Arc::new(MemoryStorage::new()),
        "redis" => Arc::new(RedisStorage::new(&config.redis_url)),
        other => panic!("unknown storage type: {other}"),
    }
}
```

---

## Performance Patterns

### Zero-copy deserialization

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    event_type: &'a str,

    #[serde(borrow)]
    payload: &'a serde_json::value::RawValue,
}

fn parse_event(json: &str) -> Result<Event> {
    Ok(serde_json::from_str(json)?)
}
```

### Stack allocation with `smallvec`

```rust
use smallvec::SmallVec;

type SmallList = SmallVec<[Item; 8]>;

fn process_items() -> SmallList {
    let mut items = SmallList::new();
    items.push(item1);
    items.push(item2);
    items
}
```

This examples file provides deeper patterns that can be referenced when needed while keeping the main SKILL.md focused and actionable.
