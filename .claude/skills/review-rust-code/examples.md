# Review Rust Code - Extended Examples

This file contains additional detailed examples for reference. The main SKILL.md provides the core patterns; refer to this file when you need more context or advanced scenarios.

## AST Parsing Examples

### Complex SQL transformation using AST

```rust
use crate::parser::{Parser, Visitor, VisitorMut};
use crate::ast::{Statement, SelectStmt, Expr, TableRef};

// Example: Add table prefix to all table references
struct TablePrefixer {
    prefix: String,
}

impl VisitorMut for TablePrefixer {
    fn visit_table_ref(&mut self, table: &mut TableRef) {
        // Only modify if not already prefixed
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

// Example: Extract all column references for dependency analysis
struct ColumnExtractor {
    columns: Vec<String>,
    current_table: Option<String>,
}

impl Visitor for ColumnExtractor {
    fn visit_table_ref(&mut self, table: &TableRef) {
        self.current_table = Some(table.name.clone());
    }
    
    fn visit_column_ref(&mut self, column: &ColumnRef) {
        let full_name = if let Some(table) = &column.table {
            format!("{}.{}", table, column.name)
        } else if let Some(table) = &self.current_table {
            format!("{}.{}", table, column.name)
        } else {
            column.name.clone()
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

// Example: Validate that all referenced columns exist
fn validate_column_references(
    sql: &str,
    schema: &DatabaseSchema,
) -> Result<Vec<ValidationError>> {
    let ast = Parser::parse_sql(sql)?;
    let mut errors = Vec::new();
    
    let mut validator = ColumnValidator { schema, errors: &mut errors };
    ast.visit(&mut validator);
    
    Ok(errors)
}

struct ColumnValidator<'a> {
    schema: &'a DatabaseSchema,
    errors: &'a mut Vec<ValidationError>,
}

impl<'a> Visitor for ColumnValidator<'a> {
    fn visit_column_ref(&mut self, column: &ColumnRef) {
        if let Some(table) = &column.table {
            if !self.schema.table_has_column(table, &column.name) {
                self.errors.push(ValidationError::UnknownColumn {
                    table: table.clone(),
                    column: column.name.clone(),
                });
            }
        }
    }
}
```

### Jinja template AST manipulation

```rust
use crate::jinja::{Parser as JinjaParser, Template, Node};

// Example: Extract all variable references from template
fn extract_template_variables(template_str: &str) -> Result<HashSet<String>> {
    let template = JinjaParser::parse(template_str)?;
    let mut variables = HashSet::new();
    
    fn visit_node(node: &Node, vars: &mut HashSet<String>) {
        match node {
            Node::Variable(name) => {
                vars.insert(name.clone());
            }
            Node::For { var, .. } => {
                vars.insert(var.clone());
            }
            Node::If { condition, .. } => {
                extract_from_expr(condition, vars);
            }
            Node::Block(children) => {
                for child in children {
                    visit_node(child, vars);
                }
            }
            _ => {}
        }
    }
    
    visit_node(&template.root, &mut variables);
    Ok(variables)
}

// Example: Validate template against available context
fn validate_template_context(
    template_str: &str,
    available_vars: &HashSet<String>,
) -> Result<Vec<String>> {
    let required_vars = extract_template_variables(template_str)?;
    let missing: Vec<String> = required_vars
        .difference(available_vars)
        .cloned()
        .collect();
    
    Ok(missing)
}
```

## Advanced Error Handling Patterns

### Error hierarchies with context

```rust
use thiserror::Error;

// Domain-specific errors
#[derive(Error, Debug)]
pub enum UserError {
    #[error("User not found: {0}")]
    NotFound(String),
    
    #[error("User already exists: {0}")]
    AlreadyExists(String),
    
    #[error("Invalid user data")]
    Invalid(#[from] ValidationError),
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid email format: {0}")]
    Email(String),
    
    #[error("Password too weak")]
    WeakPassword,
}

// Application-level error that aggregates domain errors
#[derive(Error, Debug)]
pub enum AppError {
    #[error("User operation failed")]
    User(#[from] UserError),
    
    #[error("Database error")]
    Database(#[from] sqlx::Error),
    
    #[error("Configuration error")]
    Config(#[from] config::ConfigError),
    
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// HTTP layer converts to status codes
impl From<AppError> for axum::http::StatusCode {
    fn from(err: AppError) -> Self {
        match err {
            AppError::User(UserError::NotFound(_)) => StatusCode::NOT_FOUND,
            AppError::User(UserError::AlreadyExists(_)) => StatusCode::CONFLICT,
            AppError::User(UserError::Invalid(_)) => StatusCode::BAD_REQUEST,
            AppError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
```

## Advanced Typestate Patterns

### Multi-step builder with validation

```rust
use std::marker::PhantomData;

// States
struct NeedsUrl;
struct NeedsMethod;
struct NeedsHeaders;
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
    pub fn method(self, method: impl Into<String>) -> HttpRequest<NeedsHeaders> {
        HttpRequest {
            url: self.url,
            method: Some(method.into()),
            headers: self.headers,
            body: self.body,
            _state: PhantomData,
        }
    }
}

impl HttpRequest<NeedsHeaders> {
    pub fn headers(self, headers: HashMap<String, String>) -> HttpRequest<Ready> {
        HttpRequest {
            url: self.url,
            method: self.method,
            headers: Some(headers),
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
            headers: self.headers.unwrap(),
            body: self.body,
        }
    }
}

// Usage - compiler enforces correct order
let request = HttpRequest::new()
    .url("https://api.example.com")
    .method("POST")
    .headers(headers)
    .body(body)
    .build();
```

## Advanced Async Patterns

### Async task spawning with proper error handling

```rust
use tokio::task::JoinSet;
use tracing::Instrument;

async fn process_batch(items: Vec<Item>) -> Result<Vec<Result<Output>>> {
    let mut set = JoinSet::new();
    
    for item in items {
        let span = tracing::info_span!("process_item", item_id = %item.id);
        
        set.spawn(
            async move {
                process_item(item).await
            }
            .instrument(span)
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

### Retry logic with exponential backoff

```rust
use tokio::time::{sleep, Duration};

async fn retry_with_backoff<F, T, E>(
    mut f: F,
    max_attempts: u32,
    initial_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>>>>,
{
    let mut delay = initial_delay;
    
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt == max_attempts => return Err(e),
            Err(_) => {
                tracing::warn!(
                    attempt,
                    max_attempts,
                    delay_ms = delay.as_millis(),
                    "Retry attempt failed, backing off"
                );
                sleep(delay).await;
                delay *= 2;
            }
        }
    }
    
    unreachable!()
}

// Usage
let result = retry_with_backoff(
    || Box::pin(async { api_call().await }),
    3,
    Duration::from_millis(100),
).await?;
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
        let _permit = self.semaphore.acquire().await.unwrap();
        f.await
    }
}

// Usage
let limiter = RateLimiter::new(10); // Max 10 concurrent requests

for item in items {
    let limiter = limiter.clone();
    tokio::spawn(async move {
        limiter.execute(async {
            process_item(item).await
        }).await
    });
}
```

## Dependency Injection Patterns

### Manual DI container with OnceCell

```rust
use std::sync::OnceLock;

pub struct Dependencies {
    db: Arc<dyn Database + Send + Sync>,
    cache: Arc<dyn Cache + Send + Sync>,
    config: Config,
}

static DEPS: OnceLock<Dependencies> = OnceLock::new();

impl Dependencies {
    pub fn init(db: Arc<dyn Database + Send + Sync>, cache: Arc<dyn Cache + Send + Sync>, config: Config) {
        DEPS.set(Dependencies { db, cache, config })
            .expect("Dependencies already initialized");
    }
    
    pub fn get() -> &'static Dependencies {
        DEPS.get().expect("Dependencies not initialized")
    }
}

// Usage in application startup
#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let db = create_database(&config).await?;
    let cache = create_cache(&config).await?;
    
    Dependencies::init(db, cache, config);
    
    // Now any function can access dependencies
    run_server().await
}

async fn handler() -> Result<Response> {
    let deps = Dependencies::get();
    let user = deps.db.get_user(123).await?;
    Ok(Response::new(user))
}
```

### Scoped dependencies (per-request)

```rust
use axum::{
    extract::{Extension, State},
    middleware::{self, Next},
    response::Response,
};

#[derive(Clone)]
struct RequestContext {
    request_id: String,
    user_id: Option<u64>,
    db: Arc<dyn Database + Send + Sync>,
}

async fn request_context_middleware(
    State(db): State<Arc<dyn Database + Send + Sync>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let ctx = RequestContext {
        request_id,
        user_id: None,
        db,
    };
    
    req.extensions_mut().insert(ctx);
    next.run(req).await
}

// Handler
async fn get_user(
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<u64>,
) -> Result<Json<User>> {
    tracing::info!(
        request_id = %ctx.request_id,
        user_id = id,
        "Getting user"
    );
    
    let user = ctx.db.get_user(id).await?;
    Ok(Json(user))
}
```

## Advanced Trait Patterns

### Trait with multiple implementations (polymorphism)

```rust
// Storage abstraction
#[async_trait]
trait Storage: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
}

// In-memory implementation
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

// Redis implementation
struct RedisStorage {
    client: redis::Client,
}

#[async_trait]
impl Storage for RedisStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(conn.get(key).await?)
    }
    
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        conn.set(key, value).await?;
        Ok(())
    }
    
    async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        conn.del(key).await?;
        Ok(())
    }
}

// Usage with runtime selection
fn create_storage(config: &Config) -> Arc<dyn Storage> {
    match config.storage_type.as_str() {
        "memory" => Arc::new(MemoryStorage::new()),
        "redis" => Arc::new(RedisStorage::new(&config.redis_url)),
        _ => panic!("Unknown storage type"),
    }
}
```

## Testing Patterns

### Table-Based Testing Patterns

Table-based testing (also called parameterized testing) allows you to run the same test logic against multiple inputs, keeping tests DRY and easier to maintain.

#### Using `rstest` for parameterized tests

```rust
use rstest::rstest;

// Basic case enumeration
#[rstest]
#[case(2, 4)]
#[case(3, 9)]
#[case(4, 16)]
#[case(5, 25)]
fn test_square(#[case] input: i32, #[case] expected: i32) {
    assert_eq!(square(input), expected);
}

// With descriptive test names
#[rstest]
#[case::empty_string("", true)]
#[case::whitespace("   ", true)]
#[case::with_text("hello", false)]
fn test_is_blank(#[case] input: &str, #[case] expected: bool) {
    assert_eq!(is_blank(input), expected);
}

// Combining with fixtures
#[fixture]
fn test_db() -> Database {
    Database::new_test_instance()
}

#[rstest]
#[case("user1", Role::Admin)]
#[case("user2", Role::User)]
#[case("user3", Role::Guest)]
fn test_user_roles(test_db: Database, #[case] username: &str, #[case] expected_role: Role) {
    let user = test_db.get_user(username).unwrap();
    assert_eq!(user.role, expected_role);
}

// Testing error cases
#[rstest]
#[case::negative(-5, Err(ValidationError::Negative))]
#[case::zero(0, Err(ValidationError::Zero))]
#[case::too_large(1000, Err(ValidationError::TooLarge))]
#[case::valid(50, Ok(50))]
fn test_validate_age(#[case] age: i32, #[case] expected: Result<i32, ValidationError>) {
    assert_eq!(validate_age(age), expected);
}
```

#### Manual table-based tests

```rust
// Using Vec of tuples for test cases
#[test]
fn test_http_status_codes() {
    let test_cases = vec![
        (200, "OK", true),
        (201, "Created", true),
        (400, "Bad Request", false),
        (404, "Not Found", false),
        (500, "Internal Server Error", false),
    ];
    
    for (code, description, is_success) in test_cases {
        let status = HttpStatus::new(code, description);
        assert_eq!(
            status.is_success(),
            is_success,
            "Failed for status {}: {}",
            code,
            description
        );
    }
}

// Using structs for complex test cases
#[derive(Debug)]
struct JsonTestCase {
    name: &'static str,
    input: &'static str,
    expected: Result<User, JsonError>,
}

#[test]
fn test_json_parsing() {
    let test_cases = vec![
        JsonTestCase {
            name: "valid user",
            input: r#"{"id":1,"name":"Alice"}"#,
            expected: Ok(User { id: 1, name: "Alice".to_string() }),
        },
        JsonTestCase {
            name: "missing field",
            input: r#"{"id":1}"#,
            expected: Err(JsonError::MissingField("name")),
        },
        JsonTestCase {
            name: "invalid json",
            input: r#"{invalid}"#,
            expected: Err(JsonError::ParseError),
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

// Using macros for readability
macro_rules! assert_parse_eq {
    ($input:expr, $expected:expr) => {
        assert_eq!(
            parse_value($input),
            $expected,
            "Failed to parse: {}",
            $input
        );
    };
}

#[test]
fn test_value_parsing() {
    assert_parse_eq!("true", Ok(Value::Bool(true)));
    assert_parse_eq!("false", Ok(Value::Bool(false)));
    assert_parse_eq!("42", Ok(Value::Int(42)));
    assert_parse_eq!("3.14", Ok(Value::Float(3.14)));
    assert_parse_eq!("\"hello\"", Ok(Value::String("hello".to_string())));
    assert_parse_eq!("invalid", Err(ParseError::Unknown));
}
```

#### Async table-based tests

```rust
#[cfg(test)]
mod async_tests {
    use super::*;
    use rstest::rstest;
    
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
        let response = app
            .request(method, path)
            .await
            .unwrap();
        
        assert_eq!(response.status(), expected_status);
    }
    
    // Manual async table testing
    #[tokio::test]
    async fn test_database_operations() {
        let db = setup_test_db().await;
        
        let test_cases = vec![
            ("INSERT", "users", vec!["id", "name"], true),
            ("SELECT", "users", vec!["id"], true),
            ("UPDATE", "users", vec!["name"], true),
            ("DELETE", "invalid_table", vec![], false),
        ];
        
        for (operation, table, columns, should_succeed) in test_cases {
            let result = db.execute(operation, table, columns).await;
            assert_eq!(
                result.is_ok(),
                should_succeed,
                "Operation {} on {} should {} succeed",
                operation,
                table,
                if should_succeed { "" } else { "not" }
            );
        }
    }
}
```

#### Property-based testing with cleanup

```rust
use proptest::prelude::*;

// Combine table-based with property-based for comprehensive coverage
#[test]
fn test_encoding_roundtrip() {
    // Known edge cases (table-based)
    let edge_cases = vec![
        vec![],
        vec![0],
        vec![255],
        vec![0, 0, 0],
        vec![255, 255, 255],
    ];
    
    for case in edge_cases {
        let encoded = encode(&case);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(case, decoded, "Failed for edge case: {:?}", case);
    }
    
    // Random cases (property-based)
    proptest!(|(data: Vec<u8>)| {
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        prop_assert_eq!(data, decoded);
    });
}
```

#### Testing matrices for combinatorial cases

```rust
#[test]
fn test_permission_matrix() {
    let roles = vec![Role::Admin, Role::User, Role::Guest];
    let resources = vec![Resource::User, Resource::Post, Resource::Comment];
    let actions = vec![Action::Read, Action::Write, Action::Delete];
    
    // Define expected permissions
    let expected = [
        // Admin can do everything
        ((Role::Admin, Resource::User, Action::Read), true),
        ((Role::Admin, Resource::User, Action::Write), true),
        ((Role::Admin, Resource::User, Action::Delete), true),
        // Users can read and write their own posts
        ((Role::User, Resource::Post, Action::Read), true),
        ((Role::User, Resource::Post, Action::Write), true),
        ((Role::User, Resource::Post, Action::Delete), false),
        // Guests can only read
        ((Role::Guest, Resource::Post, Action::Read), true),
        ((Role::Guest, Resource::Post, Action::Write), false),
        ((Role::Guest, Resource::Post, Action::Delete), false),
    ];
    
    for ((role, resource, action), expected_allowed) in expected {
        let result = check_permission(role, resource, action);
        assert_eq!(
            result,
            expected_allowed,
            "Permission check failed: {:?} {:?} on {:?}",
            role,
            action,
            resource
        );
    }
}
```

### Integration test with test containers

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers::{clients, images};
    
    #[tokio::test]
    async fn test_database_integration() {
        // Start a real Postgres container
        let docker = clients::Cli::default();
        let postgres = docker.run(images::postgres::Postgres::default());
        
        let port = postgres.get_host_port_ipv4(5432);
        let url = format!("postgres://postgres@localhost:{}/postgres", port);
        
        let db = Database::connect(&url).await.unwrap();
        
        // Run actual tests against real database
        let user = User::new("test@example.com");
        db.save_user(&user).await.unwrap();
        
        let loaded = db.get_user(user.id).await.unwrap();
        assert_eq!(user.email, loaded.email);
    }
}
```

### Snapshot testing

```rust
use insta::assert_json_snapshot;

#[test]
fn test_api_response_format() {
    let user = User {
        id: 123,
        name: "John Doe".to_string(),
        email: "john@example.com".to_string(),
        created_at: DateTime::from_timestamp(1234567890, 0).unwrap(),
    };
    
    let response = UserResponse::from(user);
    
    // Creates a snapshot file on first run, compares on subsequent runs
    assert_json_snapshot!(response);
}
```

## Axum Production Patterns

### Complete API server setup

```rust
use axum::{
    middleware::{self, Next},
    routing::{get, post},
    Router,
};
use tower::{ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    compression::CompressionLayer,
    trace::TraceLayer,
    cors::CorsLayer,
};

async fn create_app(state: AppState) -> Router {
    // API v1 routes
    let api_v1 = Router::new()
        .route("/users", get(list_users).post(create_user))
        .route("/users/:id", get(get_user).put(update_user).delete(delete_user))
        .route("/health", get(health_check))
        .layer(
            ServiceBuilder::new()
                .layer(middleware::from_fn(auth_middleware))
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(TimeoutLayer::new(Duration::from_secs(30)))
        );
    
    // Main router with versioning
    Router::new()
        .nest("/api/v1", api_v1)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    
    // Initialize dependencies
    let config = Config::load()?;
    let db = create_database(&config).await?;
    let state = AppState { db };
    
    // Create app
    let app = create_app(state).await;
    
    // Graceful shutdown
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    
    tracing::info!("Server listening on {}", listener.local_addr()?);
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };
    
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };
    
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    
    tracing::info!("Shutdown signal received, starting graceful shutdown");
}
```

### Custom error responses

```rust
use axum::{
    response::{IntoResponse, Response},
    http::StatusCode,
    Json,
};
use serde::Serialize;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            AppError::User(UserError::NotFound(msg)) => {
                (StatusCode::NOT_FOUND, msg.clone())
            }
            AppError::User(UserError::AlreadyExists(msg)) => {
                (StatusCode::CONFLICT, msg.clone())
            }
            AppError::User(UserError::Invalid(e)) => {
                (StatusCode::BAD_REQUEST, e.to_string())
            }
            AppError::Database(e) => {
                tracing::error!("Database error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
            _ => {
                tracing::error!("Unexpected error: {}", self);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };
        
        let body = Json(ErrorResponse {
            error: error_message,
            details: None,
        });
        
        (status, body).into_response()
    }
}
```

## Performance Patterns

### Zero-copy deserialization with `serde`

```rust
use serde::Deserialize;

// Use lifetimes to borrow from the input
#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    event_type: &'a str,
    
    #[serde(borrow)]
    payload: &'a serde_json::value::RawValue,
}

// Zero-copy parsing
fn parse_event(json: &str) -> Result<Event> {
    Ok(serde_json::from_str(json)?)
}
```

### Using `smallvec` for stack allocation

```rust
use smallvec::SmallVec;

// Store up to 8 items on the stack, heap allocate if more
type SmallList = SmallVec<[Item; 8]>;

fn process_items() -> SmallList {
    let mut items = SmallList::new();
    // Most cases have fewer than 8 items - no heap allocation
    items.push(item1);
    items.push(item2);
    items
}
```

This examples file provides deeper patterns that can be referenced when needed while keeping the main SKILL.md focused and actionable.
