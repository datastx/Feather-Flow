# Review Rust Memory - Extended Examples

Additional detailed examples for memory optimization. The main SKILL.md provides the core patterns; refer to this file for deeper scenarios and real-world refactoring.

## String Allocation Reduction

### Before/After: Log message formatting

```rust
// BAD — 3 allocations per log line in a hot loop
fn log_metrics(metrics: &[Metric]) {
    for m in metrics {
        let tag = format!("{}:{}", m.namespace, m.name);     // alloc 1
        let value = m.value.to_string();                       // alloc 2
        let line = format!("[{}] {} = {}", m.timestamp, tag, value); // alloc 3
        output.write_all(line.as_bytes()).unwrap();
    }
}

// GOOD — 0 per-iteration allocations
fn log_metrics(metrics: &[Metric]) {
    use std::fmt::Write;
    let mut buf = String::with_capacity(256);

    for m in metrics {
        buf.clear();
        write!(&mut buf, "[{}] {}:{} = {}", m.timestamp, m.namespace, m.name, m.value)
            .unwrap();
        output.write_all(buf.as_bytes()).unwrap();
    }
}
```

### Before/After: Path construction

```rust
// BAD — repeated allocation building paths
fn resolve_model_paths(base: &Path, names: &[&str]) -> Vec<PathBuf> {
    names.iter()
        .map(|name| {
            let dir = base.join("models").join(name);  // alloc
            dir.join(format!("{}.sql", name))           // alloc + alloc (format!)
        })
        .collect()
}

// GOOD — reuse PathBuf
fn resolve_model_paths(base: &Path, names: &[&str]) -> Vec<PathBuf> {
    let mut path = base.join("models");
    let models_len = path.as_os_str().len();

    names.iter()
        .map(|name| {
            path.push(name);
            path.push(name);
            path.set_extension("sql");
            let result = path.clone();
            // Reset to models/ base for next iteration
            let mut p = path.clone();
            // Truncate is cleaner
            while p.as_os_str().len() > models_len {
                p.pop();
            }
            path = p;
            result
        })
        .collect()
}

// BEST — just build it simply when collect handles allocation
fn resolve_model_paths(base: &Path, names: &[&str]) -> Vec<PathBuf> {
    let models = base.join("models");
    names.iter()
        .map(|name| models.join(name).join(format!("{name}.sql")))
        .collect()
}
// Note: sometimes clarity wins over micro-optimization.
// Profile to know if this path construction is actually a bottleneck.
```

---

## Collection Patterns

### Avoiding intermediate collections

```rust
// BAD — two intermediate Vecs
fn get_active_user_emails(users: &[User]) -> Vec<String> {
    let active: Vec<&User> = users.iter()
        .filter(|u| u.is_active)
        .collect();                      // intermediate Vec — unnecessary

    let emails: Vec<String> = active.iter()
        .map(|u| u.email.clone())
        .collect();                      // final Vec

    emails
}

// GOOD — single iterator chain, one allocation
fn get_active_user_emails(users: &[User]) -> Vec<String> {
    users.iter()
        .filter(|u| u.is_active)
        .map(|u| u.email.clone())
        .collect()
}

// EVEN BETTER — return references if caller doesn't need ownership
fn get_active_user_emails(users: &[User]) -> Vec<&str> {
    users.iter()
        .filter(|u| u.is_active)
        .map(|u| u.email.as_str())
        .collect()
}
```

### HashMap entry API to avoid double lookup + allocation

```rust
// BAD — look up twice, may allocate key string twice
fn count_words(text: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for word in text.split_whitespace() {
        if counts.contains_key(word) {
            *counts.get_mut(word).unwrap() += 1;
        } else {
            counts.insert(word.to_string(), 1);  // allocates key
        }
    }
    counts
}

// GOOD — entry API: one lookup, allocation only on first occurrence
fn count_words(text: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::with_capacity(text.len() / 5); // estimate
    for word in text.split_whitespace() {
        *counts.entry(word.to_string()).or_insert(0) += 1;
    }
    counts
}

// BEST — borrow the key to avoid allocating for existing entries
// (requires nightly or using hashbrown directly for entry_ref)
```

---

## Struct Layout Optimization

### Real-world example: AST node

```rust
// BAD — 72 bytes per node (with padding)
struct AstNode {
    is_nullable: bool,       // 1 + 7 padding
    source_offset: u64,      // 8
    kind: NodeKind,          // 1 + 7 padding (if small enum)
    parent: Option<usize>,   // 16 (8 + 8 for niche)
    children: Vec<usize>,    // 24
}

// GOOD — 48 bytes per node (fields reordered)
struct AstNode {
    children: Vec<usize>,    // 24 (align 8)
    source_offset: u64,      // 8 (align 8)
    parent: Option<usize>,   // 16 (align 8) — but Option<NonZeroUsize> would be 8
    kind: NodeKind,          // 1 (align 1)
    is_nullable: bool,       // 1 (align 1)
    // 6 bytes padding to align to 8
}

// BEST — 40 bytes with NonZero optimization
use std::num::NonZeroUsize;

struct AstNode {
    children: Vec<usize>,         // 24
    source_offset: u64,           // 8
    parent: Option<NonZeroUsize>, // 8 (niche optimization: 0 = None)
    kind: NodeKind,               // 1
    is_nullable: bool,            // 1
}

// Always verify with a test
#[test]
fn ast_node_size() {
    assert_eq!(std::mem::size_of::<AstNode>(), 40);
}
```

### Enum size optimization

```rust
// BAD — all variants pay for the largest one
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),            // 24 bytes
    List(Vec<Value>),        // 24 bytes
    Map(HashMap<String, Value>),  // 48 bytes ← all variants are 48+8(tag) = 56 bytes
}

// GOOD — Box the large variants
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),                       // 24 bytes
    List(Box<Vec<Value>>),              // 8 bytes
    Map(Box<HashMap<String, Value>>),   // 8 bytes
}
// Now 24 + 8(tag) = 32 bytes per variant

// Verify
#[test]
fn value_size() {
    assert_eq!(std::mem::size_of::<Value>(), 32);
}
```

---

## Arena Allocation for Batch Processing

### SQL AST parsing with bumpalo

```rust
use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;

struct SqlParser<'a> {
    arena: &'a Bump,
}

impl<'a> SqlParser<'a> {
    fn parse_select(&self, tokens: &[Token]) -> &'a SelectStmt<'a> {
        let columns = BumpVec::new_in(self.arena);
        let stmt = self.arena.alloc(SelectStmt {
            columns,
            from: None,
            where_clause: None,
        });
        // ... populate stmt ...
        stmt
    }
}

// Usage — all AST nodes freed at once
fn analyze_sql(sql: &str) -> AnalysisResult {
    let arena = Bump::new();
    let parser = SqlParser { arena: &arena };
    let ast = parser.parse_select(&tokenize(sql));
    let result = analyze(ast);
    result
    // arena dropped here — bulk deallocation, no per-node free
}
```

### DAG processing with arena

```rust
use bumpalo::Bump;

fn process_dag(models: &[Model]) -> DagResult {
    let arena = Bump::with_capacity(models.len() * 256); // estimate

    // All intermediate data lives in the arena
    let nodes: BumpVec<DagNode> = BumpVec::with_capacity_in(models.len(), &arena);
    let edges: BumpVec<(usize, usize)> = BumpVec::new_in(&arena);
    let labels: BumpVec<&str> = BumpVec::new_in(&arena);

    // ... build DAG ...

    let result = compute_result(&nodes, &edges);
    result
    // arena dropped — one deallocation instead of thousands
}
```

---

## Memory-Efficient I/O Patterns

### Streaming instead of loading everything

```rust
// BAD — reads entire file into memory
fn count_lines(path: &Path) -> Result<usize> {
    let contents = std::fs::read_to_string(path)?;  // could be GBs
    Ok(contents.lines().count())
}

// GOOD — streaming with buffered reader
fn count_lines(path: &Path) -> Result<usize> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count())
}

// GOOD — memory-mapped for random access on large files
fn search_file(path: &Path, needle: &[u8]) -> Result<Option<usize>> {
    let file = std::fs::File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    Ok(mmap.windows(needle.len()).position(|w| w == needle))
}
```

### Bounded buffers for network I/O

```rust
// BAD — unbounded read into memory
async fn read_body(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await?;  // attacker sends 10GB → OOM
    Ok(body)
}

// GOOD — bounded read with limit
async fn read_body(stream: &mut TcpStream, max_size: usize) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(max_size.min(8192));
    let mut total = 0;
    let mut buf = [0u8; 8192];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 { break; }
        total += n;
        if total > max_size {
            return Err(anyhow!("body exceeds {max_size} bytes"));
        }
        body.extend_from_slice(&buf[..n]);
    }
    Ok(body)
}
```

---

## Feather-Flow: Schema Propagation Pipeline

The heaviest memory path in Feather-Flow. Models are processed in topological order; each model's schema depends on its upstream models.

### Current allocation flow (per model)

```
build_schema_catalog()
  └─ For each model:
       ├─ rel_schema.clone() → schema_catalog      (deep clone)
       └─ rel_schema        → yaml_schemas          (move)

yaml_string_map conversion
  └─ For each entry in yaml_schemas:
       ├─ k.to_string()                             (String alloc)
       └─ v.clone()                                 (deep clone AGAIN)

propagate_schemas()
  └─ For each model in topo order:
       ├─ FeatherFlowProvider::new()                (45 UDF HashMap entries)
       │    └─ rel_schema_to_arrow() per upstream   (Arrow Field allocs)
       ├─ DataFusion plan creation                  (LogicalPlan tree)
       ├─ inferred_schema extraction                (new RelSchema)
       └─ catalog.insert(name.clone(), schema.clone()) (String + deep clone)

plan_pass_manager.run()
  └─ For each model × each pass:
       └─ Vec<Diagnostic> with owned String fields  (model name, message, hint)
```

### Optimized flow

```
build_schema_catalog()
  └─ For each model:
       ├─ Arc::new(rel_schema) → shared
       ├─ Arc::clone() → schema_catalog
       └─ Arc::clone() → yaml_schemas

yaml_string_map conversion
  └─ For each entry: (k.as_str(), Arc::clone(v))   (zero deep clones)

propagate_schemas()
  └─ Build scalar_fns + aggregate_fns ONCE
  └─ For each model in topo order:
       ├─ Provider borrows pre-built fn registries  (no per-model alloc)
       │    └─ arrow_cache.get() or convert once    (cached)
       ├─ DataFusion plan creation                  (same)
       ├─ inferred_schema extraction                (same)
       └─ catalog.insert(name, Arc::new(schema))    (Arc, not deep clone)

plan_pass_manager.run()
  └─ Same (diagnostics are inherently per-issue)
```

### Compile pipeline: hook cloning

```rust
// BAD — clones hook Vecs per model (current)
for model in &project.models {
    let mut pre_hook = project.config.pre_hook.clone();      // alloc
    pre_hook.extend(manifest_model.pre_hook.clone());         // alloc
    let mut post_hook = manifest_model.post_hook.clone();     // alloc
    post_hook.extend(project.config.post_hook.clone());       // alloc
}

// GOOD — pre-merge project-level hooks once, borrow per model
let project_pre_hooks = &project.config.pre_hook;
let project_post_hooks = &project.config.post_hook;

for model in &project.models {
    // Only allocate when model has its own hooks to merge
    let pre_hooks: Cow<[String]> = if manifest_model.pre_hook.is_empty() {
        Cow::Borrowed(project_pre_hooks)
    } else {
        let mut merged = project_pre_hooks.clone();
        merged.extend(manifest_model.pre_hook.iter().cloned());
        Cow::Owned(merged)
    };
}
```

### Dependency extraction: normalize_table_name

```rust
// BAD — allocates String per dependency (current)
fn normalize_table_name(name: &str) -> String {
    name.split('.').next_back().unwrap_or(name).to_string()
}

// GOOD — return a borrow, caller decides about ownership
fn normalize_table_name(name: &str) -> &str {
    name.split('.').next_back().unwrap_or(name)
}
```

### Diagnostic model names

```rust
// BAD — clones model name String into every diagnostic
for issue in &issues {
    diagnostics.push(Diagnostic {
        model: model_name.clone(),  // same string cloned N times
        message: format!("..."),
        ..
    });
}

// GOOD — Arc<str> for shared model identity
let model_arc: Arc<str> = Arc::from(model_name.as_str());
for issue in &issues {
    diagnostics.push(Diagnostic {
        model: Arc::clone(&model_arc),  // ref count bump
        message: format!("..."),
        ..
    });
}
```

---

## Decision Matrix

When choosing a memory optimization strategy:

| Symptom | Likely cause | Tool to diagnose | Fix |
|---------|-------------|------------------|-----|
| High peak RSS | Large data loaded at once | `heaptrack` | Streaming, arena, early drop |
| RSS grows over time | Fragmentation or leaks | `jemalloc stats`, `heaptrack` | Switch allocator, arena batching |
| Slow hot loop | Allocation per iteration | `dhat-rs` | Buffer reuse, `SmallVec`, `Cow` |
| Large struct size | Padding, oversized fields | `size_of` / `layout` | Reorder fields, `Box` variants |
| Millions of small identical strings | String duplication | `dhat-rs` top alloc sites | Interning, `&'static str`, enum |
| `Vec<Vec<T>>` shows up in profile | Scattered allocations | `dhat-rs` | Flatten with offset array |
