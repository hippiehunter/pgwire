# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgwire is a Rust library implementing the PostgreSQL wire protocol v3, enabling developers to build PostgreSQL-compatible servers and clients. It provides low-level protocol handling that others can build upon, similar to how Hyper works for HTTP.

## Common Development Commands

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run clippy lints (CI enforces these)
cargo clippy -- -D warnings

# Format code
cargo fmt

# Run a specific test
cargo test test_name

# Run examples (requires feature flags)
cargo run --features _sqlite --example sqlite
cargo run --features _duckdb --example duckdb
cargo run --example server
cargo run --example client

# Run integration tests (tests compatibility with various clients)
./tests-integration/test.sh
```

## Architecture

The codebase follows a three-layer architecture:

1. **Protocol Layer** (`src/messages/`): Low-level message definitions and encoding/decoding
2. **Handler Layer**: Traits with `on_` prefixed methods for different protocol phases
3. **API Layer** (`src/api/`): High-level APIs like `AuthSource`, `QueryParser`, and `do_` methods

### Key Components

- **messages**: Protocol message types and codec implementation
- **api/auth**: Authentication mechanisms (cleartext, MD5, SCRAM)
- **api/query**: Query processing for simple and extended protocols
- **api/results**: Result set building and encoding
- **tokio**: Async server/client implementations with TLS support
- **types**: PostgreSQL type conversion utilities

### Handler Traits

Implementers customize behavior by implementing these traits:
- `StartupHandler`: Connection handshake and authentication
- `SimpleQueryHandler`: Text SQL queries (psql compatibility)
- `ExtendedQueryHandler`: Prepared statements with binary encoding
- `CopyHandler`: Bulk data transfer (COPY protocol)
- `ErrorHandler`: Error and notice message handling

## Important Notes

- Minimum Rust version: 1.75
- Default crypto backend: aws-lc-rs (can switch to ring with feature flags)
- All public API additions should be exported in `src/lib.rs`
- The library aims for protocol compatibility, not PostgreSQL feature parity
- Examples in `/examples` serve as both documentation and integration tests