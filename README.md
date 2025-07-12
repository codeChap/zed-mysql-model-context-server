# Zed MySQL Context Server

A Model Context Server for MySQL that integrates with Zed AI assistant.

## Prerequisites

- Rust (latest stable version)
- MySQL/MariaDB database
- Zed

## Installation

1. Clone and build:
```bash
git clone <repository-url>
cd mcp-server-mysql
cargo build --release
```

2. Configure Zed (see Configuration section)

## Configuration

Click on the "Toggle Agent Menu" -> "Add custom Server"

```json
{
  /// The name of your MCP server
  "mysql-mcp-server": {
    /// The command which runs the MCP server
    "command": "/path/to/mcp-server-mysql/target/release/mcp-server-mysql",
    /// The arguments to pass to the MCP server
    "args": [
        "--username", "your_username",
        "--password", "your_password",
        "--database", "your_database",
        "--host", "localhost",
        "--port", "3306"
    ],
    /// The environment variables to set
    "env": {}
  }
}
```

### Options

- `--host <HOST>`: MySQL host (default: localhost)
- `--port <PORT>`: MySQL port (default: 3306)
- `--username <USERNAME>`: MySQL username (required)
- `--password <PASSWORD>`: MySQL password (default: empty)
- `--database <DATABASE>`: MySQL database name (required)

### Logging

The server uses standard Rust logging. Control log levels with the `RUST_LOG` environment variable:

- `RUST_LOG=error` - Only show errors
- `RUST_LOG=warn` - Show warnings and errors  
- `RUST_LOG=info` - Show info, warnings and errors (recommended for production)
- `RUST_LOG=debug` - Show all messages including detailed debug info

### Testing

Test manually with your database:

```bash
# Test initialization (with info logging)
RUST_LOG=info echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | cargo run --bin mcp-server-mysql -- --username admin --database mydb

# Test schema retrieval (quiet mode)
echo '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"mysql","arguments":{"table_name":"users"}}}' | cargo run --bin mcp-server-mysql -- --username admin --database mydb

# Test with debug output (for troubleshooting)
RUST_LOG=debug echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | cargo run --bin mcp-server-mysql -- --username admin --database mydb
```

Or use the test script:

```bash
./test-stdio.sh
```

### Building

```bash
# Development
cargo build

# Production
cargo build --release
```

## License

Apache-2.0
