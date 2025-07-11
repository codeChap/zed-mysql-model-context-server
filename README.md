# Zed MySQL Context Server

A Model Context Server for MySQL that integrates with Zed AI assistant.

Adds a `/mysql` slash command to inspect database schemas directly from Zed's Assistant Panel.

**Zed automatically manages the server** - no manual startup required!

## Features

- Inspect MySQL database schemas through `/mysql` commands
- Retrieve table structure, columns, types, and indexes
- Get schema information for individual tables or all tables
- Automatic server lifecycle management by Zed
- Connection pooling for efficient database access

## Prerequisites

- Rust (latest stable version)
- MySQL/MariaDB database
- Zed editor with MCP support

## Installation

1. Clone and build:
```bash
git clone <repository-url>
cd mcp-server-mysql
cargo build --release
```

2. Configure Zed (see Configuration section)

## Configuration

Add to your Zed `settings.json`:

```json
{
  "context_servers": {
    "mysql-context-server": {
      "command": "cargo",
      "args": [
        "run",
        "--bin",
        "mcp-server-mysql",
        "--",
        "--username",
        "your_username",
        "--password",
        "your_password",
        "--database",
        "your_database"
      ],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

For production, use the compiled binary:

```json
{
  "context_servers": {
    "mysql-context-server": {
      "command": "/path/to/mcp-server-mysql/target/release/mcp-server-mysql",
      "args": [
        "--username", "your_username",
        "--password", "your_password", 
        "--database", "your_database",
        "--host", "localhost",
        "--port", "3306"
      ]
    }
  }
}
```

### Options

- `--host <HOST>`: MySQL host (default: localhost)
- `--port <PORT>`: MySQL port (default: 3306)
- `--username <USERNAME>`: MySQL username (required)
- `--password <PASSWORD>`: MySQL password (default: empty)
- `--database <DATABASE>`: MySQL database name (required)

## Usage

Once configured, use these commands in Zed's assistant:

- `/mysql users` - Get schema for the users table
- `/mysql all-tables` - Get schemas for all tables
- `/mysql orders` - Get schema for the orders table

## Development

### Testing

Test manually with your database:

```bash
# Test initialization
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | cargo run --bin mcp-server-mysql -- --username admin --database mydb

# Test schema retrieval
echo '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"mysql","arguments":{"table_name":"users"}}}' | cargo run --bin mcp-server-mysql -- --username admin --database mydb
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

## Security

- Only schema inspection (no data modification)
- Connection pooling with max 5 connections
- Keep database credentials secure
- No user data retrieved, only schema information

## License

Apache-2.0
