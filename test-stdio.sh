#!/bin/bash

# Test script for the stdio-based MCP server

# Database connection settings - modify these for your database
DB_HOST="localhost"
DB_PORT="3306"
DB_USER="admin"
DB_PASS=""
DB_NAME="wts"

# Build the command line arguments
if [ -n "$DB_PASS" ]; then
    DB_ARGS="--host $DB_HOST --port $DB_PORT --username $DB_USER --password $DB_PASS --database $DB_NAME"
else
    DB_ARGS="--host $DB_HOST --port $DB_PORT --username $DB_USER --database $DB_NAME"
fi

echo "Testing MCP MySQL Server with database: $DB_NAME"
echo "Connection: $DB_USER@$DB_HOST:$DB_PORT"
echo ""

# Initialize connection
echo "1. Initializing connection..."
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"test-client","version":"1.0.0"}}}' | cargo run --bin mcp-server-mysql -- $DB_ARGS

echo ""
echo "2. Sending initialized notification..."
# Send initialized notification
echo '{"jsonrpc":"2.0","method":"initialized"}' | cargo run --bin mcp-server-mysql -- $DB_ARGS

echo ""
echo "3. Listing available tools..."
# List available tools
echo '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | cargo run --bin mcp-server-mysql -- $DB_ARGS

echo ""
echo "4. Testing schema retrieval for all tables..."
# Test schema retrieval for all tables
echo '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"mysql","arguments":{"table_name":"all-tables"}}}' | cargo run --bin mcp-server-mysql -- $DB_ARGS