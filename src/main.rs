use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{MySql, Pool, Row};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// MySQL host
    #[arg(long, default_value = "localhost")]
    host: String,
    
    /// MySQL port
    #[arg(long, default_value = "3306")]
    port: u16,
    
    /// MySQL username
    #[arg(long)]
    username: String,
    
    /// MySQL password
    #[arg(long, default_value = "")]
    password: String,
    
    /// MySQL database name
    #[arg(long)]
    database: String,
}

// JSON-RPC structures
#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}



// MCP specific structures
#[derive(Debug, Serialize)]
struct ServerInfo {
    name: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    capabilities: ServerCapabilities,
    #[serde(rename = "serverInfo")]
    server_info: ServerInfo,
}

#[derive(Debug, Serialize)]
struct ServerCapabilities {
    tools: Option<ToolsCapability>,
}

#[derive(Debug, Serialize)]
struct ToolsCapability {}

#[derive(Debug, Serialize)]
struct Tool {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct ToolsList {
    tools: Vec<Tool>,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    arguments: Value,
}

#[derive(Debug, Deserialize)]
struct SchemaArguments {
    table_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args = Args::parse();
    
    // Build database URL from command line arguments
    let database_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        args.username, args.password, args.host, args.port, args.database
    );
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Set up stdio
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Send logs to stderr to avoid interfering with JSON-RPC communication
    eprintln!("MCP MySQL Server started");

    // Process incoming messages
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(request) => {
                let response = handle_request(request, &pool).await;
                let response_str = serde_json::to_string(&response)?;
                stdout.write_all(response_str.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            Err(e) => {
                eprintln!("Failed to parse request: {e}");
                let error_response = JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: None,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32700,
                        message: "Parse error".to_string(),
                        data: None,
                    }),
                };
                let response_str = serde_json::to_string(&error_response)?;
                stdout.write_all(response_str.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
        }
    }

    Ok(())
}

async fn handle_request(request: JsonRpcRequest, pool: &Pool<MySql>) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => {
            eprintln!("Handling initialize request");
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: Some(json!(InitializeResult {
                    protocol_version: "0.1.0".to_string(),
                    capabilities: ServerCapabilities {
                        tools: Some(ToolsCapability {}),
                    },
                    server_info: ServerInfo {
                        name: "mcp-server-mysql".to_string(),
                        version: "0.1.0".to_string(),
                    },
                })),
                error: None,
            }
        }
        "initialized" => {
            eprintln!("Client initialized");
            // This is a notification, no response needed
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: Some(json!({})),
                error: None,
            }
        }
        "tools/list" => {
            eprintln!("Listing available tools");
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: Some(json!(ToolsList {
                    tools: vec![Tool {
                        name: "mysql".to_string(),
                        description: "Retrieve MySQL database schema information for tables"
                            .to_string(),
                        input_schema: json!({
                            "type": "object",
                            "properties": {
                                "table_name": {
                                    "type": "string",
                                    "description": "Name of the table to inspect, or 'all-tables' to get all table schemas"
                                }
                            },
                            "required": ["table_name"]
                        }),
                    }],
                })),
                error: None,
            }
        }
        "tools/call" => {
            eprintln!("Handling tool call");
            match request.params {
                Some(params) => match serde_json::from_value::<ToolCallParams>(params) {
                    Ok(tool_params) => {
                        if tool_params.name == "mysql" {
                            match serde_json::from_value::<SchemaArguments>(tool_params.arguments) {
                                Ok(args) => get_schema(request.id, args.table_name, pool).await,
                                Err(e) => JsonRpcResponse {
                                    jsonrpc: "2.0".to_string(),
                                    id: request.id,
                                    result: None,
                                    error: Some(JsonRpcError {
                                        code: -32602,
                                        message: format!("Invalid query arguments: {e}"),
                                        data: None,
                                    }),
                                },
                            }
                        } else {
                            JsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                id: request.id,
                                result: None,
                                error: Some(JsonRpcError {
                                    code: -32601,
                                    message: format!("Unknown tool: {}", tool_params.name),
                                    data: None,
                                }),
                            }
                        }
                    }
                    Err(e) => JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: request.id,
                        result: None,
                        error: Some(JsonRpcError {
                            code: -32602,
                            message: format!("Invalid tool call parameters: {e}"),
                            data: None,
                        }),
                    },
                },
                None => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: request.id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32602,
                        message: "Missing parameters".to_string(),
                        data: None,
                    }),
                },
            }
        }
        _ => {
            eprintln!("Unknown method: {}", request.method);
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                    data: None,
                }),
            }
        }
    }
}

async fn get_schema(
    id: Option<Value>,
    table_name: String,
    pool: &Pool<MySql>,
) -> JsonRpcResponse {
    eprintln!("Getting schema for: {table_name}");
    
    if table_name == "all-tables" {
        // Get all table schemas
        match get_all_table_schemas(pool).await {
            Ok(schemas) => {
                JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: Some(json!({
                        "content": [{
                            "type": "text",
                            "text": format!("Retrieved schemas for {} tables.", schemas.len())
                        }],
                        "schemas": schemas
                    })),
                    error: None,
                }
            }
            Err(e) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32603,
                    message: format!("Failed to get table schemas: {e}"),
                    data: None,
                }),
            },
        }
    } else {
        // Get single table schema
        match get_table_schema(pool, &table_name).await {
            Ok(schema) => {
                JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: Some(json!({
                        "content": [{
                            "type": "text",
                            "text": format!("Retrieved schema for table '{}'.", table_name)
                        }],
                        "schema": schema
                    })),
                    error: None,
                }
            }
            Err(e) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32603,
                    message: format!("Failed to get schema for table '{table_name}': {e}"),
                    data: None,
                }),
            },
        }
    }
}

async fn get_table_schema(pool: &Pool<MySql>, table_name: &str) -> Result<Value, sqlx::Error> {
    // Get table information
    let table_info_query = format!("SELECT * FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = DATABASE()");
    let table_info = sqlx::query(&table_info_query).fetch_optional(pool).await?;
    
    if table_info.is_none() {
        return Err(sqlx::Error::RowNotFound);
    }
    
    // Get column information
    let columns_query = format!(
        "SELECT column_name, data_type, is_nullable, column_default, column_key, extra, column_comment 
         FROM information_schema.columns 
         WHERE table_name = '{table_name}' AND table_schema = DATABASE() 
         ORDER BY ordinal_position"
    );
    let columns = sqlx::query(&columns_query).fetch_all(pool).await?;
    
    // Get indexes
    let indexes_query = format!("SHOW INDEX FROM `{table_name}`");
    let indexes = sqlx::query(&indexes_query).fetch_all(pool).await?;
    
    let column_info: Vec<Value> = columns
        .into_iter()
        .map(|row| {
            json!({
                "name": row.try_get::<String, _>("column_name").unwrap_or_default(),
                "type": row.try_get::<String, _>("data_type").unwrap_or_default(),
                "nullable": row.try_get::<String, _>("is_nullable").unwrap_or_default() == "YES",
                "default": row.try_get::<Option<String>, _>("column_default").unwrap_or_default(),
                "key": row.try_get::<String, _>("column_key").unwrap_or_default(),
                "extra": row.try_get::<String, _>("extra").unwrap_or_default(),
                "comment": row.try_get::<String, _>("column_comment").unwrap_or_default(),
            })
        })
        .collect();
    
    let index_info: Vec<Value> = indexes
        .into_iter()
        .map(|row| {
            json!({
                "name": row.try_get::<String, _>("Key_name").unwrap_or_default(),
                "column": row.try_get::<String, _>("Column_name").unwrap_or_default(),
                "unique": row.try_get::<i32, _>("Non_unique").unwrap_or(1) == 0,
                "type": row.try_get::<String, _>("Index_type").unwrap_or_default(),
            })
        })
        .collect();
    
    Ok(json!({
        "table_name": table_name,
        "columns": column_info,
        "indexes": index_info
    }))
}

async fn get_all_table_schemas(pool: &Pool<MySql>) -> Result<Vec<Value>, sqlx::Error> {
    // Get all tables in the current database
    let tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'";
    let tables = sqlx::query(tables_query).fetch_all(pool).await?;
    
    let mut schemas = Vec::new();
    for table_row in tables {
        let table_name: String = table_row.try_get("table_name")?;
        match get_table_schema(pool, &table_name).await {
            Ok(schema) => schemas.push(schema),
            Err(e) => {
                eprintln!("Failed to get schema for table {table_name}: {e}");
                // Continue with other tables
            }
        }
    }
    
    Ok(schemas)
}