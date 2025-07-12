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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeParams {
    initialization_options: Option<InitializationOptions>,
}

#[derive(Debug, Deserialize)]
struct InitializationOptions {
    settings: Option<ServerSettings>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerSettings {
    database_url: Option<String>,
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
struct ToolsCapability {
    #[serde(rename = "listChanged")]
    list_changed: bool,
}

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
    
    // Defer database connection until initialize request is received
    let mut pool: Option<Pool<MySql>> = None;

    // Set up stdio
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Send logs to stderr to avoid interfering with JSON-RPC communication
    eprintln!("MCP MySQL Server started and ready to accept connections");
    eprintln!("Server args: host={}, port={}, username={}, database={}", 
              args.host, args.port, args.username, args.database);
    eprintln!("Server PID: {}", std::process::id());
    eprintln!("Environment variables:");
    for (key, value) in std::env::vars() {
        if key.contains("MYSQL") || key.contains("DATABASE") || key.contains("MCP") {
            eprintln!("  {}: {}", key, value);
        }
    }
    eprintln!("Current working directory: {:?}", std::env::current_dir());

    // Process incoming messages with improved error handling
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }

                eprintln!("Received message (len={}): {}", line.len(), line);
                eprintln!("Message bytes: {:?}", line.as_bytes());
                match serde_json::from_str::<JsonRpcRequest>(&line) {
                    Ok(request) => {
                        eprintln!("Parsed request: method={}, id={:?}", request.method, request.id);
                        // Handle notifications (no response needed)
                        if request.method == "notifications/initialized" || request.method == "initialized" {
                            eprintln!("Received initialization notification: {}", request.method);
                            continue;
                        }
                        
                        let response = handle_request(request, &mut pool, &args).await;
                        match serde_json::to_string(&response) {
                            Ok(response_str) => {
                                if let Err(e) = write_response(&mut stdout, &response_str).await {
                                    eprintln!("Failed to write response: {e}");
                                    // Continue processing other requests
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to serialize response: {e}");
                                // Send a generic error response
                                let error_response = create_error_response(None, -32603, "Internal error");
                                if let Ok(error_str) = serde_json::to_string(&error_response) {
                                    let _ = write_response(&mut stdout, &error_str).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse request: {e}");
                        let error_response = create_error_response(None, -32700, "Parse error");
                        if let Ok(response_str) = serde_json::to_string(&error_response) {
                            let _ = write_response(&mut stdout, &response_str).await;
                        }
                    }
                }
            }
            Ok(None) => {
                // stdin closed, this is normal when client disconnects
                eprintln!("stdin closed - client disconnected, shutting down server");
                break;
            }
            Err(e) => {
                eprintln!("Error reading from stdin: {e} (error kind: {:?})", e.kind());
                // Add more context about the error
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    eprintln!("Unexpected EOF - client may have terminated");
                    break;
                }
                // Continue trying to read in case of transient errors
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    eprintln!("MCP MySQL Server shutdown complete");
    Ok(())
}

async fn write_response(stdout: &mut tokio::io::Stdout, response: &str) -> Result<(), Box<dyn std::error::Error>> {
    stdout.write_all(response.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

async fn connect_with_retry(database_url: &str) -> Result<Pool<MySql>, Box<dyn std::error::Error>> {
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 5;
    const RETRY_DELAY_MS: u64 = 1000;
    
    loop {
        match sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                eprintln!("Successfully connected to MySQL database");
                return Ok(pool);
            }
            Err(e) => {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    eprintln!("Failed to connect to database after {} retries: {}", MAX_RETRIES, e);
                    return Err(e.into());
                }
                eprintln!("Database connection failed (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                eprintln!("Retrying in {}ms...", RETRY_DELAY_MS);
                tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }
}

fn create_error_response(id: Option<Value>, code: i32, message: &str) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.to_string(),
            data: None,
        }),
    }
}

async fn handle_request(
    request: JsonRpcRequest,
    pool: &mut Option<Pool<MySql>>,
    args: &Args,
) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => {
            eprintln!("Handling initialize request with params: {:?}", request.params);

            // Extract database_url from initializationOptions, fallback to args
            let db_url_from_opts = request
                .params
                .as_ref()
                .and_then(|params| serde_json::from_value::<InitializeParams>(params.clone()).ok())
                .and_then(|opts| opts.initialization_options)
                .and_then(|init_opts| init_opts.settings)
                .and_then(|settings| settings.database_url);

            let database_url = match db_url_from_opts {
                Some(url) => {
                    eprintln!("Using database_url from initializationOptions: {}", url);
                    url
                }
                None => {
                    let url = format!(
                        "mysql://{}:{}@{}:{}/{}",
                        args.username, args.password, args.host, args.port, args.database
                    );
                    eprintln!("Using database_url from command-line arguments: mysql://{}:***@{}:{}/{}", 
                             args.username, args.host, args.port, args.database);
                    url
                }
            };

            eprintln!("Attempting database connection...");
            match connect_with_retry(&database_url).await {
                Ok(new_pool) => {
                    eprintln!("Database connection successful!");
                    *pool = Some(new_pool);
                    JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: request.id,
                        result: Some(json!(InitializeResult {
                            protocol_version: "2025-03-26".to_string(),
                            capabilities: ServerCapabilities {
                                tools: Some(ToolsCapability {
                                    list_changed: true,
                                }),
                            },
                            server_info: ServerInfo {
                                name: "mcp-server-mysql".to_string(),
                                version: "0.1.0".to_string(),
                            },
                        })),
                        error: None,
                    }
                }
                Err(e) => {
                    eprintln!("Database connection failed: {}", e);
                    create_error_response(
                        request.id,
                        -32001,
                        &format!("Database connection failed: {}", e),
                    )
                }
            }
        }
        "notifications/initialized" | "initialized" => {
            eprintln!("Client initialized");
            // This is a notification, no response needed - should not reach here
            // since we handle it in main loop
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
            let current_pool = match pool.as_ref() {
                Some(p) => p,
                None => {
                    return create_error_response(request.id, -32002, "Server not initialized");
                }
            };
            eprintln!("Handling tool call");
            match request.params {
                Some(params) => match serde_json::from_value::<ToolCallParams>(params) {
                    Ok(tool_params) => {
                        if tool_params.name == "mysql" {
                            match serde_json::from_value::<SchemaArguments>(tool_params.arguments) {
                                Ok(args) => {
                                    get_schema(request.id, args.table_name, current_pool).await
                                }
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
                eprintln!("Successfully retrieved schemas for {} tables", schemas.len());
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
            Err(e) => {
                eprintln!("Database error getting all table schemas: {e}");
                JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32603,
                        message: format!("Failed to get table schemas: {e}"),
                        data: None,
                    }),
                }
            }
        }
    } else {
        // Get single table schema
        match get_table_schema(pool, &table_name).await {
            Ok(schema) => {
                eprintln!("Successfully retrieved schema for table '{}'", table_name);
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
            Err(e) => {
                eprintln!("Database error getting schema for table '{}': {e}", table_name);
                JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32603,
                        message: format!("Failed to get schema for table '{table_name}': {e}"),
                        data: None,
                    }),
                }
            }
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