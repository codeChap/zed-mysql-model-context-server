use clap::Parser;
use log::{debug, info, warn, error};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Column, MySql, Pool, Row};


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
    
    /// Allow dangerous SQL keywords in queries (INSERT, UPDATE, DELETE, etc.)
    #[arg(long, default_value = "false")]
    allow_dangerous_queries: bool,
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

#[derive(Debug, Deserialize)]
struct QueryArguments {
    query: String,
}

#[derive(Debug, Deserialize)]
struct InsertArguments {
    table_name: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct UpdateArguments {
    table_name: String,
    data: serde_json::Value,
    conditions: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct DeleteArguments {
    table_name: String,
    conditions: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();
    
    let args = Args::parse();
    let allow_dangerous_queries = args.allow_dangerous_queries;
    
    // Defer database connection until initialize request is received
    let mut pool: Option<Pool<MySql>> = None;

    // Set up stdio
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Send logs to stderr to avoid interfering with JSON-RPC communication
    info!("MCP MySQL Server started and ready to accept connections");
    info!("Server args: host={}, port={}, username={}, database={}", 
              args.host, args.port, args.username, args.database);
    info!("Server PID: {}", std::process::id());
    debug!("Environment variables:");
    for (key, value) in std::env::vars() {
        if key.contains("MYSQL") || key.contains("DATABASE") || key.contains("MCP") {
            debug!("  {key}: {value}");
        }
    }
    debug!("Current working directory: {:?}", std::env::current_dir());

    // Process incoming messages with improved error handling
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }

                debug!("Received message (len={}): {}", line.len(), line);
                debug!("Message bytes: {:?}", line.as_bytes());
                match serde_json::from_str::<JsonRpcRequest>(&line) {
                    Ok(request) => {
                        debug!("Parsed request: method={}, id={:?}", request.method, request.id);
                        // Handle notifications (no response needed)
                        if request.method == "notifications/initialized" || request.method == "initialized" {
                            debug!("Received initialization notification: {}", request.method);
                            continue;
                        }
                        
                        let response = handle_request(request, &mut pool, &args, allow_dangerous_queries).await;
                        match serde_json::to_string(&response) {
                            Ok(response_str) => {
                                if let Err(e) = write_response(&mut stdout, &response_str).await {
                                    error!("Failed to write response: {e}");
                                    // Continue processing other requests
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize response: {e}");
                                // Send a generic error response
                                let error_response = create_error_response(None, -32603, "Internal error");
                                if let Ok(error_str) = serde_json::to_string(&error_response) {
                                    let _ = write_response(&mut stdout, &error_str).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse request: {e}");
                        let error_response = create_error_response(None, -32700, "Parse error");
                        if let Ok(response_str) = serde_json::to_string(&error_response) {
                            let _ = write_response(&mut stdout, &response_str).await;
                        }
                    }
                }
            }
            Ok(None) => {
                // stdin closed, this is normal when client disconnects
                info!("stdin closed - client disconnected, shutting down server");
                break;
            }
            Err(e) => {
                warn!("Error reading from stdin: {e} (error kind: {:?})", e.kind());
                // Add more context about the error
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    info!("Unexpected EOF - client may have terminated");
                    break;
                }
                // Continue trying to read in case of transient errors
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    info!("MCP MySQL Server shutdown complete");
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
                info!("Successfully connected to MySQL database");
                return Ok(pool);
            }
            Err(e) => {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    error!("Failed to connect to database after {MAX_RETRIES} retries: {e}");
                    return Err(e.into());
                }
                warn!("Database connection failed (attempt {retry_count}/{MAX_RETRIES}): {e}");
                info!("Retrying in {RETRY_DELAY_MS}ms...");
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
    allow_dangerous_queries: bool,
) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => {
            debug!("Handling initialize request with params: {:?}", request.params);

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
                    info!("Using database_url from initializationOptions: {url}");
                    url
                }
                None => {
                    let url = format!(
                        "mysql://{}:{}@{}:{}/{}",
                        args.username, args.password, args.host, args.port, args.database
                    );
                    info!("Using database_url from command-line arguments: mysql://{}:***@{}:{}/{}", 
                             args.username, args.host, args.port, args.database);
                    url
                }
            };

            info!("Attempting database connection...");
            match connect_with_retry(&database_url).await {
                Ok(new_pool) => {
                    info!("Database connection successful!");
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
                    error!("Database connection failed: {e}");
                    create_error_response(
                        request.id,
                        -32001,
                        &format!("Database connection failed: {e}"),
                    )
                }
            }
        }
        "notifications/initialized" | "initialized" => {
            info!("Client initialized");
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
    debug!("Listing available tools");
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id: request.id,
        result: Some(json!(ToolsList {
            tools: vec![
                Tool {
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
                },
                Tool {
                    name: "query".to_string(),
                    description: if allow_dangerous_queries {
                        "Execute any SQL query on the database (unrestricted)".to_string()
                    } else {
                        "Execute a SELECT query on the database (read-only)".to_string()
                    },
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": if allow_dangerous_queries {
                                    "SQL query to execute"
                                } else {
                                    "SELECT query to execute"
                                }
                            }
                        },
                        "required": ["query"]
                    }),
                },
                        Tool {
                            name: "insert".to_string(),
                            description: "Insert data into a specified table".to_string(),
                            input_schema: json!({
                                "type": "object",
                                "properties": {
                                    "table_name": {
                                        "type": "string",
                                        "description": "Name of the table to insert data into"
                                    },
                                    "data": {
                                        "type": "object",
                                        "description": "Data to insert as key-value pairs"
                                    }
                                },
                                "required": ["table_name", "data"]
                            }),
                        },
                        Tool {
                            name: "update".to_string(),
                            description: "Update data in a specified table based on conditions".to_string(),
                            input_schema: json!({
                                "type": "object",
                                "properties": {
                                    "table_name": {
                                        "type": "string",
                                        "description": "Name of the table to update data in"
                                    },
                                    "data": {
                                        "type": "object",
                                        "description": "Data to update as key-value pairs"
                                    },
                                    "conditions": {
                                        "type": "object",
                                        "description": "Conditions for update as key-value pairs"
                                    }
                                },
                                "required": ["table_name", "data", "conditions"]
                            }),
                        },
                        Tool {
                            name: "delete".to_string(),
                            description: "Delete data from a specified table based on conditions".to_string(),
                            input_schema: json!({
                                "type": "object",
                                "properties": {
                                    "table_name": {
                                        "type": "string",
                                        "description": "Name of the table to delete data from"
                                    },
                                    "conditions": {
                                        "type": "object",
                                        "description": "Conditions for deletion as key-value pairs"
                                    }
                                },
                                "required": ["table_name", "conditions"]
                            }),
                        },
                    ],
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
            debug!("Handling tool call");
            match request.params {
                Some(params) => match serde_json::from_value::<ToolCallParams>(params) {
                    Ok(tool_params) => {
                        match tool_params.name.as_str() {
                            "mysql" => {
                                match serde_json::from_value::<SchemaArguments>(tool_params.arguments) {
                                    Ok(schema_args) => {
                                        get_schema(request.id, schema_args.table_name, current_pool).await
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
                            }
                            "query" => {
                                match serde_json::from_value::<QueryArguments>(tool_params.arguments) {
                                    Ok(query_args) => {
                                        execute_query(request.id.clone().unwrap_or(json!(null)), query_args.query, current_pool, allow_dangerous_queries).await
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
                            }
                            "insert" => {
                                match serde_json::from_value::<InsertArguments>(tool_params.arguments) {
                                    Ok(insert_args) => {
                                        insert_data(request.id.clone().unwrap_or(json!(null)), insert_args.table_name, insert_args.data, current_pool).await
                                    }
                                    Err(e) => JsonRpcResponse {
                                        jsonrpc: "2.0".to_string(),
                                        id: request.id,
                                        result: None,
                                        error: Some(JsonRpcError {
                                            code: -32602,
                                            message: format!("Invalid insert arguments: {e}"),
                                            data: None,
                                        }),
                                    },
                                }
                            }
                            "update" => {
                                match serde_json::from_value::<UpdateArguments>(tool_params.arguments) {
                                    Ok(update_args) => {
                                        update_data(request.id.clone().unwrap_or(json!(null)), update_args.table_name, update_args.data, update_args.conditions, current_pool).await
                                    }
                                    Err(e) => JsonRpcResponse {
                                        jsonrpc: "2.0".to_string(),
                                        id: request.id,
                                        result: None,
                                        error: Some(JsonRpcError {
                                            code: -32602,
                                            message: format!("Invalid update arguments: {e}"),
                                            data: None,
                                        }),
                                    },
                                }
                            }
                            "delete" => {
                                match serde_json::from_value::<DeleteArguments>(tool_params.arguments) {
                                    Ok(delete_args) => {
                                        delete_data(request.id.clone().unwrap_or(json!(null)), delete_args.table_name, delete_args.conditions, current_pool).await
                                    }
                                    Err(e) => JsonRpcResponse {
                                        jsonrpc: "2.0".to_string(),
                                        id: request.id,
                                        result: None,
                                        error: Some(JsonRpcError {
                                            code: -32602,
                                            message: format!("Invalid delete arguments: {e}"),
                                            data: None,
                                        }),
                                    },
                                }
                            }
                            _ => JsonRpcResponse {
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
            warn!("Unknown method: {}", request.method);
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
    debug!("Getting schema for: {table_name}");
    
    if table_name == "all-tables" {
        // Get all table schemas
        match get_all_table_schemas(pool).await {
            Ok(schemas) => {
                info!("Successfully retrieved schemas for {} tables", schemas.len());
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
                error!("Database error getting all table schemas: {e}");
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
                info!("Successfully retrieved schema for table '{table_name}'");
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
                error!("Database error getting schema for table '{table_name}': {e}");
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

async fn insert_data(
    id: serde_json::Value,
    table_name: String,
    data: serde_json::Value,
    pool: &Pool<MySql>,
) -> JsonRpcResponse {
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get connection: {}", e);
            return create_error_response(Some(id), -32003, &format!("Database connection error: {}", e));
        }
    };

    // Validate table name to prevent SQL injection
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return create_error_response(Some(id), -32602, "Invalid table name");
    }

    // Build the INSERT query with placeholders
    let data_map = match data.as_object() {
        Some(map) => map,
        None => {
            return create_error_response(Some(id), -32602, "Data must be an object");
        }
    };

    if data_map.is_empty() {
        return create_error_response(Some(id), -32602, "Data object is empty");
    }

    let columns: Vec<String> = data_map.keys().cloned().collect();
    let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();
    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        columns.join(", "),
        placeholders.join(", ")
    );

    let mut query_builder = sqlx::query(&query);
    for column in &columns {
        if let Some(value) = data_map.get(column) {
            query_builder = query_builder.bind(value);
        }
    }

    debug!("Executing insert query: {}", query);
    match query_builder.execute(&mut *conn).await {
        Ok(_) => {
            let last_id: u64 = match sqlx::query_scalar("SELECT LAST_INSERT_ID()")
                .fetch_one(&mut *conn)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    error!("Failed to get last insert ID: {}", e);
                    0
                }
            };
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(id),
                result: Some(json!({
                    "success": true,
                    "last_insert_id": last_id
                })),
                error: None,
            }
        }
        Err(e) => {
            error!("Insert failed: {}", e);
            create_error_response(Some(id), -32004, &format!("Insert failed: {}", e))
        }
    }
}

async fn update_data(
    id: serde_json::Value,
    table_name: String,
    data: serde_json::Value,
    conditions: serde_json::Value,
    pool: &Pool<MySql>,
) -> JsonRpcResponse {
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get connection: {}", e);
            return create_error_response(Some(id), -32003, &format!("Database connection error: {}", e));
        }
    };

    // Validate table name to prevent SQL injection
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return create_error_response(Some(id), -32602, "Invalid table name");
    }

    // Build the UPDATE query with placeholders
    let data_map = match data.as_object() {
        Some(map) => map,
        None => {
            return create_error_response(Some(id), -32602, "Data must be an object");
        }
    };

    let conditions_map = match conditions.as_object() {
        Some(map) => map,
        None => {
            return create_error_response(Some(id), -32602, "Conditions must be an object");
        }
    };

    if data_map.is_empty() {
        return create_error_response(Some(id), -32602, "Data object is empty");
    }

    if conditions_map.is_empty() {
        return create_error_response(Some(id), -32602, "Conditions object is empty");
    }

    let set_clause: Vec<String> = data_map.keys().map(|k| format!("{} = ?", k)).collect();
    let where_clause: Vec<String> = conditions_map.keys().map(|k| format!("{} = ?", k)).collect();
    let query = format!(
        "UPDATE {} SET {} WHERE {}",
        table_name,
        set_clause.join(", "),
        where_clause.join(" AND ")
    );

    let mut query_builder = sqlx::query(&query);
    for key in data_map.keys() {
        if let Some(value) = data_map.get(key) {
            query_builder = query_builder.bind(value);
        }
    }
    for key in conditions_map.keys() {
        if let Some(value) = conditions_map.get(key) {
            query_builder = query_builder.bind(value);
        }
    }

    debug!("Executing update query: {}", query);
    match query_builder.execute(&mut *conn).await {
        Ok(result) => {
            let affected_rows = result.rows_affected();
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(id),
                result: Some(json!({
                    "success": true,
                    "affected_rows": affected_rows
                })),
                error: None,
            }
        }
        Err(e) => {
            error!("Update failed: {}", e);
            create_error_response(Some(id), -32004, &format!("Update failed: {}", e))
        }
    }
}

async fn delete_data(
    id: serde_json::Value,
    table_name: String,
    conditions: serde_json::Value,
    pool: &Pool<MySql>,
) -> JsonRpcResponse {
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get connection: {}", e);
            return create_error_response(Some(id), -32003, &format!("Database connection error: {}", e));
        }
    };

    // Validate table name to prevent SQL injection
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return create_error_response(Some(id), -32602, "Invalid table name");
    }

    // Build the DELETE query with placeholders
    let conditions_map = match conditions.as_object() {
        Some(map) => map,
        None => {
            return create_error_response(Some(id), -32602, "Conditions must be an object");
        }
    };

    if conditions_map.is_empty() {
        return create_error_response(Some(id), -32602, "Conditions object is empty");
    }

    let where_clause: Vec<String> = conditions_map.keys().map(|k| format!("{} = ?", k)).collect();
    let query = format!(
        "DELETE FROM {} WHERE {}",
        table_name,
        where_clause.join(" AND ")
    );

    let mut query_builder = sqlx::query(&query);
    for key in conditions_map.keys() {
        if let Some(value) = conditions_map.get(key) {
            query_builder = query_builder.bind(value);
        }
    }

    debug!("Executing delete query: {}", query);
    match query_builder.execute(&mut *conn).await {
        Ok(result) => {
            let affected_rows = result.rows_affected();
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(id),
                result: Some(json!({
                    "success": true,
                    "affected_rows": affected_rows
                })),
                error: None,
            }
        }
        Err(e) => {
            error!("Delete failed: {}", e);
            create_error_response(Some(id), -32004, &format!("Delete failed: {}", e))
        }
    }
}

async fn execute_query(
    id: serde_json::Value,
    query: String,
    pool: &Pool<MySql>,
    allow_dangerous_queries: bool,
) -> JsonRpcResponse {
    // Validate queries unless dangerous queries are allowed
    if !allow_dangerous_queries {
        // Basic validation - only allow SELECT queries
        let trimmed_query = query.trim();
        if !trimmed_query.to_uppercase().starts_with("SELECT") {
            return create_error_response(Some(id), -32602, "Only SELECT queries are allowed. Use --allow-dangerous-queries flag to execute other query types.");
        }
        
        // Check for potentially dangerous keywords
        let dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE"];
        let query_upper = trimmed_query.to_uppercase();
        for keyword in &dangerous_keywords {
            if query_upper.contains(keyword) {
                return create_error_response(Some(id), -32602, &format!("Query contains forbidden keyword: {}. Use --allow-dangerous-queries flag to allow such queries.", keyword));
            }
        }
    }

    debug!("Executing query: {}", query);
    
    match sqlx::query(&query).fetch_all(pool).await {
        Ok(rows) => {
            let mut results = Vec::new();
            
            for row in rows {
                let mut row_data = serde_json::Map::new();
                
                // Get column names and values
                for (i, column) in row.columns().iter().enumerate() {
                    let column_name = column.name();
                    
                    // Try to extract value as different types
                    if let Ok(value) = row.try_get::<Option<String>, _>(i) {
                        row_data.insert(column_name.to_string(), json!(value));
                    } else if let Ok(value) = row.try_get::<Option<i64>, _>(i) {
                        row_data.insert(column_name.to_string(), json!(value));
                    } else if let Ok(value) = row.try_get::<Option<f64>, _>(i) {
                        row_data.insert(column_name.to_string(), json!(value));
                    } else if let Ok(value) = row.try_get::<Option<bool>, _>(i) {
                        row_data.insert(column_name.to_string(), json!(value));
                    } else {
                        // Default to null if we can't determine the type
                        row_data.insert(column_name.to_string(), json!(null));
                    }
                }
                
                results.push(json!(row_data));
            }
            
            // Format results as a text table for better AI visibility
            let mut content_text = format!("Query executed successfully. Retrieved {} rows.\n\n", results.len());
            
            if !results.is_empty() {
                // Convert results to a formatted string
                content_text.push_str("Results:\n");
                content_text.push_str(&serde_json::to_string_pretty(&results).unwrap_or_else(|_| "Error formatting results".to_string()));
            }
            
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: Some(id),
                result: Some(json!({
                    "content": [{
                        "type": "text",
                        "text": content_text
                    }]
                })),
                error: None,
            }
        }
        Err(e) => {
            error!("Query execution failed: {}", e);
            create_error_response(Some(id), -32004, &format!("Query execution failed: {}", e))
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