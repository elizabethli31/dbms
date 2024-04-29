use crate::conductor::Conductor;
use crate::database_state::DatabaseState;
use crate::handler::handle_command;
use crate::server_state::ServerState;
use crate::StatManager;
use clap::Parser;
use common::catalog::CatalogRef;
use common::commands::Command;
use common::commands::Response;
use common::data_reader::{CsvReader, DataReader};
use common::logical_plan::LogicalPlan;
use common::physical_plan::PhysicalPlan;
use common::prelude::TransactionId;
use common::storage_trait::StorageTrait;
use common::traits::stat_manager_trait::StatManagerTrait;
use common::{CrustyError, QueryResult};
use env_logger::Env;
use index::IndexManager;
use queryexe::opiterator::OpIterator;
use queryexe::Managers;
use queryexe::{StorageManager, STORAGE_DIR};
use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::thread;
use txn_manager::mock_tm::MockTransactionManager as TransactionManager;

const MAX_STAT_BUDGET_MB: usize = 100;

fn create_storage_manager(storage_dir: &Path) -> &'static StorageManager {
    let storage_manager = Box::new(StorageManager::new(storage_dir));
    let storage_manager: &'static StorageManager = Box::leak(storage_manager);
    storage_manager
}

fn create_transaction_manager() -> &'static TransactionManager {
    let transaction_manager = Box::new(TransactionManager::new());
    let transaction_manager: &'static TransactionManager = Box::leak(transaction_manager);
    transaction_manager
}

fn create_index_manager(
    sm: &'static StorageManager,
    tm: &'static TransactionManager,
) -> &'static IndexManager {
    let index_manager = Box::new(IndexManager::new(sm, tm));
    let index_manager: &'static IndexManager = Box::leak(index_manager);
    index_manager
}

fn create_stat_manager(sm: &'static StorageManager) -> &'static StatManager {
    let stat_manager = Box::new(StatManager::new(sm.get_storage_path(), MAX_STAT_BUDGET_MB));
    let stat_manager: &'static StatManager = Box::leak(stat_manager);
    stat_manager
}

fn create_managers(storage_dir: &Path) -> &'static Managers {
    let sm = create_storage_manager(storage_dir);
    let tm = create_transaction_manager();
    let im = create_index_manager(sm, tm);
    let stm = create_stat_manager(sm);
    let managers = Box::new(Managers::new(sm, tm, im, stm));
    let managers: &'static Managers = Box::leak(managers);
    managers
}

fn create_server_state(base_dir: PathBuf) -> &'static ServerState {
    let managers = create_managers(&base_dir);
    let server_state = Box::new(ServerState::new(&base_dir, managers).unwrap());
    let server_state: &'static ServerState = Box::leak(server_state);
    server_state
}

#[derive(Deserialize, Debug, Parser)]
pub struct ServerConfig {
    /// Server IP address
    #[clap(short = 'h', long = "host", default_value = "127.0.0.1")]
    host: String,
    /// Server port number
    #[clap(short = 'p', long = "port", default_value = "3333")]
    port: String,
    /// Path where DB is stored
    #[clap(
        short = 'd',
        long = "db_path",
        default_value = "crusty_data/persist/default/"
    )]
    db_path: String,
    /// Log file
    #[clap(short = 'l', long = "log_file", default_value = "")]
    log_file: String,
    /// Log level
    #[clap(short = 'v', long = "log_level", default_value = "warning")]
    log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: "3333".to_owned(),
            db_path: "crusty_data/persist/default/".to_owned(),
            log_file: "".to_owned(),
            log_level: "warning".to_owned(),
        }
    }
}

impl ServerConfig {
    pub fn new() -> Self {
        ServerConfig::default()
    }

    pub fn temporary() -> Self {
        let base_dir = tempfile::tempdir().unwrap().into_path();
        ServerConfig {
            db_path: base_dir.to_str().unwrap().to_owned(),
            ..ServerConfig::default()
        }
    }
}

pub struct Server {
    cliend_id: AtomicU64,
    shutdown_signal: Arc<AtomicBool>,
    config: ServerConfig,
    server_state: &'static ServerState,
    thread_handles: Vec<thread::JoinHandle<()>>,
}

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        let base_dir = Path::new(&config.db_path).to_path_buf();
        let server_state = create_server_state(base_dir);
        Server {
            cliend_id: AtomicU64::new(1), // 0 is reserved.
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            config,
            server_state,
            thread_handles: vec![],
        }
    }

    pub fn run_server(&mut self) {
        info!("Starting server... {:?}", self.config);

        // Configure log environment
        if self.config.log_file.is_empty() {
            let _ = env_logger::Builder::from_env(
                Env::default().default_filter_or(self.config.log_level.as_str()),
            )
            .try_init();
        } else {
            let log_file = fs::File::create(self.config.log_file.as_str()).unwrap();
            let target = Box::new(log_file);
            let _ = env_logger::Builder::from_env(
                Env::default().default_filter_or(self.config.log_level.as_str()),
            )
            .target(env_logger::Target::Pipe(target))
            .try_init();
        }

        //Start listening to requests by spawning a handler per request.
        let mut bind_addr = self.config.host.clone();
        bind_addr.push(':');
        bind_addr.push_str(&self.config.port);
        let listener = TcpListener::bind(bind_addr).unwrap();

        // Accept connections and process them on independent threads.
        info!(
            "Server listening on with host {} on port {}",
            self.config.host, self.config.port
        );

        // (TODO) Here, we spawn a new thread for each client connection. This is not ideal.
        // Ideally, the loop should just put the request into a queue and the
        // worker threads should pick up the request from the queue and execute it.
        for stream in listener.incoming() {
            if self
                .shutdown_signal
                .load(std::sync::atomic::Ordering::Acquire)
            {
                info!("Received shutdown from one of the clients. Shutting down server...");
                break;
            }
            match stream {
                Ok(stream) => {
                    info!("New connection: {}", stream.peer_addr().unwrap());
                    let handle = thread::spawn({
                        let client_id = self
                            .cliend_id
                            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        let shutdown_signal = self.shutdown_signal.clone();
                        let server_state = self.server_state;
                        move || {
                            handle_client_request(client_id, shutdown_signal, stream, server_state);
                        }
                    });
                    self.thread_handles.push(handle);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No connection available.
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => {
                    // Connection failed.
                    error!("Connection error: {}", e);
                }
            }
        }

        info!("Waiting for all threads to finish...");
        for handler in self.thread_handles.drain(..) {
            handler.join().unwrap();
        }

        info!("Server shutting down...");
    }
}

pub fn handle_client_request(
    client_id: u64,
    shutdown_signal: Arc<AtomicBool>,
    mut stream: TcpStream,
    server_state: &'static ServerState,
) {
    let mut quiet_mode = false;

    while let Some(request_command) = read_command(&mut stream) {
        let (should_break, response) = handle_command(
            shutdown_signal.clone(),
            &mut quiet_mode,
            request_command,
            server_state,
            client_id,
        );

        match send_response(&mut stream, response, quiet_mode) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to send response: {:?}", e);
                break;
            }
        }

        if should_break || shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
            info!("Received server shutdown from one of the clients. Shutting down...");
            break;
        }
    }

    info!("Closing connection with client {}", client_id);
    // finally close the stream
    stream.shutdown(Shutdown::Both).unwrap();
    info!("Connection closed with client {}", client_id);
}

pub fn send_response(
    stream: &mut TcpStream,
    response: Response,
    quiet_mode: bool,
) -> Result<(), CrustyError> {
    let response = if !quiet_mode {
        response
    } else {
        info!("Quiet mode enabled");
        match response {
            Response::SystemErr(_) | Response::QueryExecutionError(_) | Response::QuietErr => {
                Response::QuietErr
            }
            Response::Shutdown => Response::Shutdown,
            _ => Response::QuietOk,
        }
    };
    // xtx maybe update here so that I can have a variable number of queries coming in that can be configured but not sure how to handle cancel and restart
    let response_bytes = serde_cbor::to_vec(&response)
        .map_err(|e| CrustyError::SerializationError(e.to_string()))?;
    // TODO magic number - I  guess there is a potential issue of if the lenght is biggerthan u64 not sure if I need to deal with this
    let response_length = response_bytes.len() as u64;
    let response_length_bytes = response_length.to_be_bytes();
    stream.write_all(&response_length_bytes)?;
    // Then send the actual response in chunks
    let mut start = 0;
    // TODO magic number
    let chunk_size = 1024 * 32; // Define a suitable chunk size
    while start < response_bytes.len() {
        let end = std::cmp::min(start + chunk_size, response_bytes.len());
        stream.write_all(&response_bytes[start..end])?;
        start = end;
    }

    Ok(())
}

pub fn read_command(stream: &mut TcpStream) -> Option<Command> {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(size) if size > 0 => serde_cbor::from_slice(&buffer[0..size]).ok(),
        Ok(_) => {
            info!("Received empty request, closing connection");
            None
        }
        Err(e) => {
            error!("Error reading from stream: {:?}", e);
            None
        }
    }
}

/// Available for testing without a running server
pub struct QueryEngine {
    base_dir_path_name: PathBuf,
    database_state: &'static DatabaseState,
    conductor: Conductor,
}

impl QueryEngine {
    pub fn new(base_dir: &Path) -> QueryEngine {
        if !base_dir.exists() {
            debug!(
                "Base directory {:?} does not exist. Creating the base directory.",
                base_dir
            );
            // Create dirs if they do not exist.
            fs::create_dir_all(base_dir).unwrap();
        }
        let storage_dir = base_dir.join(STORAGE_DIR);
        let managers = create_managers(&storage_dir);
        let database_state =
            Box::new(DatabaseState::create_db(base_dir, "db_name", managers).unwrap());
        let database_state: &'static DatabaseState = Box::leak(database_state);
        let conductor = Conductor::new(managers).unwrap();
        QueryEngine {
            base_dir_path_name: base_dir.to_path_buf(),
            database_state,
            conductor,
        }
    }

    pub fn persist(&mut self) {
        let file_path = self.base_dir_path_name.join("db_name");
        serde_json::to_writer(
            fs::File::create(file_path).expect("error creating file"),
            &self.database_state.catalog,
        )
        .expect("error serializing db");
    }

    pub fn get_base_dir_path(&self) -> &Path {
        &self.base_dir_path_name
    }

    pub fn get_catalog(&self) -> &CatalogRef {
        &self.database_state.catalog
    }

    pub fn get_storage_manager(&self) -> &'static StorageManager {
        self.database_state.managers.sm
    }

    pub fn get_table_id(&mut self, table_name: &str) -> u16 {
        self.database_state.catalog.get_table_id(table_name)
    }

    pub fn run_sql(&mut self, sql: &str) -> Result<QueryResult, CrustyError> {
        self.conductor
            .run_sql_from_string(sql.to_string(), self.database_state)
    }

    pub fn to_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan, CrustyError> {
        self.conductor.to_logical_plan(sql, self.database_state)
    }

    pub fn to_physical_plan(&mut self, sql: &str) -> Result<PhysicalPlan, CrustyError> {
        let logical_plan = self.to_logical_plan(sql)?;
        self.conductor
            .to_physical_plan(logical_plan, self.database_state)
    }

    pub fn run_physical_plan(
        &mut self,
        physical_plan: PhysicalPlan,
    ) -> Result<QueryResult, CrustyError> {
        self.conductor
            .run_physical_plan(physical_plan, self.database_state)
    }
    pub fn run_opiterator(
        &mut self,
        opiterator: Box<dyn OpIterator>,
    ) -> Result<QueryResult, CrustyError> {
        self.conductor.run_opiterator(opiterator)
    }

    pub fn import_csv<R>(
        &mut self,
        reader: R,
        delimiter: u8,
        has_header: bool,
        table_name: &str,
    ) -> Result<usize, CrustyError>
    where
        R: Read,
    {
        let table_id = self.database_state.catalog.get_table_id(table_name);
        let table_schema = self
            .database_state
            .catalog
            .get_table_schema(table_id)
            .unwrap();
        let mut csv_reader = CsvReader::new(reader, &table_schema, delimiter, has_header)?;
        self.conductor.executor.import_records_from_reader(
            &mut csv_reader as &mut dyn DataReader,
            &table_id,
            TransactionId::new(),
        )
    }
}

impl Drop for QueryEngine {
    fn drop(&mut self) {
        // drop the storage manager, transaction manager, and database state
        unsafe {
            let _ = Box::from_raw(
                self.database_state.managers.sm as *const StorageManager as *mut StorageManager,
            );
            let _ = Box::from_raw(
                self.database_state.managers.tm as *const TransactionManager
                    as *mut TransactionManager,
            );
            let _ = Box::from_raw(
                self.database_state.managers.im as *const IndexManager as *mut IndexManager,
            );
            let _ =
                Box::from_raw(self.database_state as *const DatabaseState as *mut DatabaseState);
        }
    }
}

#[cfg(test)]
mod test {
    // Test query engine
    use super::*;

    mod query_engine {
        use super::*;

        #[test]
        fn test_run_sql() {
            let base_dir = tempfile::tempdir().unwrap().into_path();
            let mut query_engine = QueryEngine::new(&base_dir);
            // create table with primary key
            let sql = "CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(10));";
            query_engine.run_sql(sql).unwrap();
            // Insert 10 tuples
            let sql = "INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j');";
            query_engine.run_sql(sql).unwrap();
            // Select with some predicate
            let sql = "SELECT * FROM foo WHERE id > 5;";
            let result = query_engine.run_sql(sql);
            let result = result.unwrap();
            let t = if let QueryResult::Select { result, .. } = result {
                result
            } else {
                panic!("Expected select result");
            };
            assert_eq!(t.len(), 5);
        }

        #[test]
        fn test_load_csv_and_run_sql() {
            let base_dir = tempfile::tempdir().unwrap().into_path();
            let mut query_engine = QueryEngine::new(&base_dir);
            // create table with primary key
            let sql = "CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(10));";
            query_engine.run_sql(sql).unwrap();
            // import tuples from csv data
            let csv_data = "1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n8,h\n9,i\n10,j\n";
            let cursor = std::io::Cursor::new(csv_data);
            query_engine.import_csv(cursor, b',', false, "foo").unwrap();
            // Select with some predicate
            let sql = "SELECT * FROM foo WHERE id > 5;";
            let result = query_engine.run_sql(sql);
            let result = result.unwrap();
            let t = if let QueryResult::Select { result, .. } = result {
                result
            } else {
                panic!("Expected select result");
            };
            assert_eq!(t.len(), 5);
        }
    }
}
