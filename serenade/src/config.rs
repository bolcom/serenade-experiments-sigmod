use std::convert::TryInto;
use std::ffi::OsStr;
use std::fs::File;

use justconfig::item::ValueExtractor;
use justconfig::processors::Trim;
use justconfig::sources::env::Env;
use justconfig::sources::text::ConfigText;
use justconfig::ConfPath;
use justconfig::Config;

use crate::config_processors::Unquote;

// Set some default values
const DEFAULT_MOST_RECENT_SESSIONS_M: usize = 500;
const DEFAULT_NEIGHBORHOOD_SIZE_K: usize = 500;
const DEFAULT_NUM_ITEMS_TO_RECOMMEND: usize = 21;
const DEFAULT_MAX_ITEMS_IN_SESSION: usize = 2;

pub struct AppConfig {
    pub server: ServerConfig,
    pub log: LogConfig,
    pub data: DataConfig,
    pub model: ModelConfig,
    pub logic: LogicConfig,
}

pub struct ServerConfig {
    pub host: String,
    pub port: usize,
    pub num_workers: usize,
}

pub struct LogConfig {
    pub level: String,
}

pub struct DataConfig {
    pub training_data_path: String,
}

pub struct ModelConfig {
    pub m_most_recent_sessions: usize,
    pub neighborhood_size_k: usize,
    pub num_items_to_recommend: usize,
    pub max_items_in_session: usize,
}

pub struct LogicConfig {
    pub enable_business_logic: bool,
}

impl AppConfig {
    pub fn new(config_path: String) -> AppConfig {
        // Initialize config object
        let mut conf = Config::default();

        // Check if there is a config file
        if let Ok(config_file) = File::open(&config_path) {
            let config_text = ConfigText::new(config_file, &config_path)
                .expect("Loading configuration file failed.");
            conf.add_source(config_text);
        }

        // Define config params from environment variables
        let config_env = Env::new(&[
            (
                ConfPath::from(&["data", "training_data_path"]),
                OsStr::new("TRAINING_DATA"),
            ),
            (
                ConfPath::from(&["server", "num_workers"]),
                OsStr::new("NUM_WORKERS"),
            ),
        ]);
        conf.add_source(config_env);

        // Parse into custom config struct
        AppConfig::parse(conf)
    }

    fn parse(conf: justconfig::Config) -> AppConfig {
        AppConfig {
            server: ServerConfig::parse(&conf, ConfPath::from(&["server"])),
            log: LogConfig::parse(&conf, ConfPath::from(&["log"])),
            data: DataConfig::parse(&conf, ConfPath::from(&["data"])),
            model: ModelConfig::parse(&conf, ConfPath::from(&["model"])),
            logic: LogicConfig::parse(&conf, ConfPath::from(&["logic"]))
        }
    }
}

impl ServerConfig {
    fn parse(conf: &Config, path: ConfPath) -> ServerConfig {
        ServerConfig {
            host: conf
                .get(path.push("host"))
                .unquote()
                .value()
                .unwrap_or_else(|_| String::from("0.0.0.0")),
            port: conf.get(path.push("port")).trim().value().unwrap_or(8080),
            num_workers: conf
                .get(path.push("num_workers"))
                .trim()
                .value()
                // Detect number of CPUs
                .unwrap_or_else(|_| sys_info::cpu_num().unwrap_or_default().try_into().unwrap()),
        }
    }
}

impl LogConfig {
    fn parse(conf: &Config, path: ConfPath) -> LogConfig {
        LogConfig {
            level: conf
                .get(path.push("level"))
                .unquote()
                .value()
                .unwrap_or_default(),
        }
    }
}

impl DataConfig {
    fn parse(conf: &Config, path: ConfPath) -> DataConfig {
        DataConfig {
            training_data_path: conf
                .get(path.push("training_data_path"))
                .unquote()
                .value()
                .unwrap(),
        }
    }
}

impl ModelConfig {
    fn parse(conf: &Config, path: ConfPath) -> ModelConfig {
        ModelConfig {
            m_most_recent_sessions: conf
                .get(path.push("m_most_recent_sessions"))
                .trim()
                .value()
                .unwrap_or(DEFAULT_MOST_RECENT_SESSIONS_M),
            neighborhood_size_k: conf
                .get(path.push("neighborhood_size_k"))
                .trim()
                .value()
                .unwrap_or(DEFAULT_NEIGHBORHOOD_SIZE_K),
            num_items_to_recommend: conf
                .get(path.push("num_items_to_recommend"))
                .trim()
                .value()
                .unwrap_or(DEFAULT_NUM_ITEMS_TO_RECOMMEND),
            max_items_in_session: conf
                .get(path.push("max_items_in_session"))
                .trim()
                .value()
                .unwrap_or(DEFAULT_MAX_ITEMS_IN_SESSION),
        }
    }
}

impl LogicConfig {
    fn parse(conf: &Config, path: ConfPath) -> LogicConfig {
        LogicConfig {
            enable_business_logic: conf
                .get(path.push("enable_business_logic"))
                .unquote()
                .value()
                .unwrap(),
        }
    }
}
