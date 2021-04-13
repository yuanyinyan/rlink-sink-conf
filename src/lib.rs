//! # rlink-sink-conf
//!
//! A library to upgrade [rlink](https://docs.rs/rlink) task smoothly.
//!
//! # Example
//!
//! ```
//! use rlink_sink_conf::sink_config::{init_sink_config, get_sink_topic};
//! use std::collections::HashMap;
//!
//! let sin_conf_url = "http://web.rlink.17usoft.com/upgrade/config/name";
//! let application_name = "tlb_base_qa";
//! let timestamp = 123 as u64;
//!
//! init_sink_config(sin_conf_url.to_string(), application_name.to_string());
//!
//! let mut expression_param = HashMap::new();
//! expression_param.insert("timestamp".to_string(), timestamp.to_string());
//! let sink_topic = get_sink_topic(expression_param);
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

pub mod sink_config;

#[cfg(test)]
mod tests {
    use crate::sink_config::{init_sink_config, get_sink_topic};
    use std::collections::HashMap;

    #[test]
    fn it_works() {
        let sin_conf_url = "http://web.rlink.17usoft.com/upgrade/config/name";
        let application_name = "tlb_base_qa";
        let timestamp = 123 as u64;

        init_sink_config(sin_conf_url.to_string(), application_name.to_string());

        let mut expression_param = HashMap::new();
        expression_param.insert("timestamp".to_string(), timestamp.to_string());
        let _sink_topic = get_sink_topic(expression_param);
        println!()
    }
}
