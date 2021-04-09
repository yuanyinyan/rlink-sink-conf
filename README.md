# rlink-sink-conf

[![Crates.io](https://img.shields.io/crates/v/rlink?color=blue)](https://crates.io/crates/rlink-sink-conf)
[![Released API docs](https://docs.rs/rlink/badge.svg)](https://docs.rs/rlink-sink-conf)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE-MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE-APACHE)

Sink config for [rlink](https://github.com/rlink-rs/rlink-rs). 
Upgrade tasks smoothly.

## Example

```yaml
[dependencies]
rlink-sink-conf = "0.1"
```

``` rust
        let sin_conf_url = "http://web.rlink.17usoft.com/upgrade/config/name";
        let application_name = "tlb_base_qa";
        let timestamp = 123 as u64;

        init_sink_config(sin_conf_url.to_string(), application_name.to_string());

        let mut expression_param = HashMap::new();
        expression_param.insert("timestamp".to_string(), timestamp.to_string());
        let sink_topic = get_sink_topic(expression_param);
```
