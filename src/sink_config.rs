use std::collections::HashMap;
use crate::sink_config::CompareType::{Greater, Smaller, GreaterEqual, SmallerEqual};
use rlink::utils::http::client::get;
use std::error::Error;
use dashmap::DashMap;
use tokio::time::Instant;

lazy_static! {
    static ref GLOBAL_SINK_CONFIG: DashMap<String, KafkaSinkContext> = DashMap::new();
}

pub const SINK_CONTEXT_KEY: &str = "sink_context";

pub fn init_sink_config(sink_conf_url: String, application_name: String) {
    let param = KafkaSinkConfParam::new(sink_conf_url, application_name);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(load_remote_conf(&param));

    std::thread::spawn(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let start = Instant::now() + std::time::Duration::from_secs(60);
            let mut interval = tokio::time::interval_at(start, std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                load_remote_conf(&param).await;
            }
        });
    });
}

pub fn get_sink_topic(expression_param: HashMap<String, String>) -> String {
    let sink_config: &DashMap<String, KafkaSinkContext> = &*GLOBAL_SINK_CONFIG;
    let sink_context = match sink_config.get(SINK_CONTEXT_KEY) {
        Some(conf) => (*conf).clone(),
        None => KafkaSinkContext::new(),
    };
    let mut expression = sink_context.expression.clone();
    for (key, value) in expression_param {
        expression = expression.replace(key.as_str(), value.as_str());
    }
    if compute_expression(expression.as_str()) {
        sink_context.topic_true.to_string()
    } else {
        sink_context.topic_false.to_string()
    }
}

async fn load_remote_conf(param: &KafkaSinkConfParam) {
    let sink_context =
        get_config_data(param).await.unwrap_or(KafkaSinkContext::new());
    let sink_config: &DashMap<String, KafkaSinkContext> = &*GLOBAL_SINK_CONFIG;
    sink_config.insert(SINK_CONTEXT_KEY.to_string(), sink_context);
}

async fn get_config_data(sink_conf_param: &KafkaSinkConfParam) -> Option<KafkaSinkContext> {
    let url = format!(
        "{}?applicationName={}",
        sink_conf_param.url, sink_conf_param.application_name
    );
    match get(url.as_str()).await {
        Ok(resp) => {
            info!("sink config response={}", resp);
            match parse_conf_response(resp.as_str()) {
                Ok(response) => {
                    let sink_context = response.result;
                    Some(sink_context)
                }
                Err(e) => {
                    error!("parse config response error. {}", e);
                    None
                }
            }
        }
        Err(e) => {
            error!("get sink config error. {}", e);
            None
        }
    }
}

fn parse_conf_response(string: &str) -> serde_json::Result<ConfigResponse> {
    let result: ConfigResponse = serde_json::from_str(string)?;
    Ok(result)
}

enum CompareType {
    Greater(i64, i64),
    Smaller(i64, i64),
    GreaterEqual(i64, i64),
    SmallerEqual(i64, i64),
    None,
}

fn compute_expression(expression: &str) -> bool {
    match get_compare_type(expression) {
        Ok(compare_type) => match compare_type {
            Greater(item1, item2) => item1 > item2,
            Smaller(item1, item2) => item1 < item2,
            GreaterEqual(item1, item2) => item1 >= item2,
            SmallerEqual(item1, item2) => item1 <= item2,
            _ => true,
        },
        Err(e) => {
            error!("get compare type error!{}", e);
            true
        }
    }
}

fn get_compare_type(expression: &str) -> Result<CompareType, Box<dyn Error>> {
    if expression.contains(">=") {
        let (item1, item2) = get_compare_item(expression, ">=");
        Ok(GreaterEqual(item1, item2))
    } else if expression.contains("<=") {
        let (item1, item2) = get_compare_item(expression, "<=");
        Ok(SmallerEqual(item1, item2))
    } else if expression.contains(">") {
        let (item1, item2) = get_compare_item(expression, ">");
        Ok(Greater(item1, item2))
    } else if expression.contains("<") {
        let (item1, item2) = get_compare_item(expression, "<");
        Ok(Smaller(item1, item2))
    } else {
        Ok(CompareType::None)
    }
}

fn get_compare_item(expression: &str, key: &str) -> (i64, i64) {
    let index = expression.find(key).unwrap();
    let item1 = expression.get(0..index).unwrap().trim().parse::<i64>().unwrap_or_default();
    let item2 = expression.get(index + key.len()..).unwrap().trim().parse::<i64>().unwrap_or_default();
    (item1, item2)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    code: i32,
    #[serde(default)]
    message: String,
    result: KafkaSinkContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkContext {
    #[serde(rename = "applicationName")]
    application_name: String,
    #[serde(rename = "expression")]
    expression: String,
    #[serde(rename = "topicTrue")]
    topic_true: String,
    #[serde(rename = "topicFalse")]
    topic_false: String,
}

impl KafkaSinkContext {
    pub fn new() -> Self {
        KafkaSinkContext {
            application_name: String::new(),
            expression: String::new(),
            topic_true: String::new(),
            topic_false: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfParam {
    url: String,
    application_name: String,
}

impl KafkaSinkConfParam {
    pub fn new(url: String, application_name: String) -> Self {
        KafkaSinkConfParam {
            url,
            application_name,
        }
    }
}