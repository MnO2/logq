//https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
pub struct ClassicLoadBalancerLogRecord {
    pub timestamp: String,
    pub elbname: String,
    pub client_and_port: String,
    pub backend_and_port: String,
    pub request_processing_time: String,
    pub backend_processing_time: String,
    pub response_processing_time: String,
    pub elb_status_code: String,
    pub backend_status_code: String,
    pub received_bytes: String,
    pub sent_bytes: String,
    pub request: String,
    pub user_agent: String,
    pub ssl_cipher: String,
    pub ssl_protocol: String
}

impl ClassicLoadBalancerLogRecord {
    pub fn empty() -> Self {
        ClassicLoadBalancerLogRecord {
            timestamp: "".to_string(),
            elbname: "".to_string(),
            client_and_port: "".to_string(),
            backend_and_port: "".to_string(),
            request_processing_time: "".to_string(),
            backend_processing_time: "".to_string(),
            response_processing_time: "".to_string(),
            elb_status_code: "".to_string(),
            backend_status_code: "".to_string(),
            received_bytes: "".to_string(),
            sent_bytes: "".to_string(),
            request: "".to_string(),
            user_agent: "".to_string(),
            ssl_cipher: "".to_string(),
            ssl_protocol: "".to_string()
        } 
    }
}
