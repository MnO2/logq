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
