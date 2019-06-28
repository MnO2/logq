use std::str::FromStr;

//https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
pub enum ClassicLoadBalancerLogField {
    Timestamp = 0,
    Elbname = 1,
    ClientAndPort = 2,
    BackendAndPort = 3,
    RequestProcessingTime = 4,
    BackendProcessingTime = 5,
    ResponseProcessingTime = 6,
    ELBStatusCode = 7,
    BackendStatusCode = 8,
    ReceivedBytes = 9,
    SentBytes = 10,
    Request = 11,
    UserAgent = 12,
    SSLCipher = 13,
    SSLProtocol = 14,
}

impl FromStr for ClassicLoadBalancerLogField {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "timestamp" => Ok(ClassicLoadBalancerLogField::Timestamp),
            "elbname" => Ok(ClassicLoadBalancerLogField::Elbname),
            "client_and_port" => Ok(ClassicLoadBalancerLogField::ClientAndPort),
            "backend_and_port" => Ok(ClassicLoadBalancerLogField::BackendAndPort),
            "request_processing_time" => Ok(ClassicLoadBalancerLogField::RequestProcessingTime),
            "backend_processing_time" => Ok(ClassicLoadBalancerLogField::BackendProcessingTime),
            "response_processing_time" => Ok(ClassicLoadBalancerLogField::ResponseProcessingTime),
            "elb_status_code" => Ok(ClassicLoadBalancerLogField::ELBStatusCode),
            "backend_status_code" => Ok(ClassicLoadBalancerLogField::BackendStatusCode),
            "received_bytes" => Ok(ClassicLoadBalancerLogField::ReceivedBytes),
            "sent_bytes" => Ok(ClassicLoadBalancerLogField::SentBytes),
            "request" => Ok(ClassicLoadBalancerLogField::Request),
            "user_agent" => Ok(ClassicLoadBalancerLogField::UserAgent),
            "ssl_cipher" => Ok(ClassicLoadBalancerLogField::SSLCipher),
            "ssl_protocol" => Ok(ClassicLoadBalancerLogField::SSLProtocol),
            _ => Err("unknown column name".to_string()),
        }
    }
}
