use std::net::SocketAddr;

use super::expectation::Expectation;

pub struct Client {
    rest_endpoint: SocketAddr,
    rest_client: reqwest::Client,
}

impl Client {
    pub fn new(rest_endpoint: SocketAddr) -> Self {
        Self {
            rest_endpoint,
            rest_client: reqwest::Client::new(),
        }
    }

    pub async fn check_expectation(&self, expectation: &Expectation) {
        match expectation {
            Expectation::HealthyService => self.check_healthy_service().await,
        }
    }

    async fn check_healthy_service(&self) {
        let response = self
            .rest_client
            .get(&format!("http://{}/health", self.rest_endpoint))
            .send()
            .await
            .expect("Cannot get response from rest health endpoint");
        let status = response.status();
        if !status.is_success() {
            panic!("REST health endpoint responds {}", status);
        }
    }
}
