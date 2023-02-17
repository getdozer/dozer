use web3::transports::WebSocket;

pub async fn get_wss_client(url: &str) -> Result<web3::Web3<WebSocket>, web3::Error> {
    Ok(web3::Web3::new(
        web3::transports::WebSocket::new(url).await?,
    ))
}
