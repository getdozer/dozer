use web3::transports::{Batch, Http, WebSocket};

pub async fn get_wss_client(url: &str) -> Result<web3::Web3<WebSocket>, web3::Error> {
    Ok(web3::Web3::new(
        web3::transports::WebSocket::new(url).await?,
    ))
}

pub async fn get_batch_wss_client(
    url: &str,
) -> Result<(web3::Web3<Batch<WebSocket>>, WebSocket), web3::Error> {
    let transport = web3::transports::WebSocket::new(url).await?;
    Ok((
        web3::Web3::new(web3::transports::Batch::new(transport.clone())),
        transport,
    ))
}

pub async fn get_batch_http_client(
    url: &str,
) -> Result<(web3::Web3<Batch<Http>>, Http), web3::Error> {
    let transport = web3::transports::Http::new(url)?;
    Ok((
        web3::Web3::new(web3::transports::Batch::new(transport.clone())),
        transport,
    ))
}
