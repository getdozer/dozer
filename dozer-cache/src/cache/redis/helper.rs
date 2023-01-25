pub fn init() -> redis::RedisResult<(redis::Client, redis::Connection)> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let con = client.get_connection()?;

    Ok((client, con))
}
