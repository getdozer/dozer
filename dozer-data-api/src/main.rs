use server::EndPoint;

mod server;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let server = server::Server {
        port: 8080,
        endpoints: vec![
            {
                EndPoint {
                    route: "/posts".to_string(),
                    args: "".to_string(),
                }
            },
            {
                EndPoint {
                    route: "/comments".to_string(),
                    args: "".to_string(),
                }
            },
        ],
    };
    server.run().await
}
