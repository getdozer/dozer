use actix_web::{web, App, HttpResponse, HttpServer, Responder};

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[derive(Clone, Debug)]
pub struct EndPoint {
    pub route: String,
    pub args: String,
}
pub struct Server {
    pub port: u16,
    pub endpoints: Vec<EndPoint>,
}

// #[async_trait]
impl Server {
    pub async fn run(&self) -> std::io::Result<()> {
        let endpoints = self.endpoints.clone();
        HttpServer::new(move || {
            endpoints.iter().fold(App::new(), |app, endpoint| {
                app.route(&endpoint.route.clone(), web::get().to(manual_hello))
            })
        })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
    }
}
