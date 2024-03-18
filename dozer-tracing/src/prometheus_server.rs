use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use prometheus::{Encoder, Registry, TextEncoder};
use std::convert::Infallible;
use std::sync::Arc;

async fn serve_req(
    req: Request<Body>,
    registry: Arc<Registry>,
) -> Result<Response<Body>, hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .unwrap()
        }
        _ => Response::builder()
            .status(404)
            .body(Body::from("Metrics are available on /metrics "))
            .unwrap(),
    };
    Ok(response)
}
pub async fn serve(
    registry: &Registry,
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let registry = Arc::new(registry.clone()).clone();
    let make_svc = make_service_fn(move |_conn| {
        let registry = registry.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| serve_req(req, registry.clone()))) }
    });

    let addr = address.parse().unwrap();

    let server = Server::bind(&addr).serve(make_svc);

    println!("Listening on http://{addr}");

    server.await?;

    Ok(())
}
