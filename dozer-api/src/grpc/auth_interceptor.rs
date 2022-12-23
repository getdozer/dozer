use tonic::{Request, Status};

pub fn auth_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    println!("==== intercept hit {:?}", req);
    Ok(req)
}
