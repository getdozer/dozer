use crate::controllers;
use actix_web::web;

pub fn routes(cfg: &mut web::ServiceConfig) {
    cfg.service(controllers::connections::index);
    cfg.service(controllers::connections::create_connection);
    cfg.service(controllers::connections::test_connection);

    cfg.service(controllers::sources::create_source);
}
