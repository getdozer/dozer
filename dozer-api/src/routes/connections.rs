use actix_web::{web};
use crate::api::connections;

pub fn route(cfg: &mut web::ServiceConfig) {
    cfg.service(connections::controllers::get::index);
    cfg.service(connections::controllers::post::create_connection);
    cfg.service(connections::controllers::post::test_connection);

}