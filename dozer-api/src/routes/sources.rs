use actix_web::{web};
use crate::api::sources;

pub fn route(cfg: &mut web::ServiceConfig) {
    cfg.service(sources::controller::create_source);
}