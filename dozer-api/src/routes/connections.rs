use actix_web::{web};
use crate::api::connections;

pub fn route(cfg: &mut web::ServiceConfig) {
    cfg.service(connections::controller::index);
}