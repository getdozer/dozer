use actix_web::http::StatusCode;
use actix_web::{ResponseError,HttpResponse, dev, Result, http};
use actix_web::middleware::ErrorHandlerResponse;
use serde::Serialize;
use serde_json::{json, to_string_pretty};
use std::fmt::{Display, Formatter, Result as FmtResult};
use actix_http::body::{EitherBody, BoxBody};

#[derive(Debug, Serialize)]
pub struct Error {
    pub errmsg: String,
    pub errcode: u32,
    pub status: u16,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", to_string_pretty(self).unwrap())
    }
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        let err_json = json!({ "errcode": self.errcode, "errmsg": self.errmsg });
        HttpResponse::build(StatusCode::from_u16(self.status).unwrap()).json(err_json)
    }
}

pub fn new(errcode: u32, errmsg: &str, status: u16) -> Error {
    Error {
        errmsg: errmsg.to_string(),
        errcode,
        status,
    }
}

pub fn res(errcode: u32, errmsg: &str, status: u16) -> Result<HttpResponse, Error> {
    Err(Error {
        errmsg: errmsg.to_string(),
        errcode,
        status,
    })
}

pub fn err500() -> Error {
    Error {
        errmsg: "Internal Server Error".to_string(),
        errcode: 500,
        status: 500,
    }
}

pub fn render_404<B>(mut res: dev::ServiceResponse<B>) -> Result<ErrorHandlerResponse<BoxBody>> {
    res.response_mut().headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let new_res = res.map_body(|_, _| {
        EitherBody::left(BoxBody::new("{\"errcode\": 404, \"errmsg\": \"Not Found\"}"))
    });
    Ok(ErrorHandlerResponse::Response(new_res))
}

pub fn render_405<B>(mut res: dev::ServiceResponse<B>) -> Result<ErrorHandlerResponse<BoxBody>> {
    res.response_mut().headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let new_res = res.map_body(|_, _| {
        EitherBody::left(BoxBody::new("{\"errcode\": 405, \"errmsg\": \"Method Not Allowed\"}"))
    });
    Ok(ErrorHandlerResponse::Response(new_res))
}

pub fn render_500<B>(mut res: dev::ServiceResponse<B>) -> Result<ErrorHandlerResponse<BoxBody>> {
    res.response_mut().headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let new_res = res.map_body(|_, _| {
        EitherBody::left(BoxBody::new("{\"errcode\": 500, \"errmsg\": \"Internal Server Error\"}"))
    });
    Ok(ErrorHandlerResponse::Response(new_res))
}

pub fn render_400<B>(mut res: dev::ServiceResponse<B>) -> Result<ErrorHandlerResponse<BoxBody>> {
    res.response_mut().headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/json"),
    );

    let new_res = res.map_body(|_, _| {
        EitherBody::left(BoxBody::new("{\"errcode\": 400, \"errmsg\": \"Bad Request\"}"))
    });
    Ok(ErrorHandlerResponse::Response(new_res))
}