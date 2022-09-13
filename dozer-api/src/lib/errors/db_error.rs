use std::fmt::{self};
use actix_web::{error};
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
pub struct DbError<T> 
where T: Serialize, {
  pub details: T,
  pub message: String
}

impl<T> fmt::Debug for DbError<T> where
T: Serialize + std::fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DB Error").field("details", &self.details).field("message", &self.message).finish()
    }
}


impl<T> fmt::Display for DbError<T> where
T: Serialize {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      write!(f, "{:?}", serde_json::to_string(self)) // user-facing output
  }
}


impl<T> error::ResponseError for DbError<T> where
T: Serialize + std::fmt::Debug {
    fn status_code(&self) -> actix_http::StatusCode {
        actix_http::StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> actix_web::HttpResponse<actix_http::body::BoxBody> {
      actix_web::HttpResponse::InternalServerError().json(self)
    }
}
