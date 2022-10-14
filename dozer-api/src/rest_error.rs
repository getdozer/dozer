use actix_web::{body, error, http::StatusCode, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{self};

#[derive(Serialize, Deserialize)]
pub enum RestError {
    NotFound {
        message: Option<String>,
    },
    Validation {
        message: Option<String>,
        details: Option<Value>,
    },
    Unknown {
        message: Option<String>,
        details: Option<Value>,
    },
}
impl RestError {
    pub fn name(&self) -> String {
        match self {
            Self::NotFound { message: _ } => "NotFound".to_string(),
            Self::Unknown {
                message: _,
                details: _,
            } => "Unknown".to_string(),
            Self::Validation {
                message: _,
                details: _,
            } => "Validation".to_string(),
        }
    }
}
impl fmt::Debug for RestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", serde_json::to_string(self))
    }
}

impl fmt::Display for RestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", serde_json::to_string(self)) // user-facing output
    }
}

impl error::ResponseError for RestError {
    fn status_code(&self) -> StatusCode {
        match self {
            RestError::NotFound { message: _ } => StatusCode::NOT_FOUND,
            RestError::Validation {
                message: _,
                details: _,
            } => StatusCode::BAD_REQUEST,
            RestError::Unknown {
                message: _,
                details: _,
            } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> actix_web::HttpResponse<body::BoxBody> {
        let status_code = self.status_code();
        HttpResponse::build(status_code).json(self)
    }
}
