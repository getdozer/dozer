use dozer_cache::cache::expression::FilterExpression;
use dozer_types::serde;
use jsonwebtoken::{
    decode, encode, errors::ErrorKind, Algorithm, DecodingKey, EncodingKey, Header, TokenData,
    Validation,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::ApiAuthError;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
struct Claims {
    aud: String,
    sub: String,
    exp: usize,
    access: Access,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
// Access gets resolved in cache query, get and list functions
enum Access {
    /// Access to all indexes
    All,
    /// Specific permissions to each of the indexes
    Custom(Vec<AccessFilter>),
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
/// This filter gets dynamically added to the query.
struct AccessFilter {
    /// Name of the index
    indexes: Vec<String>,

    /// FilterExpression to evaluate access
    filter: Option<FilterExpression>,

    /// Fields to be restricted
    fields: Vec<String>,
}
pub struct ApiAuth<'a> {
    secret: &'a [u8],
    aud: String,
    sub: String,
}

impl<'a> ApiAuth<'a> {
    fn new(secret: &'a [u8], aud: Option<String>, sub: Option<String>) -> Self {
        Self {
            secret,
            aud: aud.unwrap_or("cache_user".to_owned()),
            sub: sub.unwrap_or("api@dozer.com".to_owned()),
        }
    }

    /// Creates exp based on duration provided with a default of 300 seconds
    pub fn get_expiry(dur: Option<Duration>) -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        println!("{:?}", since_the_epoch);

        let dur = match dur {
            Some(dur) => dur + since_the_epoch,
            // 300 seconds
            None => Duration::new(300, 0) + since_the_epoch,
        };

        dur.as_secs() * 1000 + dur.subsec_nanos() as u64 / 1_000_000
    }

    pub fn generate_token(
        &self,
        access: Access,
        dur: Option<Duration>,
    ) -> Result<String, ApiAuthError> {
        let exp = Self::get_expiry(dur);

        let my_claims = Claims {
            exp: exp as usize,
            access,
            aud: self.aud.to_owned(),
            sub: self.sub.to_owned(),
        };

        encode(
            &Header::default(),
            &my_claims,
            &EncodingKey::from_secret(self.secret),
        )
        .map_err(|e| ApiAuthError::InternalError(Box::new(e)))
    }

    pub fn validate_token(&self, token: String) -> Result<TokenData<Claims>, ApiAuthError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.sub = Some(self.sub.to_owned());
        validation.set_audience(&[self.aud.to_owned()]);

        match decode::<Claims>(&token, &DecodingKey::from_secret(self.secret), &validation) {
            Ok(c) => Ok(c),
            Err(err) => Err(match *err.kind() {
                ErrorKind::InvalidToken => ApiAuthError::InvalidToken,
                ErrorKind::InvalidIssuer => ApiAuthError::InvalidIssuer,
                _ => ApiAuthError::InternalError(Box::new(err)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api_auth::Access;

    use super::ApiAuth;

    #[test]
    fn generate_and_verify_claim() {
        let auth_utils = ApiAuth::new(b"secret", None, None);

        let token = auth_utils.generate_token(Access::All, None).unwrap();

        println!("{:?}", token);

        let token_data = auth_utils.validate_token(token).unwrap();
        assert_eq!(token_data.claims.access, Access::All, "must be equal");
    }
}
