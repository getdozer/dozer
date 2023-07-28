use dozer_types::models::api_security::ApiSecurity;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::AuthError;

use super::{Access, Claims};

pub struct Authorizer<'a> {
    secret: &'a [u8],
    aud: &'a str,
    sub: &'a str,
}

impl<'a> Authorizer<'a> {
    pub fn new(secret: &'a str, aud: Option<&'a str>, sub: Option<&'a str>) -> Self {
        Self {
            secret: secret.as_bytes(),
            aud: aud.unwrap_or("cache_user"),
            sub: sub.unwrap_or("api@dozer.com"),
        }
    }

    /// Creates exp based on duration provided with a default of 300 seconds
    pub fn get_expiry(dur: Option<Duration>) -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

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
    ) -> Result<String, AuthError> {
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
        .map_err(Into::into)
    }

    pub fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.sub = Some(self.sub.to_owned());
        validation.set_audience(&[self.aud.to_owned()]);

        match decode::<Claims>(token, &DecodingKey::from_secret(self.secret), &validation) {
            Ok(c) => Ok(c.claims),
            Err(err) => Err(err.into()),
        }
    }
}

impl<'a> From<&'a ApiSecurity> for Authorizer<'a> {
    fn from(value: &'a ApiSecurity) -> Self {
        match value {
            ApiSecurity::Jwt(secret) => Authorizer::new(secret, None, None),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::Access;
    use super::Authorizer;

    #[test]
    fn generate_and_verify_claim() {
        let auth_utils = Authorizer::new("secret", None, None);

        let token = auth_utils.generate_token(Access::All, None).unwrap();

        let token_data = auth_utils.validate_token(&token).unwrap();
        assert_eq!(token_data.access, Access::All, "must be equal");
    }
}
