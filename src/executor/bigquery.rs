//! BigQuery query executor via REST API.
//!
//! Supports two auth methods:
//! - `access_token` / `access_token_var`: a pre-obtained OAuth2 token (e.g. from
//!   `gcloud auth print-access-token`). Expires in ~an hour and must be
//!   refreshed out of band.
//! - `key_file` / `key_json` (+ `_var` forms): a service-account JSON key. The
//!   [`auth`] module mints its own access token from it — signing a JWT
//!   assertion with RS256 and redeeming it at Google's OAuth2 token endpoint —
//!   and caches the token until it nears expiry.

use super::{BigQueryConnection, ExecutionResult};
use crate::dialect::Dialect;
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &BigQueryConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let project = config.get_project()?;
    let token = config.get_access_token()?;

    let final_sql = inline_params(sql, params);

    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
        project
    );

    let mut body = serde_json::json!({
        "query": final_sql,
        "useLegacySql": false,
        "maxResults": 10000,
    });

    if let Some(ref dataset) = config.dataset {
        body["defaultDataset"] = serde_json::json!({
            "projectId": project,
            "datasetId": dataset,
        });
    }

    let resp = ureq::post(&url)
        .set("Authorization", &format!("Bearer {}", token))
        .set("Content-Type", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| EngineError::QueryError(format!("BigQuery request failed: {}", e)))?;

    let json: JsonValue = resp.into_json().map_err(|e| {
        EngineError::QueryError(format!("Failed to parse BigQuery response: {}", e))
    })?;

    if let Some(errors) = json["errors"].as_array() {
        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| e["message"].as_str().unwrap_or("unknown"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(EngineError::QueryError(format!(
                "BigQuery query failed: {}",
                msg
            )));
        }
    }

    // Check for error in status
    if let Some(err) = json.get("error") {
        return Err(EngineError::QueryError(format!(
            "BigQuery error: {}",
            err["message"].as_str().unwrap_or("unknown")
        )));
    }

    let schema_fields = json["schema"]["fields"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let columns: Vec<String> = schema_fields
        .iter()
        .map(|f| f["name"].as_str().unwrap_or("unknown").to_string())
        .collect();

    let bq_rows = json["rows"].as_array().cloned().unwrap_or_default();

    let mut rows = Vec::with_capacity(bq_rows.len());
    for bq_row in &bq_rows {
        let cells = bq_row["f"].as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let raw = cells
                .get(i)
                .and_then(|c| c.get("v"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            let typed = coerce_bigquery_value(&raw, schema_fields.get(i));
            obj.insert(col_name.clone(), typed);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn coerce_bigquery_value(val: &JsonValue, field: Option<&JsonValue>) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    let s = match val.as_str() {
        Some(s) => s,
        None => return val.clone(),
    };

    if let Some(field) = field {
        let bq_type = field["type"].as_str().unwrap_or("");
        match bq_type {
            "INTEGER" | "INT64" => {
                if let Ok(n) = s.parse::<i64>() {
                    return JsonValue::Number(n.into());
                }
            }
            "FLOAT" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC" => {
                if let Ok(n) = s.parse::<f64>() {
                    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                        return JsonValue::Number((n as i64).into());
                    }
                    return serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(s.to_string()));
                }
            }
            // BigQuery returns a TIMESTAMP as epoch seconds ("1767225600.0"),
            // which is unreadable as a date anywhere downstream — and outright
            // breaks the Parquet rollup cache, whose time bucket has to parse
            // as a timestamp. DATE/DATETIME/TIME already arrive as ISO text.
            "TIMESTAMP" => {
                if let Some(iso) = epoch_seconds_to_iso(s) {
                    return JsonValue::String(iso);
                }
            }
            "BOOLEAN" | "BOOL" => {
                return match s {
                    "true" | "TRUE" | "1" => JsonValue::Bool(true),
                    "false" | "FALSE" | "0" => JsonValue::Bool(false),
                    _ => JsonValue::String(s.to_string()),
                };
            }
            _ => {}
        }
    }

    JsonValue::String(s.to_string())
}

/// Render BigQuery's epoch-seconds TIMESTAMP encoding as an ISO datetime
/// (UTC, which is the zone a BigQuery TIMESTAMP is defined in).
fn epoch_seconds_to_iso(s: &str) -> Option<String> {
    // Split on the decimal point rather than going through `f64`: a
    // microsecond-precision timestamp does not survive the round trip (an
    // `f64` carries ~120ns of error at this magnitude, and `%.6f` truncates
    // rather than rounds, so `…123456` can come back as `…123455`).
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let mut whole: i64 = int_part.parse().ok()?;
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Pad or truncate to nanosecond precision.
    let mut nanos: i64 = format!("{:0<9}", frac_part).get(..9)?.parse::<i64>().ok()?;
    // A pre-1970 timestamp is written `-100.5`, meaning 100.5s *before* the
    // epoch, so the fraction moves the instant further back.
    if int_part.starts_with('-') {
        whole -= 1;
        nanos = 1_000_000_000 - nanos;
        if nanos == 1_000_000_000 {
            whole += 1;
            nanos = 0;
        }
    }
    let frac = nanos;
    let dt = chrono::DateTime::from_timestamp(whole, frac as u32)?;
    Some(dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
}

/// Service-account authentication: mint an OAuth2 access token from a JSON key.
///
/// The flow is Google's "JWT bearer" grant: build a short-lived assertion
/// claiming the service account's identity, sign it with the key's RSA private
/// key (RS256), and exchange it at the key's token endpoint for an access
/// token. `ring` does the signing; it is already in the build via `ureq`'s TLS.
///
/// Every error here is a fixed, redacted string. The inputs and intermediates
/// on this path — the key file's contents, the private key, the assertion, the
/// token endpoint's response body — are all either the secret itself or
/// derived from it, and the underlying error types quote their input freely.
pub mod auth {
    use super::BigQueryConnection;
    use crate::engine::EngineError;
    use base64::Engine as _;

    /// Scope requested for the minted token.
    const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
    /// Endpoint used when the key omits `token_uri`.
    const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
    /// Assertion lifetime. Google rejects anything over an hour.
    const ASSERTION_LIFETIME_SECS: i64 = 3600;
    /// Treat a token as expired this many seconds early, so a token that would
    /// die mid-flight is replaced before the request rather than after it.
    pub const REFRESH_SKEW_SECS: i64 = 60;

    const REDACT_PARSE: &str =
        "BigQuery service-account key is malformed (details redacted — it contains the private key)";
    const REDACT_SIGN: &str =
        "BigQuery service-account key could not be used to sign (details redacted — it contains the private key)";

    fn b64url(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// The fields airlayer needs out of a service-account JSON key.
    pub struct ServiceAccountKey {
        /// `client_email` — the assertion's issuer and subject.
        pub client_email: String,
        /// `private_key` — a PKCS#8 PEM RSA private key.
        private_key_pem: String,
        /// `token_uri` — where the assertion is redeemed, and therefore its
        /// audience.
        pub token_uri: String,
    }

    /// Redacting by hand, for the same reason as [`super::super::TokenCache`]:
    /// a derived impl would print the private key.
    impl std::fmt::Debug for ServiceAccountKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ServiceAccountKey(<redacted>)")
        }
    }

    impl ServiceAccountKey {
        pub fn from_json(json: &str) -> Result<Self, EngineError> {
            let value: serde_json::Value = serde_json::from_str(json)
                .map_err(|_| EngineError::QueryError(REDACT_PARSE.to_string()))?;
            let field = |name: &str| -> Result<String, EngineError> {
                value[name]
                    .as_str()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        // The field *name* is ours, not the user's secret.
                        EngineError::QueryError(format!(
                            "BigQuery service-account key is missing `{}` (rest of the key redacted)",
                            name
                        ))
                    })
            };
            Ok(Self {
                client_email: field("client_email")?,
                private_key_pem: field("private_key")?,
                token_uri: value["token_uri"]
                    .as_str()
                    .filter(|v| !v.is_empty())
                    .unwrap_or(DEFAULT_TOKEN_URI)
                    .to_string(),
            })
        }
    }

    /// Whether a token expiring at `expires_at` should be re-minted at `now`
    /// (both epoch seconds).
    pub fn needs_refresh(expires_at: i64, now: i64) -> bool {
        expires_at - now < REFRESH_SKEW_SECS
    }

    /// The signed JWT assertion for `key`, issued at `now` (epoch seconds).
    pub fn build_assertion(key: &ServiceAccountKey, now: i64) -> Result<String, EngineError> {
        let header = b64url(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "iss": key.client_email,
            "sub": key.client_email,
            "scope": SCOPE,
            "aud": key.token_uri,
            "iat": now,
            "exp": now + ASSERTION_LIFETIME_SECS,
        });
        let claims = b64url(claims.to_string().as_bytes());
        let signing_input = format!("{}.{}", header, claims);
        let signature = sign_rs256(&key.private_key_pem, signing_input.as_bytes())?;
        Ok(format!("{}.{}", signing_input, b64url(&signature)))
    }

    /// Strip PEM armor and base64-decode the body to DER.
    fn pem_to_der(pem: &str) -> Result<Vec<u8>, EngineError> {
        let body: String = pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .flat_map(|l| l.chars().filter(|c| !c.is_whitespace()))
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .map_err(|_| EngineError::QueryError(REDACT_SIGN.to_string()))
    }

    /// RSASSA-PKCS1-v1_5 over SHA-256, the `RS256` of the JWT spec.
    fn sign_rs256(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, EngineError> {
        let der = pem_to_der(private_key_pem)?;
        let key_pair = ring::signature::RsaKeyPair::from_pkcs8(&der)
            .map_err(|_| EngineError::QueryError(REDACT_SIGN.to_string()))?;
        let mut signature = vec![0u8; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                message,
                &mut signature,
            )
            .map_err(|_| EngineError::QueryError(REDACT_SIGN.to_string()))?;
        Ok(signature)
    }

    /// Redeem `assertion` at `token_uri`. Returns the token and its absolute
    /// expiry in epoch seconds.
    ///
    /// Only the HTTP status reaches the caller — the response body can echo the
    /// assertion back.
    fn exchange(token_uri: &str, assertion: &str, now: i64) -> Result<(String, i64), EngineError> {
        let response = ureq::post(token_uri)
            .send_form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion),
            ])
            .map_err(|e| {
                let status = match &e {
                    ureq::Error::Status(code, _) => format!("HTTP {}", code),
                    ureq::Error::Transport(t) => format!("transport error: {}", t.kind()),
                };
                EngineError::QueryError(format!(
                    "BigQuery service-account token exchange failed ({}; response redacted)",
                    status
                ))
            })?;

        let json: serde_json::Value = response.into_json().map_err(|_| {
            EngineError::QueryError(
                "BigQuery service-account token exchange returned an unreadable response \
                 (redacted)"
                    .to_string(),
            )
        })?;

        let token = json["access_token"]
            .as_str()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                EngineError::QueryError(
                    "BigQuery service-account token exchange returned no access token \
                     (response redacted)"
                        .to_string(),
                )
            })?;
        // Google always sends expires_in; fall back to the assertion lifetime.
        let expires_in = json["expires_in"]
            .as_i64()
            .unwrap_or(ASSERTION_LIFETIME_SECS);
        Ok((token.to_string(), now + expires_in))
    }

    /// A valid access token for `config`, from the cache when one is still
    /// good, otherwise freshly minted from the service-account key.
    pub fn access_token(config: &BigQueryConnection) -> Result<String, EngineError> {
        let now = chrono::Utc::now().timestamp();
        if let Some(token) = config.cached_token_at(now) {
            return Ok(token);
        }
        let key = ServiceAccountKey::from_json(&config.service_account_key_json()?)?;
        let assertion = build_assertion(&key, now)?;
        let (token, expires_at) = exchange(&key.token_uri, &assertion, now)?;
        config.cache_token(&token, expires_at);
        Ok(token)
    }
}

/// Inline @p0, @p1, ... parameters into the SQL as escaped string literals.
/// Escaping goes through `Dialect::escape_string_literal`, so a value
/// carrying a quote or a backslash means here exactly what it means on
/// the pre-aggregation tier, which inlines the same value itself.
fn inline_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("@p{}", i);
        let escaped = Dialect::BigQuery.escape_string_literal(param);
        result = result.replace(&placeholder, &format!("'{}'", escaped));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_coerces_to_iso() {
        // BigQuery hands back epoch seconds for a TIMESTAMP. Left alone it
        // reaches the Parquet rollup cache as "1767225600.0", which does not
        // parse as a time bucket and silently disables the whole cache.
        let field = serde_json::json!({"type": "TIMESTAMP"});
        let out =
            coerce_bigquery_value(&JsonValue::String("1767225600.0".to_string()), Some(&field));
        assert_eq!(out, JsonValue::String("2026-01-01T00:00:00.000000".into()));
    }

    #[test]
    fn test_timestamp_passthrough_when_not_epoch() {
        // Already-ISO values must survive untouched.
        let field = serde_json::json!({"type": "TIMESTAMP"});
        let out = coerce_bigquery_value(
            &JsonValue::String("2026-01-01 00:00:00 UTC".to_string()),
            Some(&field),
        );
        assert_eq!(out, JsonValue::String("2026-01-01 00:00:00 UTC".into()));
    }

    #[test]
    fn test_timestamp_before_epoch_keeps_its_fraction() {
        let field = serde_json::json!({"type": "TIMESTAMP"});
        let out = coerce_bigquery_value(&JsonValue::String("-100.5".to_string()), Some(&field));
        assert_eq!(out, JsonValue::String("1969-12-31T23:58:19.500000".into()));
    }

    #[test]
    fn test_timestamp_keeps_full_microsecond_precision() {
        // Routed through an f64 this comes back as `…123455`.
        let field = serde_json::json!({"type": "TIMESTAMP"});
        let out = coerce_bigquery_value(
            &JsonValue::String("1767225600.123456".to_string()),
            Some(&field),
        );
        assert_eq!(out, JsonValue::String("2026-01-01T00:00:00.123456".into()));
    }

    #[test]
    fn test_inline_params_basic() {
        let sql = "SELECT * FROM t WHERE x = @p0 AND y = @p1";
        let result = inline_params(sql, &["hello".into(), "world".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'hello' AND y = 'world'");
    }

    #[test]
    fn test_inline_params_single_quote_escaped() {
        let sql = "SELECT * FROM t WHERE x = @p0";
        let result = inline_params(sql, &["it's a test".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'it''s a test'");
    }

    #[test]
    fn test_inline_params_empty() {
        let sql = "SELECT 1";
        let result = inline_params(sql, &[]);
        assert_eq!(result, "SELECT 1");
    }
    #[test]
    fn test_inline_params_escapes_backslashes() {
        // BigQuery reads backslash escape sequences in single-quoted
        // literals, so a value carrying one has to be doubled here to match
        // what the engine (and the pre-agg tier, which inlines it itself)
        // means by it.
        let sql = "SELECT * FROM t WHERE x = @p0";
        assert_eq!(
            inline_params(sql, &["a\\b".into()]),
            "SELECT * FROM t WHERE x = 'a\\\\b'"
        );
        assert_eq!(
            inline_params(sql, &["O'Hara".into()]),
            "SELECT * FROM t WHERE x = 'O''Hara'"
        );
    }
}

#[cfg(test)]
mod auth_tests {
    use super::auth;
    use crate::executor::{BigQueryConnection, DatabaseConnection};

    /// A throwaway 2048-bit RSA key, PKCS#8 DER, base64. Stored without PEM
    /// armor on purpose: it is a test fixture, and a literal
    /// `BEGIN PRIVATE KEY` block in the source trips secret scanners.
    /// `test_key_pem` re-armors it, so the PEM parser is exercised too.
    const TEST_KEY_DER_B64: &str = concat!(
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCeO77xnEbn0E7vTukNIyQmfHRg",
        "tQ7dXJbf6uR+Npyaq56kLh38DcC9TDvT+UcDF9siYCL3JXN7gvMaDJAsWsdYZ8lc/kFOPg/ytpq6",
        "Yq279KWXPfA3efYDJgSv1tnPHqVB/TDdP2/TUjOgajFjLgt3IxuL/tTmVvb6At9R7LNZNb5IpqQw",
        "RXxtn3e8H3DJOgO9x2yjdHbZDVAUcWR12aNizdlbY9vgHpnWXTbnWkjhDD8Qbk3hhLJAUnQvo+CN",
        "T2VhCEYWLJk1QYOJYWnTqwd33MvAO8AciGKxhia+qoKxPcTSq5fCXTFbHO81PSCtqAiBQ7IZhNTT",
        "l/aIB6+9WyCVAgMBAAECggEAOVXGy39oBelqILaEJbl9COEBvmT83OMG3F4dq0oYlsbm9fCr9r/4",
        "/d6YsydZPtqvEZaqNMmCPdfmRKMWvquqHfOFeEe83CAK4VSfXjgRYdC1C8dqun9b0Co/eoOsaqtd",
        "EDsxMIoi9/yKLm8sNBbkGqhC/Ag7lrceSNsvllzhY8pOmI1hYnqLSUYMkQtwXN9AB2/yoR+VIDGO",
        "TYkPGmRIOH9VkCscCEpXctmWfeuGrHXFgE0/N3/67iJoxxBjKBaUQa6rWlKVaiAPJI4AmPCvt4Pn",
        "7EPIjH2JolN39BgMgCO5IMhW6n1l1mN5Ro0hXz/1TCfGl7ahTQvpbQjC0kzjJQKBgQDP6z0uef7e",
        "stZ3NK3IEdKlWmHo6I45QRujaGkWBKJqHmGMnBRGwK5k3Slqas3z5sWuSD3S4QaixLXTzi15EAxq",
        "50SorEViesWOvK/K6CqFd5DKbEuKY1lB9s+SlQgMuDMNmA6rjdai3G3atBjefKNkUHvsPR2iSGvf",
        "OnVj1AYOQwKBgQDC0yIYR9UUmGRx79npOPGz4+iqxoz0lLagGebF1E3d1MEu7G4T7WGc+tRr9ENo",
        "VBTj6z/pi8LYUZurAUdgz6iL+YL3CpAUUiLwupIQr2YRQi3vJoNlOq/gMGlYeg5B7GepIjZMIVVR",
        "xYzgtzCSDEO2B95I7vbjVVrJRpV060lkRwKBgQCqx/emIFDIDa95lMyVhIY4icfYboS75I3WKCIB",
        "EudxMOlBfMZu4z+b28lz/qyShWCkafRWLb2sntUXV8gkI5l/idzsiywm2t9BAh2HFjIvFOnaSx+1",
        "WStsslUHeuB0yiwtI1QRd9zwQwz80meHAGuZLz7K6dxYexIX9sWLrREYTwKBgQC3biJDyh4M7g5k",
        "V/dLZpnrTUHayGYeQYZQ5xIoYOOPYKkijOh3SqEFNAScP7bXm5KvpObf9P7WvL9cGjAiLmH6qElu",
        "XzuYZl0PWhn8K6hlx3GIITLFNKQy6GHHM+QInZRb4iJNO3UhMGabjN7mIzX9RRs8gAFjuRFpQFOn",
        "aYAIMQKBgBCzunzG6+AHR6+Os47PU7o1n9hOZH83e78uCX8kIorVc8o9LF2tZbBavYCwNKuKjbWh",
        "ixkglw55T2rAJ1FG7UpcPmkxOwULEx+Mr7qiqIHFH5WgKYZe/gb4Jzdn5tR/X4BbuJ9yV6wdawVJ",
        "h18vOStl6BQ7F34HHeLvciSoaHBB"
    );

    fn test_key_pem() -> String {
        let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
        for chunk in TEST_KEY_DER_B64.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END PRIVATE KEY-----\n");
        pem
    }

    fn test_key_json() -> String {
        serde_json::json!({
            "type": "service_account",
            "client_email": "airlayer@my-project.iam.gserviceaccount.com",
            "private_key": test_key_pem(),
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        .to_string()
    }

    fn parse_connection(json: serde_json::Value) -> BigQueryConnection {
        match serde_json::from_value::<DatabaseConnection>(json).expect("parse connection") {
            DatabaseConnection::Bigquery(bq) => bq,
            other => panic!("expected bigquery connection, got {}", other.dialect_str()),
        }
    }

    fn decode_segment(seg: &str) -> serde_json::Value {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(seg)
            .expect("segment is base64url");
        serde_json::from_slice(&bytes).expect("segment is JSON")
    }

    // ---- assertion construction -------------------------------------------

    #[test]
    fn test_assertion_header_is_rs256_jwt() {
        let key = auth::ServiceAccountKey::from_json(&test_key_json()).expect("parse key");
        let assertion = auth::build_assertion(&key, 1_700_000_000).expect("build assertion");
        let header = decode_segment(assertion.split('.').next().unwrap());
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
    }

    #[test]
    fn test_assertion_claims_carry_issuer_scope_and_audience() {
        let key = auth::ServiceAccountKey::from_json(&test_key_json()).expect("parse key");
        let assertion = auth::build_assertion(&key, 1_700_000_000).expect("build assertion");
        let claims = decode_segment(assertion.split('.').nth(1).unwrap());
        assert_eq!(claims["iss"], "airlayer@my-project.iam.gserviceaccount.com");
        assert_eq!(claims["scope"], "https://www.googleapis.com/auth/bigquery");
        assert_eq!(claims["aud"], "https://oauth2.googleapis.com/token");
    }

    #[test]
    fn test_assertion_expiry_is_one_hour_after_issue() {
        let key = auth::ServiceAccountKey::from_json(&test_key_json()).expect("parse key");
        let assertion = auth::build_assertion(&key, 1_700_000_000).expect("build assertion");
        let claims = decode_segment(assertion.split('.').nth(1).unwrap());
        assert_eq!(claims["iat"], 1_700_000_000i64);
        assert_eq!(claims["exp"], 1_700_003_600i64);
    }

    #[test]
    fn test_assertion_signature_verifies_against_the_public_key() {
        // The whole point of the crypto: Google must be able to verify what we
        // signed. Verify it ourselves with the matching public key.
        let key = auth::ServiceAccountKey::from_json(&test_key_json()).expect("parse key");
        let assertion = auth::build_assertion(&key, 1_700_000_000).expect("build assertion");
        let (signing_input, sig_b64) = assertion.rsplit_once('.').unwrap();

        use base64::Engine;
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(sig_b64)
            .expect("signature is base64url");

        let der = base64::engine::general_purpose::STANDARD
            .decode(TEST_KEY_DER_B64)
            .unwrap();
        let pair = ring::signature::RsaKeyPair::from_pkcs8(&der).expect("key pair");
        let public = ring::signature::UnparsedPublicKey::new(
            &ring::signature::RSA_PKCS1_2048_8192_SHA256,
            pair.public().as_ref(),
        );
        public
            .verify(signing_input.as_bytes(), &sig)
            .expect("signature verifies");
    }

    #[test]
    fn test_assertion_uses_the_keys_own_token_uri() {
        // Emulator / private-endpoint keys carry a non-default token_uri, and
        // `aud` must match wherever the assertion is actually redeemed.
        let json = serde_json::json!({
            "client_email": "svc@example.iam.gserviceaccount.com",
            "private_key": test_key_pem(),
            "token_uri": "https://oauth2.example.test/token",
        })
        .to_string();
        let key = auth::ServiceAccountKey::from_json(&json).expect("parse key");
        assert_eq!(key.token_uri, "https://oauth2.example.test/token");
        let assertion = auth::build_assertion(&key, 1_700_000_000).expect("build assertion");
        let claims = decode_segment(assertion.split('.').nth(1).unwrap());
        assert_eq!(claims["aud"], "https://oauth2.example.test/token");
    }

    // ---- refresh decision --------------------------------------------------

    #[test]
    fn test_needs_refresh_boundaries() {
        // Refresh once the token is within the skew window of expiring.
        let exp = 1_000_000i64;
        assert!(!auth::needs_refresh(exp, exp - auth::REFRESH_SKEW_SECS - 1));
        assert!(!auth::needs_refresh(exp, exp - auth::REFRESH_SKEW_SECS));
        assert!(auth::needs_refresh(exp, exp - auth::REFRESH_SKEW_SECS + 1));
        assert!(auth::needs_refresh(exp, exp));
        assert!(auth::needs_refresh(exp, exp + 3600));
    }

    #[test]
    fn test_cached_token_is_reused_until_it_nears_expiry() {
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "key_json": test_key_json(),
        }));
        conn.cache_token("tok-1", 1_000_000);
        assert_eq!(
            conn.cached_token_at(1_000_000 - 3600).as_deref(),
            Some("tok-1")
        );
        // Inside the skew window it is treated as gone, so a mint is forced.
        assert_eq!(conn.cached_token_at(1_000_000 - 10), None);
    }

    #[test]
    fn test_cache_is_shared_across_clones() {
        // `execute` takes `&BigQueryConnection` but callers clone connections
        // freely; a per-clone cache would re-mint on every query.
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "key_json": test_key_json(),
        }));
        let clone = conn.clone();
        conn.cache_token("tok-1", 1_000_000);
        assert_eq!(clone.cached_token_at(0).as_deref(), Some("tok-1"));
    }

    // ---- config parsing ----------------------------------------------------

    #[test]
    fn test_access_token_config_still_works() {
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "my-project",
            "dataset": "analytics",
            "access_token": "ya29.token",
        }));
        assert_eq!(conn.get_project().unwrap(), "my-project");
        assert_eq!(conn.get_access_token().unwrap(), "ya29.token");
    }

    #[test]
    fn test_key_file_config_parses() {
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "my-project",
            "key_file": "/path/to/sa.json",
        }));
        assert_eq!(conn.key_file.as_deref(), Some("/path/to/sa.json"));
        assert!(conn.has_service_account_key());
    }

    #[test]
    fn test_key_file_var_resolves_from_env() {
        std::env::set_var("AIRLAYER_TEST_BQ_KEY_FILE", "/from/env/sa.json");
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "key_file_var": "AIRLAYER_TEST_BQ_KEY_FILE",
        }));
        assert!(conn.has_service_account_key());
        std::env::remove_var("AIRLAYER_TEST_BQ_KEY_FILE");
    }

    #[test]
    fn test_key_file_is_read_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sa.json");
        std::fs::write(&path, test_key_json()).unwrap();
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "key_file": path.to_str().unwrap(),
        }));
        let key = auth::ServiceAccountKey::from_json(&conn.service_account_key_json().unwrap())
            .expect("parse key");
        assert_eq!(
            key.client_email,
            "airlayer@my-project.iam.gserviceaccount.com"
        );
    }

    #[test]
    fn test_access_token_wins_over_service_account_key() {
        // A pre-minted token is cheaper than a round trip to Google, and an
        // operator who set both meant the explicit one.
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "access_token": "ya29.explicit",
            "key_json": test_key_json(),
        }));
        assert_eq!(conn.get_access_token().unwrap(), "ya29.explicit");
    }

    #[test]
    fn test_no_auth_configured_is_an_actionable_error() {
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
        }));
        let err = conn.get_access_token().unwrap_err().to_string();
        assert!(err.contains("access_token"), "got: {}", err);
        assert!(err.contains("key_file"), "got: {}", err);
    }

    // ---- redaction ---------------------------------------------------------

    #[test]
    fn test_malformed_key_json_error_leaks_nothing() {
        let secret = "-----BEGIN PRIVATE KEY-----SUPERSECRET-----END PRIVATE KEY-----";
        let json = format!(
            r#"{{"client_email":"svc@example.com","private_key":"{}","trailing"#,
            secret
        );
        let err = auth::ServiceAccountKey::from_json(&json)
            .unwrap_err()
            .to_string();
        assert!(!err.contains("SUPERSECRET"), "leaked key: {}", err);
        assert!(!err.contains("BEGIN PRIVATE KEY"), "leaked key: {}", err);
        assert!(err.contains("redacted"), "got: {}", err);
    }

    #[test]
    fn test_unparseable_private_key_error_leaks_nothing() {
        let json = serde_json::json!({
            "client_email": "svc@example.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nSUPERSECRETnotbase64!!\n-----END PRIVATE KEY-----\n",
        })
        .to_string();
        let key = auth::ServiceAccountKey::from_json(&json).expect("parse key");
        let err = auth::build_assertion(&key, 0).unwrap_err().to_string();
        assert!(!err.contains("SUPERSECRET"), "leaked key: {}", err);
        assert!(err.contains("redacted"), "got: {}", err);
    }

    #[test]
    fn test_wrong_shaped_der_error_leaks_nothing() {
        // Well-formed base64 that is not a PKCS#8 RSA key: ring's own error
        // text must not reach the user carrying key bytes with it.
        let json = serde_json::json!({
            "client_email": "svc@example.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nU1VQRVJTRUNSRVRieXRlcw==\n-----END PRIVATE KEY-----\n",
        })
        .to_string();
        let key = auth::ServiceAccountKey::from_json(&json).expect("parse key");
        let err = auth::build_assertion(&key, 0).unwrap_err().to_string();
        assert!(!err.contains("SUPERSECRET"), "leaked key: {}", err);
        assert!(!err.contains("U1VQRVJT"), "leaked key: {}", err);
        assert!(err.contains("redacted"), "got: {}", err);
    }

    #[test]
    fn test_debug_of_connection_does_not_print_the_cached_token() {
        // `DatabaseConnection` is `Debug` and lands in error paths and logs.
        let conn = parse_connection(serde_json::json!({
            "type": "bigquery",
            "name": "bq",
            "project": "p",
            "key_json": test_key_json(),
        }));
        conn.cache_token("ya29.supersecret", i64::MAX);
        let rendered = format!("{:?}", conn);
        assert!(
            !rendered.contains("ya29.supersecret"),
            "leaked token: {}",
            rendered
        );
    }
}
