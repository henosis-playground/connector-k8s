//! Versioned connector-owned component context.

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Current wire-level context version.
pub const API_VERSION: &str = "henosis.dev/k8s-component-context/v1";

/// Strict JSON object carried in `ComponentSpec.connector_context`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ComponentContext {
    /// Versioned schema discriminator.
    pub api_version: String,
    /// Environment identity that owns the rendered branch.
    pub environment: EnvironmentContext,
    /// Immutable component source pin.
    pub source: SourceContext,
    /// Immutable workload image pin.
    pub image: ImageContext,
}

/// Environment fields required by the renderer and publisher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentContext {
    /// Stable environment token or `preview_` `TypeID`.
    pub id: String,
}

/// Source fields consumed by the platform manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceContext {
    /// GitHub repository in `owner/name` form.
    pub repository: String,
    /// Full immutable Git commit SHA.
    pub revision: String,
}

/// Workload image fields consumed by the platform manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageContext {
    /// OCI digest in `sha256:<64 lowercase hex characters>` form.
    pub digest: String,
}

/// Context decoding or validation failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContextError {
    /// The context is not strict UTF-8 JSON matching the current schema.
    #[error("invalid {API_VERSION} JSON: {0}")]
    Json(String),
    /// A field violates the current schema's semantic constraints.
    #[error("invalid {API_VERSION}: {0}")]
    Invalid(String),
}

impl ComponentContext {
    /// Decode and validate a context without accepting unknown versions or
    /// fields.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ContextError> {
        let context: Self =
            serde_json::from_slice(bytes).map_err(|error| ContextError::Json(error.to_string()))?;
        context.validate()?;
        Ok(context)
    }

    /// Validate every semantic constraint in the v1 vocabulary.
    pub fn validate(&self) -> Result<(), ContextError> {
        if self.api_version != API_VERSION {
            return Err(ContextError::Invalid(format!(
                "unsupported apiVersion {:?}; expected {API_VERSION:?}",
                self.api_version
            )));
        }
        validate_environment_id(&self.environment.id)?;
        validate_repository(&self.source.repository)?;
        if !is_lower_hex(&self.source.revision, 40) {
            return Err(ContextError::Invalid(
                "source.revision must be a full 40-character lowercase Git SHA".into(),
            ));
        }
        let digest = self.image.digest.strip_prefix("sha256:").ok_or_else(|| {
            ContextError::Invalid("image.digest must use the sha256 algorithm".into())
        })?;
        if !is_lower_hex(digest, 64) {
            return Err(ContextError::Invalid(
                "image.digest must contain 64 lowercase hexadecimal characters".into(),
            ));
        }
        Ok(())
    }
}

/// Validate a platform manifest component/environment name.
pub fn validate_dns_label(value: &str, field: &str) -> Result<(), ContextError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if valid {
        Ok(())
    } else {
        Err(ContextError::Invalid(format!(
            "{field} must be a lowercase DNS label of at most 63 characters"
        )))
    }
}

fn validate_environment_id(value: &str) -> Result<(), ContextError> {
    if let Some(suffix) = value.strip_prefix("preview_") {
        const ALPHABET: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";
        let valid = suffix.len() == 26
            && suffix.as_bytes().first().is_some_and(|byte| *byte <= b'7')
            && suffix.bytes().all(|byte| ALPHABET.contains(&byte));
        return if valid {
            Ok(())
        } else {
            Err(ContextError::Invalid(
                "environment.id preview suffix must be a canonical 26-character TypeID suffix"
                    .into(),
            ))
        };
    }
    validate_dns_label(value, "environment.id")?;
    if value == "preview" || value.starts_with("preview-") {
        return Err(ContextError::Invalid(
            "environment.id reserves preview identities for canonical preview_ TypeIDs".into(),
        ));
    }
    Ok(())
}

fn validate_repository(value: &str) -> Result<(), ContextError> {
    let mut parts = value.split('/');
    let owner = parts.next().unwrap_or_default();
    let name = parts.next().unwrap_or_default();
    let valid_part = |part: &str| {
        !part.is_empty()
            && part.len() <= 100
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    };
    if parts.next().is_none() && valid_part(owner) && valid_part(name) {
        Ok(())
    } else {
        Err(ContextError::Invalid(
            "source.repository must be an owner/name GitHub repository".into(),
        ))
    }
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_context() -> ComponentContext {
        ComponentContext {
            api_version: API_VERSION.into(),
            environment: EnvironmentContext {
                id: "preview_3jhc7x633z88188fzqhcbbrf84".into(),
            },
            source: SourceContext {
                repository: "henosis-playground/service-a".into(),
                revision: "a".repeat(40),
            },
            image: ImageContext {
                digest: format!("sha256:{}", "b".repeat(64)),
            },
        }
    }

    #[test]
    fn accepts_ratified_preview_type_id() {
        valid_context().validate().unwrap();
    }

    #[test]
    fn context_v1_bytes_are_a_fixed_producer_contract() {
        let bytes = serde_json::to_vec(&valid_context()).unwrap();
        let expected = format!(
            "{{\"apiVersion\":\"henosis.dev/k8s-component-context/v1\",\"environment\":{{\"id\":\"\
             preview_3jhc7x633z88188fzqhcbbrf84\"}},\"source\":{{\"repository\":\"\
             henosis-playground/service-a\",\"revision\":\"{}\"}},\"image\":{{\"digest\":\"sha256:\
             {}\"}}}}",
            "a".repeat(40),
            "b".repeat(64)
        );
        assert_eq!(bytes, expected.as_bytes());
        assert_eq!(
            ComponentContext::from_bytes(&bytes).unwrap(),
            valid_context()
        );
    }

    #[test]
    fn rejects_legacy_and_noncanonical_preview_ids() {
        for id in [
            "preview-728b0fd3-0c7f-4202-843f-f78b16bc3d04",
            "preview_8jhc7x633z88188fzqhcbbrf84",
            "preview_3JHC7X633Z88188FZQHCBBrF84",
        ] {
            let mut context = valid_context();
            context.environment.id = id.into();
            assert!(context.validate().is_err(), "accepted {id}");
        }
    }

    #[test]
    fn rejects_unknown_json_fields() {
        let mut value = serde_json::to_value(valid_context()).unwrap();
        value["extra"] = serde_json::json!(true);
        assert!(ComponentContext::from_bytes(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
