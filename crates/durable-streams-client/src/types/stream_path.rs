use crate::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamPath(String);

impl StreamPath {
    /// Validate and construct a stream path.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is empty or not absolute.
    pub fn new(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        if path.is_empty() {
            return Err(Error::InvalidStreamPath {
                path,
                reason: "path cannot be empty".to_string(),
            });
        }
        if !path.starts_with('/') {
            return Err(Error::InvalidStreamPath {
                path,
                reason: "path must start with `/`".to_string(),
            });
        }
        Ok(Self(path))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for StreamPath {
    fn from(value: &str) -> Self {
        Self::new(value).expect("stream path must start with `/`")
    }
}

impl From<String> for StreamPath {
    fn from(value: String) -> Self {
        Self::new(value).expect("stream path must start with `/`")
    }
}

#[cfg(test)]
mod tests {
    use super::StreamPath;

    #[test]
    fn accepts_absolute_paths() {
        let path = StreamPath::new("/v1/stream/orders").unwrap();
        assert_eq!(path.as_str(), "/v1/stream/orders");
    }
}
