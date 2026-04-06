use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum Offset {
    #[default]
    Start,
    Now,
    Value(String),
}

impl Offset {
    pub const START: &'static str = "-1";
    pub const NOW: &'static str = "now";

    #[must_use]
    pub fn start() -> Self {
        Self::Start
    }

    #[must_use]
    pub fn now() -> Self {
        Self::Now
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Start => Self::START,
            Self::Now => Self::NOW,
            Self::Value(value) => value.as_str(),
        }
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for Offset {
    fn from(value: &str) -> Self {
        match value {
            Self::START => Self::Start,
            Self::NOW => Self::Now,
            other => Self::Value(other.to_string()),
        }
    }
}

impl From<String> for Offset {
    fn from(value: String) -> Self {
        match value.as_str() {
            Self::START => Self::Start,
            Self::NOW => Self::Now,
            _ => Self::Value(value),
        }
    }
}

impl FromStr for Offset {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(crate::error::Error::InvalidHeader {
                operation: "parse",
                path: "<offset>".to_string(),
                name: "Stream-Next-Offset",
                reason: "offset cannot be empty".to_string(),
            });
        }
        Ok(Self::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::Offset;

    #[test]
    fn preserves_opaque_offsets() {
        let offset = Offset::from("abc123");
        assert_eq!(offset.as_str(), "abc123");
    }

    #[test]
    fn maps_reserved_sentinels() {
        assert_eq!(Offset::from("-1"), Offset::Start);
        assert_eq!(Offset::from("now"), Offset::Now);
    }
}
