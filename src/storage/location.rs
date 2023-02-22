use anyhow::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use object_store::path::Path as ObjStorePath;
use serde::{Deserialize, Serialize};

/// Data type that locates a piece of data (for now this is always a partition of a table,
/// stored as a parquet file).
///
/// the reason it uses [`url::Url`] is because datafusion's context uses types
/// that impl `AsRef<Url>` to look up object stores.
///
///
#[derive(Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct Location {
    inner: url::Url,
}
impl std::fmt::Debug for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Location {
    /// Create a new partition locator.
    ///
    ///
    ///
    /// `scheme` maps to the type of storage being used. e.g. `file`, `ipfs`, `s3`
    /// are examples of what scheme should be.
    ///
    /// `bucket` is often None as many stores wont have a buckets (file system, ipfs).
    ///
    /// `loc` is a path **relative to the root**.
    pub fn new<L>(scheme: &str, bucket: Option<&str>, loc: L) -> Self
    where
        L: Into<object_store::path::Path>,
    {
        let h = bucket.unwrap_or("");
        let loc = loc.into();
        let url = url::Url::parse(&format!("{scheme}://{h}/{loc}"))
            .expect("got unparseable characters when creating new PartitionURL");
        Self { inner: url }
    }
    fn parse(s: &str) -> Result<Self> {
        Ok(Self {
            inner: url::Url::parse(s)?,
        })
    }

    pub fn bucket(&self) -> Option<&str> {
        self.inner.host_str()
    }

    pub fn path(&self) -> ObjStorePath {
        self.inner.path().into()
    }
    pub fn scheme(&self) -> &str {
        self.inner.scheme()
    }
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}
impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Location {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl AsRef<url::Url> for Location {
    fn as_ref(&self) -> &url::Url {
        &self.inner
    }
}
impl From<Location> for ObjectStoreUrl {
    fn from(value: Location) -> Self {
        let scheme = value.scheme();
        let bucket = value.bucket().unwrap_or("");
        Self::parse(format!("{scheme}://{bucket}/")).unwrap()
    }
}
impl From<Location> for ObjStorePath {
    fn from(value: Location) -> Self {
        value.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_obj_store_url() {
        let loc1 = Location::new("ipfs", None, ObjStorePath::parse("somepath").unwrap());
        let loc2 = Location::new("ipfs", None, ObjStorePath::parse("someotherpath").unwrap());
        let osu1 = ObjectStoreUrl::from(loc1);
        let osu2 = ObjectStoreUrl::from(loc2);
        assert_eq!(osu1.as_str(), osu2.as_str());
        let locbucket = Location::new(
            "s3",
            Some("testy"),
            ObjStorePath::parse("someotherpath").unwrap(),
        );
        let locbucket2 = Location::new(
            "s3",
            Some("testy2"),
            ObjStorePath::parse("someotherpath").unwrap(),
        );
        assert_ne!(
            ObjectStoreUrl::from(locbucket).as_str(),
            ObjectStoreUrl::from(locbucket2).as_str()
        );
    }
}
