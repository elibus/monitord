//! Shared helpers for batching D-Bus property reads.
//!
//! When several properties are needed from the same interface on the same object,
//! that's N round trips to dbus where one `Properties.GetAll` would do. These helpers
//! make that batching easy for callers that need more than one property from
//! a single interface.

use std::collections::HashMap;

use zbus::zvariant::{ObjectPath, OwnedValue};

const SYSTEMD_DESTINATION: &str = "org.freedesktop.systemd1";

pub(crate) async fn get_all_properties(
    connection: &zbus::Connection,
    object_path: &ObjectPath<'_>,
    interface: &str,
) -> zbus::Result<HashMap<String, OwnedValue>> {
    let proxy = zbus::fdo::PropertiesProxy::builder(connection)
        .destination(SYSTEMD_DESTINATION)?
        .path(object_path)?
        .build()
        .await?;
    proxy
        .get_all(zbus::names::InterfaceName::try_from(interface)?)
        .await
        .map_err(Into::into)
}

pub(crate) fn extract_property<T>(
    props: &HashMap<String, OwnedValue>,
    name: &str,
) -> zbus::Result<T>
where
    T: TryFrom<OwnedValue>,
    T::Error: Into<zbus::Error>,
{
    let value = props
        .get(name)
        .ok_or_else(|| zbus::Error::Failure(format!("Missing property {name}")))?
        .clone();
    T::try_from(value).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props_with(name: &str, value: OwnedValue) -> HashMap<String, OwnedValue> {
        HashMap::from([(name.to_string(), value)])
    }

    #[test]
    fn test_extract_property_found_correct_type() {
        let props = props_with(
            "StateChangeTimestamp",
            zbus::zvariant::Value::from(42u64).try_into().unwrap(),
        );
        let value: u64 = extract_property(&props, "StateChangeTimestamp").unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_extract_property_missing_key() {
        let props: HashMap<String, OwnedValue> = HashMap::new();
        let result: zbus::Result<u64> = extract_property(&props, "StateChangeTimestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_property_wrong_type() {
        let props = props_with(
            "StateChangeTimestamp",
            zbus::zvariant::Value::from("not a number")
                .try_into()
                .unwrap(),
        );
        let result: zbus::Result<u64> = extract_property(&props, "StateChangeTimestamp");
        assert!(result.is_err());
    }
}
