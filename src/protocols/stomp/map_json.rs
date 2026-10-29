use std::collections::BTreeMap;

use serde_json::Value as Json;

use crate::core::Value;

pub(super) fn decode(bytes: &[u8]) -> Result<BTreeMap<String, Value>, String> {
    let json: Json = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid jms-map-json body: {error}"))?;
    let object = json.as_object().ok_or("jms-map-json requires an object")?;
    if object.len() == 1
        && let Some(map) = object.get("map").and_then(Json::as_object)
        && (map.is_empty() || map.contains_key("entry"))
    {
        let Some(entries) = map.get("entry") else {
            return Ok(BTreeMap::new());
        };
        let entries = match entries {
            Json::Array(entries) => entries.as_slice(),
            entry => std::slice::from_ref(entry),
        };
        let mut result = BTreeMap::new();
        for entry in entries {
            let entry = entry.as_object().ok_or("map entry requires an object")?;
            let (key, value) = match entry.get("string") {
                Some(Json::Array(strings)) if entry.len() == 1 && strings.len() == 2 => {
                    let key = strings[0].as_str().ok_or("map key must be a string")?;
                    let value = strings[1]
                        .as_str()
                        .ok_or("string map value must be a string")?;
                    (key, Value::String(value.to_owned()))
                }
                Some(Json::String(key)) if entry.len() == 2 => {
                    let (kind, json) = entry.iter().find(|(kind, _)| *kind != "string").unwrap();
                    (key.as_str(), typed_value(kind, json)?)
                }
                _ => return Err("map entry must contain a string key and one typed value".into()),
            };
            if result.insert(key.to_owned(), value).is_some() {
                return Err(format!("duplicate map key {key:?}"));
            }
        }
        return Ok(result);
    }
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), plain_value(value)?)))
        .collect()
}

fn plain_value(json: &Json) -> Result<Value, String> {
    match json {
        Json::Null => Ok(Value::Null),
        Json::Bool(value) => Ok(Value::Bool(*value)),
        Json::String(value) => Ok(Value::String(value.clone())),
        Json::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Long(value))
            } else if value.is_f64() {
                value
                    .as_f64()
                    .map(Value::Double)
                    .ok_or_else(|| "invalid floating point map value".into())
            } else {
                Err("map integer exceeds signed 64-bit range".into())
            }
        }
        Json::Array(_) | Json::Object(_) => {
            Err("JMS map values must be primitives, not nested arrays or objects".into())
        }
    }
}

fn typed_value(kind: &str, json: &Json) -> Result<Value, String> {
    let text = match json {
        Json::String(value) => value.clone(),
        Json::Number(value) => value.to_string(),
        Json::Bool(value) => value.to_string(),
        Json::Null if kind == "null" => return Ok(Value::Null),
        _ => return Err(format!("invalid {kind} map value")),
    };
    let invalid = || format!("invalid {kind} map value {text:?}");
    match kind {
        "byte" => text.parse().map(Value::Byte).map_err(|_| invalid()),
        "short" => text.parse().map(Value::Short).map_err(|_| invalid()),
        "int" => text.parse().map(Value::Int).map_err(|_| invalid()),
        "long" => text.parse().map(Value::Long).map_err(|_| invalid()),
        "boolean" => text.parse().map(Value::Bool).map_err(|_| invalid()),
        "float" => text
            .parse::<f32>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Value::Float)
            .ok_or_else(invalid),
        "double" => text
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Value::Double)
            .ok_or_else(invalid),
        "char" => {
            let mut chars = text.chars();
            match (chars.next(), chars.next()) {
                (Some(value), None) if value.len_utf16() == 1 => Ok(Value::Char(value as u16)),
                _ => Err(invalid()),
            }
        }
        "null" if text.is_empty() => Ok(Value::Null),
        _ => Err(format!("unsupported JMS map type {kind:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_classic_typed_entries_and_plain_objects() {
        let map = decode(
            br#"{"map":{"entry":[
            {"string":["name","order"]},
            {"string":"count","int":42},
            {"string":"small","byte":"-12"},
            {"string":"enabled","boolean":true},
            {"string":"price","double":1.5},
            {"string":"letter","char":"A"},
            {"string":"empty","null":""}
        ]}}"#,
        )
        .unwrap();
        assert_eq!(map["name"], Value::String("order".into()));
        assert_eq!(map["count"], Value::Int(42));
        assert_eq!(map["small"], Value::Byte(-12));
        assert_eq!(map["enabled"], Value::Bool(true));
        assert_eq!(map["price"], Value::Double(1.5));
        assert_eq!(map["letter"], Value::Char(65));
        assert_eq!(map["empty"], Value::Null);
        assert_eq!(decode(br#"{"a":42}"#).unwrap()["a"], Value::Long(42));
        assert!(decode(br#"{"map":{}}"#).unwrap().is_empty());
        assert_eq!(
            decode(br#"{"map":{"entry":{"string":["key","value"]}}}"#).unwrap()["key"],
            Value::String("value".into())
        );
    }

    #[test]
    fn rejects_invalid_and_unsupported_maps() {
        for json in [
            "[]",
            "{",
            r#"{"a":18446744073709551615}"#,
            r#"{"a":[]}"#,
            r#"{"map":{"entry":{"string":"n","byte":128}}}"#,
            r#"{"map":{"entry":{"string":"n","double":"NaN"}}}"#,
            r#"{"map":{"entry":[{"string":["k","v"]},{"string":["k","v2"]}]}}"#,
            r#"{"map":{"entry":{"string":"n","arbitrary-class":{}}}}"#,
        ] {
            assert!(decode(json.as_bytes()).is_err(), "{json}");
        }
    }
}
