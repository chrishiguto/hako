//! The generator every committed schema shares, behind the `schema`
//! feature so product crates never carry schemars. Each artifact's
//! module owns its root function — [`crate::flow::json_schema`], the
//! per-stage [`crate::pipeline::stage_schema`] — but all of them must
//! generate through here, so every artifact carries the same
//! corrections.

use schemars::generate::SchemaSettings;
use schemars::transform::{Transform, transform_subschemas};
use schemars::{JsonSchema, Schema};

/// Generates a root schema with the house transforms applied.
pub(crate) fn root_schema_for<T: JsonSchema>() -> Schema {
    SchemaSettings::default()
        .with_transform(BoundIntegerFormats)
        .into_generator()
        .into_root_schema_for::<T>()
}

/// Stamps explicit bounds onto every sub-64-bit integer in the
/// schema. schemars emits only `minimum: 0` for them, and JSON
/// Schema treats `format` as an annotation — without real bounds
/// the schema would bless out-of-range values strict serde
/// rejects.
#[derive(Debug, Clone)]
struct BoundIntegerFormats;

impl Transform for BoundIntegerFormats {
    fn transform(&mut self, schema: &mut Schema) {
        let format = schema.get("format").and_then(serde_json::Value::as_str);
        if let Some((minimum, maximum)) = format.and_then(integer_bounds) {
            schema.insert("minimum".into(), minimum);
            schema.insert("maximum".into(), maximum);
        }
        transform_subschemas(self, schema);
    }
}

/// `uint{N}`/`int{N}` → that type's bounds, derived from the bit
/// width so any integer a config field adopts is covered. Formats
/// of 64 bits and up get none: TOML integers are i64, so their
/// bounds are unreachable from a flow file.
fn integer_bounds(format: &str) -> Option<(serde_json::Value, serde_json::Value)> {
    let (signed, bits) = match format.strip_prefix("uint") {
        Some(bits) => (false, bits),
        None => (true, format.strip_prefix("int")?),
    };
    let bits: u32 = bits.parse().ok()?;
    if !(1..64).contains(&bits) {
        return None;
    }
    Some(if signed {
        let magnitude = 1i64 << (bits - 1);
        (
            serde_json::json!(-magnitude),
            serde_json::json!(magnitude - 1),
        )
    } else {
        (serde_json::json!(0), serde_json::json!((1u64 << bits) - 1))
    })
}
