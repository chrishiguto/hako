//! The structural guards every generated schema must pass, shared by
//! the per-artifact agreement suites. They walk every node — not just
//! the top-level `$defs` — so future inline subschemas stay covered.

/// Depth-first visit of every object node in the schema, with its
/// path — the one walker the guards share.
fn walk_objects(
    node: &serde_json::Value,
    path: &str,
    visit: &mut impl FnMut(&serde_json::Map<String, serde_json::Value>, &str),
) {
    match node {
        serde_json::Value::Object(entries) => {
            visit(entries, path);
            for (key, entry) in entries {
                walk_objects(entry, &format!("{path}/{key}"), visit);
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk_objects(item, &format!("{path}/{index}"), visit);
            }
        }
        _ => {}
    }
}

/// The schema must carry the same strictness as the serde types:
/// every object schema anywhere in it rejects unknown keys, or an
/// editor would bless documents strict serde rejects.
pub fn assert_every_object_rejects_unknown_keys(schema: &serde_json::Value) {
    let mut found = 0;
    walk_objects(schema, "", &mut |entries, path| {
        let is_object_schema = entries.contains_key("properties")
            || entries.get("type") == Some(&serde_json::json!("object"));
        if is_object_schema {
            found += 1;
            assert_eq!(
                entries.get("additionalProperties"),
                Some(&serde_json::json!(false)),
                "{path}: object schema does not reject unknown keys"
            );
        }
    });
    assert!(found > 0, "the walk matched no object schemas");
}

/// Every sub-64-bit integer anywhere in the schema must carry the
/// explicit bounds the generator's transform stamps — a field with an
/// integer type the transform doesn't know fails here instead of
/// becoming a hole editors would bless. The 64-bit formats are
/// exempt: their bounds are unreachable from TOML's i64 integers.
/// Returns how many formats it saw, so a suite whose schema has
/// integers can assert the guard actually bit.
pub fn assert_integer_formats_carry_bounds(schema: &serde_json::Value) -> usize {
    let mut found = 0;
    walk_objects(schema, "", &mut |entries, path| {
        let sub_64_bit = |f: &&str| f.contains("int") && *f != "uint64" && *f != "int64";
        let format = entries.get("format").and_then(serde_json::Value::as_str);
        if let Some(format) = format.filter(sub_64_bit) {
            found += 1;
            for bound in ["minimum", "maximum"] {
                assert!(
                    entries.contains_key(bound),
                    "{path}: format `{format}` lacks `{bound}` — teach the \
                     generator's bounds transform this format or use a bounded type"
                );
            }
        }
    });
    found
}
