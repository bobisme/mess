//! Dependency-free canonical JSON primitives and neutral counter schema.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InputCounters {
    pub waiter_reservations_after: Option<u64>,
    pub byte_reservations_after:   Option<u64>,
    pub owned_batches:             Option<u64>,
    pub owned_records:             Option<u64>,
    pub owned_payload_bytes:       Option<u64>,
    pub borrowed_batches:          Option<u64>,
    pub borrowed_records:          Option<u64>,
    pub borrowed_payload_bytes:    Option<u64>,
    pub copied_records:            Option<u64>,
    pub copied_bytes:              Option<u64>,
}

fn escape_json(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            value if value <= '\u{1f}' => {
                use std::fmt::Write as _;
                write!(&mut output, "\\u{:04x}", value as u32)
                    .expect("write string");
            }
            value => output.push(value),
        }
    }
    output.push('"');
    output
}

pub fn json_string(value: &str) -> String { escape_json(value) }

pub fn json_u64(value: u64) -> String { value.to_string() }

pub fn json_bool(value: bool) -> String {
    if value { "true" } else { "false" }.to_owned()
}

pub fn json_optional(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

pub fn json_available(value: Option<u64>) -> String {
    value
        .map_or_else(|| json_string("not_available"), |value| value.to_string())
}

pub fn canonical_object(fields: &[(&str, String)]) -> String {
    assert!(
        fields.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "JSON keys are not sorted"
    );
    let mut output = String::from("{");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&escape_json(key));
        output.push(':');
        output.push_str(value);
    }
    output.push('}');
    output
}
