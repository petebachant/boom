// MongoDB specific functionality

/// Convert a JSON value to a MongoDB Document
pub fn json_to_document(value: &serde_json::Value) -> mongodb::bson::Document {
    match value {
        serde_json::Value::Object(map) => {
            let mut doc = mongodb::bson::Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), json_to_document(v));
            }
            doc
        }
        serde_json::Value::Array(arr) => {
            let arr_docs: Vec<_> = arr.iter().map(json_to_document).collect();
            mongodb::bson::Document::from_iter(vec![(
                "array".to_string(),
                mongodb::bson::Bson::Array(
                    arr_docs
                        .into_iter()
                        .map(mongodb::bson::Bson::Document)
                        .collect(),
                ),
            )])
        }
        _ => mongodb::bson::to_document(value).unwrap_or_default(),
    }
}
