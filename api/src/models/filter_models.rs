use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{ObjectBuilder, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(serde::Deserialize, Clone)]
pub struct FilterSubmissionBody {
    pub pipeline: Option<Vec<mongodb::bson::Document>>,
    pub permissions: Option<Vec<i32>>,
    pub catalog: Option<String>,
    pub id: Option<i32>,
}
impl ToSchema for FilterSubmissionBody {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("FilterSubmissionBody")
    }
}
impl PartialSchema for FilterSubmissionBody {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property(
                    "pipeline",
                    RefOr::T(Schema::Object(ObjectBuilder::new().build())),
                )
                .property(
                    "permissions",
                    RefOr::T(Schema::Object(ObjectBuilder::new().build())),
                )
                .property(
                    "catalog",
                    RefOr::T(Schema::Object(
                        ObjectBuilder::new()
                            .schema_type(utoipa::openapi::Type::String)
                            .build(),
                    )),
                )
                .property(
                    "id",
                    RefOr::T(Schema::Object(
                        ObjectBuilder::new()
                            .schema_type(utoipa::openapi::Type::Integer)
                            .build(),
                    )),
                )
                .build(),
        ))
    }
}
