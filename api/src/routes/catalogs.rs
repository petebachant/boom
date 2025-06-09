use crate::models::response;
use actix_web::{HttpResponse, get, post, web};
use mongodb::{Database, bson::doc};

/// Get a list of catalogs
#[get("/catalogs")]
pub async fn get_catalogs(db: web::Data<Database>) -> HttpResponse {
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error getting catalog info: {:?}", e));
        }
    };
    let mut catalog_names = collection_names
        .iter()
        .filter(|name| !name.starts_with("system."))
        .cloned()
        .collect::<Vec<String>>();
    catalog_names.sort();
    let mut catalogs = Vec::new();
    for catalog in catalog_names {
        match db
            .run_command(doc! {
                "collstats": catalog
            })
            .await
        {
            Ok(d) => catalogs.push(d),
            Err(e) => {
                return response::internal_error(&format!("Error getting catalog info: {:?}", e));
            }
        };
    }
    // Serialize catalogs
    match serde_json::to_value(&catalogs) {
        Ok(v) => return response::ok("success", v),
        Err(e) => {
            return response::internal_error(&format!("Error serializing catalog info: {:?}", e));
        }
    };
}

#[derive(serde::Deserialize, Clone)]
struct DocumentCountQuery {
    filter: Option<mongodb::bson::Document>,
}

impl Default for DocumentCountQuery {
    fn default() -> Self {
        DocumentCountQuery {
            filter: Some(doc! {}),
        }
    }
}

/// Post a query to get the number of documents in a catalog
#[post("/catalogs/{catalog_name}/queries/count")]
pub async fn post_catalog_document_count_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    web::Json(query): web::Json<DocumentCountQuery>,
) -> HttpResponse {
    // Validate catalog name
    if catalog_name.is_empty() {
        return response::bad_request("Catalog name cannot be empty");
    }
    let catalog_name = catalog_name.into_inner();
    // Check that there is a collection with this catalog name
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error getting catalog info: {:?}", e));
        }
    };
    let catalog_names = collection_names
        .iter()
        .filter(|name| !name.starts_with("system."))
        .cloned()
        .collect::<Vec<String>>();
    if !catalog_names.contains(&catalog_name) {
        return response::bad_request(&format!("Catalog '{}' does not exist", catalog_name));
    }
    // Validate the query
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&catalog_name);
    // Count documents with optional filter
    let count = match collection
        .count_documents(query.filter.unwrap_or_default())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error counting documents: {:?}", e));
        }
    };
    // Return the count
    response::ok("success", serde_json::to_value(count).unwrap())
}
