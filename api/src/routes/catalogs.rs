use crate::models::response;

use actix_web::{HttpResponse, get, post, web};
use futures::StreamExt;
use mongodb::{Database, bson::doc};

/// Helper function to add a prefix to a catalog name to get its collection name
fn get_collection_name(catalog_name: &str) -> String {
    // Assuming catalogs are prefixed with "catalog_"
    format!("catalog_{}", catalog_name.to_lowercase())
}

#[derive(serde::Deserialize)]
struct CatalogsQueryParams {
    get_details: Option<bool>,
}
impl Default for CatalogsQueryParams {
    fn default() -> Self {
        CatalogsQueryParams {
            get_details: Some(false),
        }
    }
}

/// Get a list of catalogs
#[get("/catalogs")]
pub async fn get_catalogs(
    db: web::Data<Database>,
    params: web::Query<CatalogsQueryParams>,
) -> HttpResponse {
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error getting catalog info: {:?}", e));
        }
    };
    // Catalogs should have a prefix like "catalog_"
    let collection_names = collection_names
        .iter()
        .filter(|name| name.starts_with("catalog_"))
        .cloned()
        .collect::<Vec<String>>();
    // Remove the prefix to get the catalog names
    let mut catalog_names: Vec<String> = collection_names
        .iter()
        .map(|name| name.trim_start_matches("catalog_").to_string())
        .collect();
    catalog_names.sort();
    let mut catalogs = Vec::new();
    if params.get_details.unwrap_or_default() {
        for catalog in catalog_names {
            match db
                .run_command(doc! {
                    "collstats": get_collection_name(&catalog)
                })
                .await
            {
                Ok(d) => catalogs.push(doc! {"name": catalog, "details": d}),
                Err(e) => {
                    return response::internal_error(&format!(
                        "Error getting catalog info: {:?}",
                        e
                    ));
                }
            };
        }
    } else {
        // If no details requested, just return the names
        for catalog in catalog_names {
            catalogs.push(doc! { "name": catalog });
        }
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
struct CountQuery {
    filter: Option<mongodb::bson::Document>,
}

impl Default for CountQuery {
    fn default() -> Self {
        CountQuery {
            filter: Some(doc! {}),
        }
    }
}

/// Post a query to get the number of documents in a catalog
#[post("/catalogs/{catalog_name}/queries/count")]
pub async fn post_catalog_count_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    web::Json(query): web::Json<CountQuery>,
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
    let collection_name = get_collection_name(&catalog_name);
    if !collection_names.contains(&collection_name) {
        return response::bad_request(&format!("Catalog '{}' does not exist", catalog_name));
    }
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
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

/// Get index information for a catalog
#[get("/catalogs/{catalog_name}/indexes")]
pub async fn get_catalog_indexes(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
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
    let collection_name = get_collection_name(&catalog_name);
    if !collection_names.contains(&collection_name) {
        return response::bad_request(&format!("Catalog '{}' does not exist", catalog_name));
    }
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Get index information
    match collection.list_indexes().await {
        Ok(indexes) => {
            let indexes: Vec<_> = indexes
                .map(|index| index.unwrap())
                .collect::<Vec<_>>()
                .await;
            response::ok("success", serde_json::to_value(indexes).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error getting indexes: {:?}", e)),
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct SampleQuery {
    size: Option<u16>,
}

impl Default for SampleQuery {
    fn default() -> Self {
        SampleQuery { size: Some(1) }
    }
}

/// Get a sample of data from a catalog
#[get("/catalogs/{catalog_name}/sample")]
pub async fn get_catalog_sample(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    params: web::Query<SampleQuery>,
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
    let collection_name = get_collection_name(&catalog_name);
    if !collection_names.contains(&collection_name) {
        return response::bad_request(&format!("Catalog '{}' does not exist", catalog_name));
    }
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Get a sample of documents
    match collection
        .aggregate(vec![
            doc! { "$sample": { "size": params.size.unwrap_or_default() as i32 } },
        ])
        .await
    {
        Ok(cursor) => {
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect::<Vec<_>>().await;
            response::ok("success", serde_json::to_value(docs).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error getting sample: {:?}", e)),
    }
}
