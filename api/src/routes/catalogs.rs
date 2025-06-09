use crate::models::response;

use actix_web::{HttpResponse, get, post, web};
use mongodb::{Database, bson::doc};

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
    // TODO: Catalogs should have a prefix like "catalog"
    let mut catalog_names = collection_names
        .iter()
        .filter(|name| !name.starts_with("system."))
        .filter(|name| !name.starts_with("users"))
        .cloned()
        .collect::<Vec<String>>();
    catalog_names.sort();
    let mut catalogs = Vec::new();
    if params.get_details.unwrap_or_default() {
        for catalog in catalog_names {
            match db
                .run_command(doc! {
                    "collstats": &catalog
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
