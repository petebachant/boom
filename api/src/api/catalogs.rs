use crate::models::response;
use actix_web::{HttpResponse, get, web};
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
    let _ = match serde_json::to_value(&catalogs) {
        Ok(v) => return response::ok("success", v),
        Err(e) => {
            return response::internal_error(&format!("Error serializing catalog info: {:?}", e));
        }
    };
}
