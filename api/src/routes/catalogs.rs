use crate::models::response;

use actix_web::{HttpResponse, get, post, web};
use futures::StreamExt;
use futures::TryStreamExt;
use mongodb::{Database, bson::doc};
use std::collections::HashMap;
use std::fmt;

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

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct FindQuery {
    filter: mongodb::bson::Document,
    projection: Option<mongodb::bson::Document>,
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<mongodb::bson::Document>,
    max_time_ms: Option<u64>,
}

impl Default for FindQuery {
    fn default() -> Self {
        FindQuery {
            filter: doc! {},
            projection: None,
            limit: None,
            skip: None,
            sort: None,
            max_time_ms: None,
        }
    }
}

impl FindQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> mongodb::options::FindOptions {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = Some(projection.clone());
        }
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = Some(sort.clone());
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        }
        options
    }
}

/// Post a query to find records in a data catalog
#[post("/catalogs/{catalog_name}/queries/find")]
pub async fn post_catalog_find_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    body: web::Json<FindQuery>,
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
    // Find documents with the provided filter
    let filter = body.filter.clone();
    let find_options = body.to_find_options();
    match collection.find(filter).with_options(find_options).await {
        Ok(cursor) => {
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect::<Vec<_>>().await;
            response::ok("success", serde_json::to_value(docs).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error finding documents: {:?}", e)),
    }
}

#[derive(serde::Deserialize, Clone)]
pub enum Unit {
    Degrees,
    Radians,
    Arcseconds,
    Arcminutes,
}

impl fmt::Debug for Unit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Unit::Degrees => {
                write!(f, "{}", "Degrees")
            }
            Unit::Radians => {
                write!(f, "{}", "Radians")
            }
            Unit::Arcseconds => {
                write!(f, "{}", "Arcseconds")
            }
            Unit::Arcminutes => {
                write!(f, "{}", "Arcminutes")
            }
        }
    }
}

#[derive(serde::Deserialize, Clone)]
struct ConeSearchQuery {
    filter: mongodb::bson::Document,
    projection: Option<mongodb::bson::Document>,
    radius: f64,
    unit: Unit,
    object_coordinates: HashMap<String, [f64; 2]>, // Map of catalog name to coordinates [RA, Dec]
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<mongodb::bson::Document>,
    max_time_ms: Option<u64>,
}

impl ConeSearchQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> mongodb::options::FindOptions {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = Some(projection.clone());
        }
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = Some(sort.clone());
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        }
        options
    }
}

/// Post a query to do a cone search on a data catalog
#[post("/catalogs/{catalog_name}/queries/cone-search")]
pub async fn post_catalog_cone_search_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    body: web::Json<ConeSearchQuery>,
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
    // Perform cone search over each set of object coordinates
    let find_options = body.to_find_options();
    let mut radius = body.radius;
    let unit = body.unit.clone();
    // Convert radius to radians based on unit
    match unit {
        Unit::Degrees => radius = radius.to_radians(),
        Unit::Arcseconds => radius = radius.to_radians() / 3600.0,
        Unit::Arcminutes => radius = radius.to_radians() / 60.0,
        Unit::Radians => {}
    }
    let object_coordinates = &body.object_coordinates;
    let mut docs: HashMap<String, Vec<mongodb::bson::Document>> = HashMap::new();
    for (object_name, radec) in object_coordinates {
        let mut filter = body.filter.clone();
        let ra = radec[0] - 180.0;
        let dec = radec[1];
        let center_sphere = doc! {
            "$centerSphere": [[ra, dec], radius]
        };
        let geo_within = doc! {
            "$geoWithin": center_sphere
        };
        filter.insert("coordinates.radec_geojson", geo_within);
        let cursor = match collection
            .find(filter)
            .with_options(find_options.clone())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return response::internal_error(&format!("Error finding documents: {:?}", e));
            }
        };
        // Create map entry for this object's cone search
        let data = match cursor.try_collect::<Vec<mongodb::bson::Document>>().await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error collecting documents: {:?}", e));
            }
        };
        docs.insert(object_name.clone(), data);
    }
    return response::ok(
        &format!("Cone Search on {} completed", catalog_name),
        serde_json::json!(docs),
    );
}
