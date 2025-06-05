use crate::models::response;
use actix_web::{HttpResponse, delete, get, post, web};
use futures::stream::StreamExt;
use mongodb::{Collection, Database, bson::doc};
use serde_json::{json, to_value};

#[derive(serde::Deserialize, Clone)]
pub struct UserPost {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[post("/users")]
pub async fn post_user(db: web::Data<Database>, body: web::Json<UserPost>) -> HttpResponse {
    let user_collection: Collection<mongodb::bson::Document> = db.collection("users");

    // Create a new user document
    // First, hash password
    // TODO: Permissions?
    let hashed_password =
        bcrypt::hash(&body.password, bcrypt::DEFAULT_COST).expect("failed to hash password");
    let user_bson = doc! {
        "username": &body.username,
        "email": &body.email,
        "password": hashed_password,
    };

    // Save new user to database
    match user_collection.insert_one(user_bson).await {
        Ok(_) => HttpResponse::Ok().body("successfully created new user"),
        // Catch unique index constraint error
        Err(e) if e.to_string().contains("E11000 duplicate key error") => HttpResponse::Conflict()
            .body(format!(
                "user with username '{}' already exists",
                body.username
            )),
        // Catch other errors
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to insert user into database. error: {}", e)),
    }
}

#[derive(serde::Serialize)]
struct User {
    username: String,
    email: String,
    id: String,
}

#[get("/users")]
pub async fn get_users(db: web::Data<Database>) -> HttpResponse {
    let user_collection: Collection<mongodb::bson::Document> = db.collection("users");
    let users = user_collection.find(doc! {}).await;

    match users {
        Ok(mut cursor) => {
            let mut user_list = Vec::new();
            while let Some(user) = cursor.next().await {
                match user {
                    Ok(doc) => {
                        // extract only what we want from the user document
                        let user = User {
                            username: doc
                                .get_str("username")
                                .map(|val| val.to_string())
                                .unwrap_or_default(),
                            email: doc
                                .get_str("email")
                                .map(|val| val.to_string())
                                .unwrap_or_default(),
                            id: doc
                                .get_object_id("_id")
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                        };
                        // convert to JSON value
                        user_list.push(to_value(user).unwrap());
                    }
                    Err(e) => {
                        return HttpResponse::InternalServerError()
                            .body(format!("error reading user: {}", e));
                    }
                }
            }
            response::ok("success", serde_json::Value::Array(user_list))
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to query users: {}", e)),
    }
}

#[delete("/users/{username}")]
pub async fn delete_user(db: web::Data<Database>, path: web::Path<String>) -> HttpResponse {
    // TODO: Ensure the caller is authorized to delete this user
    let username = path.into_inner();
    let user_collection: Collection<mongodb::bson::Document> = db.collection("users");

    match user_collection
        .delete_one(doc! { "username": &username })
        .await
    {
        Ok(delete_result) => {
            if delete_result.deleted_count > 0 {
                HttpResponse::Ok().json(json!({
                    "status": "success",
                    "message": format!("user '{}' deleted successfully", username)
                }))
            } else {
                HttpResponse::NotFound().body("user not found")
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to delete user: {}", e)),
    }
}
