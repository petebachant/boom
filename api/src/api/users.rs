use actix_web::{HttpResponse, post, web};
use mongodb::{Collection, Database, bson::doc};

#[derive(serde::Deserialize, Clone)]
pub struct UserPost {
    pub username: String,
    pub email: String,
    pub password: String,
}

#[post("/users")]
pub async fn post_user(db: web::Data<Database>, body: web::Json<UserPost>) -> HttpResponse {
    let body = body.clone();

    // check that a user with this username/email does not already exist
    // TODO: drop usernames altogether and simply use email?
    let user_collection: Collection<mongodb::bson::Document> = db.collection("users");
    let existing_user = user_collection
        .find_one(doc! { "username": &body.username })
        .await;
    match existing_user {
        Ok(Some(_)) => {
            return HttpResponse::BadRequest().body("user with this username already exists");
        }
        Ok(None) => {}
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to query database for existing user. error: {}",
                e
            ));
        }
    }
    let existing_user = user_collection
        .find_one(doc! { "email": &body.email })
        .await;
    match existing_user {
        Ok(Some(_)) => {
            return HttpResponse::BadRequest().body("user with this email already exists");
        }
        Ok(None) => {}
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to query database for existing user. error: {}",
                e
            ));
        }
    }

    // create a new user document
    // first, hash password
    // TODO: permissions?
    let hashed_password =
        bcrypt::hash(&body.password, bcrypt::DEFAULT_COST).expect("failed to hash password");
    let user_bson = doc! {
        "username": &body.username,
        "email": &body.email,
        "password": hashed_password,
    };

    // save new user to database
    match user_collection.insert_one(user_bson).await {
        Ok(_) => {
            return HttpResponse::Ok().body("successfully created new user");
        }
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("failed to insert user into database. error: {}", e));
        }
    }
}
