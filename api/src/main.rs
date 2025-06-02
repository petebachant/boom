use boom_api::api;
use boom_api::conf::AppConfig;

use actix_web::{App, HttpResponse, HttpServer, get, middleware::Logger, web};
use mongodb::{Client, Database};

#[get("/")]
pub async fn get_health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Greetings from BOOM!"
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Read the config file
    let config = AppConfig::from_default_path().database;
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!(
            "mongodb://{}:{}@{}:{}",
            config.username, config.password, config.host, config.port
        )
        .into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    let database: Database = client.database(&config.name);

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .service(get_health)
            .service(api::query::get_info)
            .service(api::query::sample)
            .service(api::query::cone_search)
            .service(api::query::count_documents)
            .service(api::query::find)
            .service(api::alerts::get_object)
            .service(api::filters::post_filter)
            .service(api::filters::add_filter_version)
            .service(api::users::post_user)
            .wrap(Logger::default())
    })
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}
