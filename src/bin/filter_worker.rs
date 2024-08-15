use redis::AsyncCommands;
use mongodb::Client;
use mongodb::bson::doc;
use boom::conf;
use std::{
    error::Error,
    num::NonZero,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();
    // con.subscribe("filterafterml").await.unwrap(); // subscribe queue containing candids from ML workers

    let res = con.rpop::<&str, Vec<i64>>("filterafterml", NonZero::new(100)).await;
    match res {
        Err(e) => {
            println!("got error: {:?}", e);
        },
        Ok(value) => {
            println!("received {:?} candids", value.len());
            for i in 0..value.len() {
                println!("{:?}", value[i]);
            }
        },
        _ => {
            println!("something else happened...");
        },
    }
    
    /*
    let client = Client::with_uri_str("mongodb://mongoadmin:mongoadminsecret@localhost:27017/").await?;
    let result = client.database("boom").collection::<mongodb::bson::Document>("alerts").aggregate([
        doc! {
            "$match": doc! {
                "candid": doc! {
                    "$in": [
                        // list of candids goes here
                        "2695378930215015068".parse::<i64>()?,
                        "2695438883815015001".parse::<i64>()?
                    ]
                }
            }
        },
        doc! {
            "$project": doc! {
                "cutoutScience": 0,
                "cutoutDifference": 0,
                "cutoutTemplate": 0,
                "publisher": 0,
                "schemavsn": 0
            }
        },
        doc! {
            "$lookup": doc! {
                "from": "alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candid": 1,
                "candidate": 1,
                "classifications": 1,
                "coordinates": 1,
                "prv_candidates": doc! {
                    "$arrayElemAt": [
                        "$aux.prv_candidates",
                        0
                    ]
                },
                "cross_matches": doc! {
                    "$arrayElemAt": [
                        "$aux.cross_matches",
                        0
                    ]
                }
            }
        },
        doc! {
            "$match": doc! {
                "candidate.magpsf": doc! {
                    "$lte": 18.5
                }
            }
        }
    ]).await?;
    */

    Ok(())
}