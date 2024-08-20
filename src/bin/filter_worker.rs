use redis::AsyncCommands;
use mongodb::{
    bson::{doc, Document}, Client
};
use futures::stream::StreamExt;
use boom::conf;
use std::{
    error::Error,
    num::NonZero,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // temp demo_filter
    let demo_filter = vec! [
        doc! {
            "$match": doc! {
                "candidate.magpsf": doc! {
                    "$lte": 18.5
                }
            }
        }];

    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();

    loop {
        // grab candids from redis queue
        let res: Result<Vec<i64>, redis::RedisError> = 
            con.rpop::<&str, Vec<i64>>("filterafterml", NonZero::new(1000)).await;

        match res {
            Ok(candids) => {
                if candids.len() == 0 {
                    println!("Queue empty");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }
                let in_count = candids.len();
                let start = std::time::Instant::now();
                println!("received {:?} candids from redis", candids.len());
                println!("running demo filter...");
                
                // build filter
                let filter = build_filter(candids, demo_filter.clone()).unwrap();

                // run filter
                let mut result = db.collection::<mongodb::bson::Document>(
                    "alerts"
                ).aggregate(filter).await?;
                
                // grab output candids (I am sure there is a faster way to do this...)
                let mut out_candids: Vec<i64> = Vec::new();
                let mut out_count = 0;
                while let Some(doc) = result.next().await {
                    let doc = doc.unwrap();
                    out_candids.push(doc.get_i64("candid").unwrap());
                    out_count += 1;
                }
                // send resulting candids to redis
                con.lpush::<&str, Vec<i64>, isize>("afterfilter", out_candids).await?;
                println!("Filtered {} alerts in {} seconds, output {} candids to redis", 
                    in_count, 
                    (std::time::Instant::now() - start).as_secs_f64(),
                    out_count
                );
            },
            Err(e) => {
                println!("Got error: {:?}", e);
            },
        }
    }
}


// builds a mongodb aggregate pipeline by adding the database_filter on top of the default prefix
pub fn build_filter(
    candids: Vec<i64>, user_filter: Vec<mongodb::bson::Document>
) -> Result<Vec<mongodb::bson::Document>, Box<dyn Error>> {
    let mut filter = vec![
        doc! {
        "$match": doc! {
            "candid": doc! {
                "$in": candids
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
    }];
    for stage in user_filter {
        filter.push(stage);
    }
    Ok(filter)
}
