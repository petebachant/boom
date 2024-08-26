use redis::AsyncCommands;
use mongodb::{
    bson::doc, bson::Document
};
use futures::stream::StreamExt;
use boom::conf;
use std::{
    error::Error, num::NonZero, env,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args: Vec<String> = env::args().collect();
    let mut filter_id = 1;
    if args.len() > 1 {
        filter_id = args[1].parse::<i32>().unwrap();
    }

    // connect to mongo and redis
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();

    loop {
        // retrieve candids from redis queue
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
                println!("received {:?} candids from redis", candids.len());
                println!("running demo filter {} on alerts", filter_id);

                let start = std::time::Instant::now();
                // build filter
                let filter = build_filter(candids, filter_id, &db).await?;

                // run filter
                let mut result = db.collection::<Document>(
                    "alerts"
                ).aggregate(filter).await?;
                
                // grab output candids
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


pub async fn build_filter(
    candids: Vec<i64>, filter_id: i32, db: &mongodb::Database,
) -> Result<Vec<mongodb::bson::Document>, Box<dyn Error>> {
    // filter prefix
    let mut out_filter = vec![
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
        }
    ];
    
    // grab filter from database
    // >> pipeline returns document with group_id, permissions, and filter pipeline
    let filter_obj = db.collection::<Document>("filters")
        .aggregate(vec![
        doc! {
            "$match": doc! {
                "filter_id": filter_id,
                "active": true,
                "catalog": "ZTF_alerts"
            }
        },
        doc! {
            "$project": doc! {
                "fv": doc! {
                    "$filter": doc! {
                        "input": "$fv",
                        "as": "x",
                        "cond": doc! {
                            "$eq": [
                                "$$x.fid",
                                "$active_fid"
                            ]
                        }
                    }
                },
                "group_id": 1,
                "permissions": 1
            }
        },
        doc! {
            "$project": doc! {
                "pipeline": doc! {
                    "$arrayElemAt": [
                        "$fv.pipeline",
                        0
                    ]
                },
                "group_id": 1,
                "permissions": 1
            }
        }]
    ).await;
    if let Err(e) = filter_obj {
        println!("Got ERROR when retrieving filter from database: {}", e);
        return Result::Err(Box::new(e));
    }

    // get document from cursor
    let filter_obj = filter_obj
        .unwrap().deserialize_current().unwrap();
    // get filter pipeline as str and convert to Vec<Bson>
    let filter = filter_obj.get("pipeline")
        .unwrap().as_str().unwrap();
    let filter = serde_json::from_str::<serde_json::Value>(filter).expect("Invalid input");
    let filter = filter.as_array().unwrap();

    // append stages to prefix
    for stage in filter {
        let x = mongodb::bson::to_document(stage).expect("invalid filter BSON");
        out_filter.push(x);
    }

    Ok(out_filter)
}
