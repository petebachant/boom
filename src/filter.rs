use std::error::Error;
use mongodb::{bson::doc, bson::Document};
use futures::stream::StreamExt;

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


pub async fn run_filter(
    candids: Vec<i64>, filter_id: i32, db: &mongodb::Database,
) -> Result<Vec<i64>, Box<dyn Error>> {
    // build filter
    let filter = build_filter(candids, filter_id, db).await?;

    // run filter
    let mut result = db.collection::<Document>(
        "alerts"
    ).aggregate(filter).await?;
    
    let mut out_candids: Vec<i64> = Vec::new();

    // grab output candids
    while let Some(doc) = result.next().await {
        let doc = doc.unwrap();
        // out_candids.push(doc.get_i64("candid").unwrap()); // faster but less safe
        // slower but safer
        let res = doc.get_i64("candid");
        // check if candid field is present
        match res {
            Ok(x) => {
                out_candids.push(x);
            },
            Err(e) => {
                println!("got error {}", e);
                return Result::Err(Box::new(e));
            }
        }
    }
    Ok(out_candids)
}