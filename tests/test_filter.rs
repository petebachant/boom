use mongodb::bson::{doc, Document};
use tracing::{error, info};

use boom::{
    conf,
    filter,
    testing_util as tu,
    alert::{AlertWorker, ZtfAlertWorker},
};

const CONFIG_FILE: &str = "tests/config.test.yaml";

#[tokio::test]
async fn test_build_filter() {
    let config_file = conf::load_config(CONFIG_FILE).unwrap();
    let db = conf::build_db(&config_file).await;
    tu::insert_test_filter().await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": {} },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$project": {
                "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1,
                "cross_matches": { "$arrayElemAt": ["$aux.cross_matches", 0] },
                "prv_candidates": {
                    "$filter": {
                        "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$in": ["$$x.programid", [1_i64]] },
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]}
                            ]
                        }
                    }
                }
            }
        },
        doc! { "$match": { "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    assert_eq!(vec![1], filter.permissions);
    assert_eq!("ZTF_alerts", filter.catalog);
}

// checks result of running filter
#[tokio::test]
async fn test_run_filter() {
    tu::insert_test_filter().await;
    let config_file = conf::load_config(CONFIG_FILE).unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let mut filter = filter_result.unwrap();

    let test_col_name = "ZTF_alerts";
    let test_cutout_col_name = "ZTF_alerts_cutouts";
    let test_aux_col_name = "ZTF_alerts_aux";
    let _ = tu::drop_alert_collections(&test_col_name, &test_cutout_col_name, &test_aux_col_name).await;

    let mut alert_worker = ZtfAlertWorker::new(CONFIG_FILE).await.unwrap();
    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "Error processing alert: {:?}", result);

    let candids = vec![result.unwrap()];

    info!("received {} candids from redis", candids.len());
    let out_candids = filter.run(candids.clone(), &db).await;
    match out_candids {
        Ok(out) => {
            assert_eq!(out.len(), 1);
        }
        Err(e) => {
            panic!("Got error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_filter_no_alerts() {
    tu::insert_test_filter().await;
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let mut filter = filter_result.unwrap();
    
    let candids: Vec<i64> = vec![1];

    let out_candids = filter.run(candids.clone(), &db).await;
    match out_candids {
        Err(e) => {
            error!("Error running filter: {}", e);
        }
        _ => {}
    }
}

#[tokio::test]
async fn test_no_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter = filter::Filter::build(-2, &db).await;
    match filter {
        Err(e) => {
            assert!(e.is::<filter::FilterError>());
            error!("error: {}", e);
        }
        _ => {
            panic!("Was supposed to get error");
        }
    }
}

#[tokio::test]
async fn test_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    tu::insert_test_filter().await;
    let filter = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    match filter {
        Ok(_) => {
            info!("successfully got filter from db");
        }
        Err(e) => {
            panic!("ERROR, was supposed to find filter in database. {}", e);
        }
    }
}
