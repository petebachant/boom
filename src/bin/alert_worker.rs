use boom::conf;
use boom::types;
use boom::spatial;

use redis::AsyncCommands;

use apache_avro::Reader;
use apache_avro::from_value;

use mongodb::bson::doc;

fn avro_to_alert(avro_bytes: Vec<u8>) -> Result<types::Alert, Box<dyn std::error::Error>> {
    let reader = match Reader::new(&avro_bytes[..]) {
        Ok(reader) => reader,
        Err(e) => {
            println!("Error creating avro reader: {}", e);
            return Err(Box::new(e));
        }
    };
    
    let value = reader.map(|x| x.unwrap()).next().unwrap();
    let alert: types::Alert = from_value(&value).unwrap();
    Ok(alert)
}

// returns the candid that was successfully processed
async fn process_alert(
    avro_bytes: Vec<u8>,
    xmatch_configs: &Vec<types::CatalogXmatchConfig>,
    db: &mongodb::Database
) -> Result<Option<i64>, Box<dyn std::error::Error>> {

    // decode the alert
    let alert = match avro_to_alert(avro_bytes) {
        Ok(alert) => alert,
        Err(e) => {
            println!("Error reading alert packet: {}", e);
            return Ok(None);
        }
    };

    // check if the alert already exists in the alerts collection
    let collection_alert = db.collection("alerts");
    if !collection_alert.find_one(doc! { "candid": &alert.candid }).await.unwrap().is_none() {
        // we return early if there is already an alert with the same candid
        println!("alert with candid {} already exists", &alert.candid);
        return Ok(None);
    }

    // get the object_id, candid, ra, dec from the alert
    let object_id = alert.object_id.clone();
    let candid = alert.candid;
    let ra = alert.candidate.ra;
    let dec = alert.candidate.dec;

    // separate the alert and its history components
    let (
        alert_no_history,
        prv_candidates,
        fp_hist
    ) = alert.pop_history();
    
    // insert the alert into the alerts collection
    let alert_doc = alert_no_history.mongify();
    collection_alert.insert_one(alert_doc).await.unwrap();

    // - new objects - new entry with prv_candidates, fp_hists, xmatches
    // - existing objects - update prv_candidates, fp_hists
    let collection_alert_aux: mongodb::Collection<mongodb::bson::Document> = db.collection("alerts_aux");

    let prv_candidates_doc = prv_candidates.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();
    let fp_hist_doc = fp_hist.unwrap_or(vec![]).into_iter().map(|x| x.mongify()).collect::<Vec<_>>();

    if collection_alert_aux.find_one(doc! { "_id": &object_id }).await.unwrap().is_none() {
        let mut doc = doc! {
            "_id": &object_id,
            "prv_candidates": prv_candidates_doc,
            "fp_hists": fp_hist_doc,
        };
        doc.insert("cross_matches", spatial::xmatch(ra, dec, xmatch_configs, &db).await);

        collection_alert_aux.insert_one(doc).await.unwrap();
    } else {
        let update_doc = doc! {
            "$addToSet": {
                "prv_candidates": { "$each": prv_candidates_doc },
                "fp_hists": { "$each": fp_hist_doc }
            }
        };

        collection_alert_aux.update_one(doc! { "_id": &object_id }, update_doc).await.unwrap();
    }

    Ok(Some(candid))
}

#[tokio::main]
async fn main() {
    let config_file = conf::load_config("./config.yaml").unwrap();    

    let xmatch_configs = conf::build_xmatch_configs(config_file);

    let client_mongo = mongodb::Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
    let db = client_mongo.database("zvar");

    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();

    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush("myqueue", "myqueuetemp").await.unwrap();
        match result {
            Some(value) => {
                let candid = process_alert(value[0].clone(), &xmatch_configs, &db).await;
                match candid {
                    Ok(Some(candid)) => {
                        println!("Processed alert with candid: {}, queueing for classification", candid);
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>("myclassifier", candid).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                    }
                    Err(e) => {
                        println!("Error processing alert: {}", e);
                    }
                }
                con.lrem::<&str, Vec<u8>, isize>("myqueuetemp", 1, value[0].clone()).await.unwrap();

                if count > 1 && count % 100 == 0 {
                    let elapsed = start.elapsed().as_secs();
                    println!("\nProcessed {} alerts in {} seconds, avg: {:.4} alerts/s\n", count, elapsed, elapsed as f64 / count as f64);
                }
                count += 1;
            }
            None => {
                println!("Queue is empty");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

}