use redis::AsyncCommands;

use boom::conf;
use boom::alert;

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
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush("alertpacketqueue", "alertpacketqueuetemp").await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(value[0].clone(), &xmatch_configs, &db).await;
                match candid {
                    Ok(Some(candid)) => {
                        println!("Processed alert with candid: {}, queueing for classification", candid);
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>("alertclassifierqueue", candid).await.unwrap();
                        con.lrem::<&str, Vec<u8>, isize>("alertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                    }
                    Err(e) => {
                        println!("Error processing alert: {}, requeueing", e);
                        // put it back in the queue, to the left (pop from the right, push to the left)
                        con.rpoplpush::<&str, Vec<u8>, isize>("alertpacketqueuetemp", value[0].clone()).await.unwrap();
                    }
                }

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