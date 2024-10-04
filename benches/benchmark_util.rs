use boom::conf;
use boom::alert;
use boom::types::ztf_alert_schema;
use redis::AsyncCommands;


// puts candids of processed alerts into a redis queue queue_name
pub async fn setup_benchmark(queue_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // drop alert and alert_aux collections in database
    drop_alert_collections().await?;
    // get alert files and process alerts and send candids into queue of choice
    fake_kafka_consumer("20240617").await?;
    println!("processing alerts...");
    alert_worker(queue_name).await;
    println!("candids successfully placed into redis queue '{}'", queue_name);
    Ok(())
}

pub async fn drop_alert_collections() -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    db.collection::<mongodb::bson::Document>("alerts").drop().await?;
    db.collection::<mongodb::bson::Document>("alerts_aux").drop().await?;
    println!("dropped collections: 'alerts' & 'alerts_aux'");
    Ok(())
}

async fn alert_worker(output_queue_name: &str) {

    let config_file = conf::load_config("./config.yaml").unwrap();

    let xmatch_configs = conf::build_xmatch_configs(&config_file, "ZTF");
    let db: mongodb::Database = conf::build_db(&config_file).await;
    let alert_collection = db.collection::<mongodb::bson::Document>("ZTF_alerts");
    let alert_aux_collection = db.collection::<mongodb::bson::Document>("ZTF_alerts_aux");

    if let Err(e) = db.list_collection_names().await {
        println!("Error connecting to the database: {}", e);
        return;
    }

    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();

    let schema = ztf_alert_schema().unwrap();

    loop {
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush("benchalertpacketqueue", "benchalertpacketqueuetemp").await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(
                    value[0].clone(),
                    &xmatch_configs,
                    &db,
                    &alert_collection,
                    &alert_aux_collection,
                    &schema
                ).await;
                match candid {
                    Ok(Some(candid)) => {
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>(output_queue_name, candid).await.unwrap();
                        con.lrem::<&str, Vec<u8>, isize>("benchalertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>("benchalertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                    }
                    Err(_) => {
                        // put it back in the alertpacketqueue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>("benchalertpacketqueuetemp", 1, value[0].clone()).await.unwrap();
                        con.lpush::<&str, Vec<u8>, isize>("benchalertpacketqueue", value[0].clone()).await.unwrap();
                    }
                }
            }
            None => {
                println!("Benchmark Alert Worker: found 0 alerts in queue, proceeding...");
                return;
            }
        }
    }
}

fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // given a date in the format YYYYMMDD, download public ZTF alerts from the archive
    // in this method we just validate the date format,
    // then use wget to download the alerts from the archive
    // and finally we extract the alerts to a folder

    // validate the date format
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    // create the data folder if it doesn't exist, in data/alerts/ztf/<date>
    let data_folder = format!("data/alerts/ztf/{}", date);

    // if it already exists and has the alerts, we don't need to download them again
    if std::path::Path::new(&data_folder).exists() && std::fs::read_dir(&data_folder)?.count() > 0 {
        println!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder)?.count();
        return Ok(count as i64);
    }

    std::fs::create_dir_all(&data_folder)?;

    println!("Downloading alerts for date {}", date);
    // download the alerts to data folder
    let url = format!("https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz", date);
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to download alerts".into());
    } else {
        println!("Downloaded alerts to {}", data_folder);
    }

    // extract the alerts
    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        println!("Extracted alerts to {}", data_folder);
    }

    // remove the tar.gz file
    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    // count the number of alerts
    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
}

async fn fake_kafka_consumer(alert_date: &str) -> Result<(), Box<dyn std::error::Error>> {
    
    let date = alert_date;
    let total_nb_alerts = download_alerts_from_archive(date)?;

    let client = redis::Client::open(
        "redis://localhost:6379".to_string()
    )?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>("benchalertpacketqueue").await.unwrap();

    let mut total = 0;

    println!("Pushing {} alerts to the queue", total_nb_alerts);
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry?;
        let path = entry.path();
        let payload = std::fs::read(path)?;

        con.rpush::<&str, Vec<u8>, usize>("benchalertpacketqueue", payload.to_vec()).await.unwrap();
        total += 1;
        if total % 1000 == 0 {
            println!("Pushed {} items since {:?}", total, start.elapsed());
        }
    }

    Ok(())
}