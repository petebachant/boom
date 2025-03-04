use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use boom::kafka::{
    download_alerts_from_archive,
    produce_from_archive
};

#[tokio::test]
async fn test_download_from_archive() {
    let date = "20240617";
    // if the data folder already exists, and the nb of files isn't 710, delete it
    let data_folder = format!("data/alerts/ztf/{}", date);
    if std::path::Path::new(&data_folder).exists() {
        let count = std::fs::read_dir(&data_folder).unwrap().count();
        if count != 710 {
            std::fs::remove_dir_all(&data_folder).unwrap();
        }
    }
    match download_alerts_from_archive(date) {
        Ok(count) => {
            assert_eq!(count, 710);
        }
        Err(e) => {
            assert!(false, "Error downloading alerts: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_produce_from_archive() {
    let topic = uuid::Uuid::new_v4().to_string();

    let result = produce_from_archive("20240617", 0, Some(topic.clone())).await;
    assert!(result.is_ok());

    let consumer: BaseConsumer = match ClientConfig::new()
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            assert!(false, "Error creating consumer: {:?}", e);
            return;
        }
    };

    match consumer.subscribe(&[&topic]) {
        Ok(_) => {}
        Err(e) => {
            assert!(false, "Error subscribing to topic: {:?}", e);
            return;
        }
    }

    let metadata = match consumer.fetch_metadata(Some(&topic), std::time::Duration::from_secs(1)) {
        Ok(m) => m,
        Err(e) => {
            assert!(false, "Error fetching metadata: {:?}", e);
            return;
        }
    };

    let mut found_topic = false;

    for metadata_topic in metadata.topics().iter() {
        if metadata_topic.name() == topic {
            found_topic = true;
            assert_eq!(metadata_topic.partitions().len(), 1);
            match consumer.fetch_watermarks(
                metadata_topic.name(),
                metadata_topic.partitions()[0].id(),
                std::time::Duration::from_secs(1),
            ) {
                Ok((low, high)) => {
                    assert_eq!(high - low, 710);
                }
                Err(e) => {
                    assert!(false, "Error fetching watermarks: {:?}", e);
                    return;
                }
            }
        }
    }

    assert!(found_topic);
}
