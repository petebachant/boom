use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open(
        "redis://localhost:6379".to_string()
    )?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>("alertpacketqueue").await.unwrap();

    let mut total = 0;
    // generate a random group id every time
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &Uuid::new_v4().to_string())
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&["ztf_20240519_programid1"]).unwrap();

    println!("Reading from kafka topic and pushing to the queue");
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    loop {
        let message = consumer.poll(tokio::time::Duration::from_secs(30));
        match message {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap();
                con.rpush::<&str, Vec<u8>, usize>("alertpacketqueue", payload.to_vec()).await.unwrap();
                total += 1;
                if total % 1000 == 0 {
                    println!("Pushed {} items since {:?}", total, start.elapsed());
                }
            }
            Some(Err(err)) => {
                println!("Error: {:?}", err);
            }
            None => {
                println!("No message");
            }
        }
    }
}