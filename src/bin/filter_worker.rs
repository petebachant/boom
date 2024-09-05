use redis::AsyncCommands;
use std::{
    env, 
    error::Error, 
    num::NonZero
};
use boom::{conf, filter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // grab command line arguments
    // args: <filter_id>
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
    
    // build filter
    let mut filter = filter::Filter::build(filter_id, &db).await?;
    
    loop {
        // retrieve candids from redis queue
        let res: Result<Vec<i64>, redis::RedisError> = con.rpop::<&str, Vec<i64>>(
                "filterafterml", NonZero::new(1000)).await;

        match res {
            Ok(candids) => {
                if candids.len() == 0 {
                    println!("Queue empty");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }
                let in_count = candids.len();
                println!("received {:?} candids from redis", candids.len());
                println!("running filter with id {} on alerts", filter_id);

                let start = std::time::Instant::now();

                let out_candids = filter.run(candids, &db).await?;

                // send resulting candids to redis
                con.lpush::<&str, Vec<i64>, isize>(
                    "afterfilter", out_candids.clone()
                ).await?;

                println!("Filtered {} alerts in {} seconds, output {} candids to redis", 
                    in_count, 
                    (std::time::Instant::now() - start).as_secs_f64(),
                    out_candids.len()
                );
            },
            Err(e) => {
                println!("Got error: {:?}", e);
            },
        }
    }
}
