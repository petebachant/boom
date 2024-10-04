use redis::AsyncCommands;
use std::{
    env,
    num::NonZero,
    error::Error,
};
use boom::{
    filter,
    conf,
};
mod benchmark_util;
use crate::benchmark_util as util;

// run: cargo bench filter_benchmark -- <filter_id> <num_iterations_on_candids>
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let queue_name = "benchmarkqueue";
    util::setup_benchmark(&queue_name).await?;

    // grab command line arguments
    let args: Vec<String> = env::args().collect();
    let mut filter_id = 1;
    let mut n = 20;
    if args.len() > 2 {
        filter_id = args[2].parse::<i32>().unwrap();
        n = args[3].parse::<i32>().unwrap();
    }

    // connect to mongo and redis
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();
    
    println!("running filter benchmark...");

    let mut runs: Vec<(i32, usize, f64)> = Vec::new();

    let mut test_filter = filter::Filter::build(filter_id, &db).await?;

    // run benchmark 5 times
    for i in 0..n {

        let start = std::time::Instant::now();
        // retrieve candids from redis queue
        let res: Result<Vec<i64>, redis::RedisError> = con.rpop::<&str, Vec<i64>>(
                &queue_name, NonZero::new(1000)).await;

        match res {
            Ok(candids) => {
                if candids.len() == 0 {
                    println!("Queue empty");
                    return Ok(());
                }
                
                let _out_candids = test_filter.run(candids.clone(), &db).await?;

                let total_time = (std::time::Instant::now() - start).as_secs_f64();
                runs.push((i, candids.len(), total_time));

                // push all candids back onto the redis queue
                con.lpush::<&str, Vec<i64>, isize>(
                    &queue_name, candids.clone()
                ).await?;
            },
            Err(e) => {
                println!("Got error: {:?}", e);
            },
        }
    }
    // println!("=========================\n   FULL OUTPUT\n=========================");
    // for run in runs.clone() {
    //     println!("run {} filtered {} candids in {} seconds", run.0, run.1, run.2);
    // }

    let mut total_alerts = 0;
    let mut total_time = 0.0;
    let mut min_time: (i32, f64) = (-1, 99999.0);
    let mut max_time: (i32, f64) = (-1, 0.0);
    for i in runs.clone() {
        total_alerts += i.1;
        total_time += i.2;
        if i.2 < min_time.1 {
            min_time = (i.0, i.2);
        }
        if i.2 > max_time.1 {
            max_time = (i.0, i.2);
        }
    }
    println!("=========================\n   SUMMARY\n");
    let total_alerts = total_alerts as f64;
    let average = total_alerts / total_time;
    println!("   average speed: {} alerts filtered / sec", average);
    println!("   fastest run: {} @ {}\n   slowest run: {} @ {}", min_time.0, min_time.1, max_time.0, max_time.1);
    println!("=========================");
    Ok(())
}
