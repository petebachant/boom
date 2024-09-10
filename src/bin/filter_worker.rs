use redis::AsyncCommands;
use redis::streams::{StreamReadOptions,StreamReadReply};
use std::{
    env, 
    error::Error, 
};

use boom::{conf, filter};


async fn get_candids_from_stream(con: &mut redis::aio::MultiplexedConnection, stream: &str, options: &StreamReadOptions) -> Vec<i64> {
    let result: Option<StreamReadReply> = con.xread_options(
        &[stream.to_owned()], &[">"], options).await.unwrap();
    let mut candids: Vec<i64> = Vec::new();
    if let Some(reply) = result {
        for stream_key in reply.keys {
            let xread_ids = stream_key.ids;
            for stream_id in xread_ids {
                let candid = stream_id.map.get("candid").unwrap();
                // candid is a Value type, so we need to convert it to i64
                match candid {
                    redis::Value::BulkString(x) => {
                        // then x is a Vec<u8> type, so we need to convert it an i64
                        let x = String::from_utf8(x.to_vec()).unwrap().parse::<i64>().unwrap();
                        // append to candids
                        candids.push(x);
                    },
                    _ => {
                        println!("Candid unknown type: {:?}", candid);
                    }
                }
            }
        }
    }
    candids
}

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
    
    // build filter
    let mut filter = filter::Filter::build(filter_id, &db).await?;

    println!("Starting filter worker for filter {}", filter_id);

    let redis_stream = format!("{stream}_programid_{programid}_filter_stream", 
        stream=filter.catalog, programid=filter.permissions.iter().max().unwrap());

    // this uses consumer groups to more easily "resume" reading from where we might have stopped
    // as well as to allow multiple workers to read from the same stream for a given filter,
    // which let's us scale the number of workers per filter if needed in the future
    // https://medium.com/redis-with-raphael-de-lio/understanding-redis-streams-33aa96ca7206

    // let consumer_group = format!("filter_{filter_id}_{rand}", filter_id=filter_id, rand=rand::random::<i32>());
    let consumer_group = format!("filter_{filter_id}_group", filter_id=filter_id);
    let consumer_group_res: Result<(), redis::RedisError> = con.xgroup_create(
        &redis_stream, &consumer_group, "0").await;

    let filter_results_queue = format!("filter_{filter_id}_results", filter_id=filter_id);

    match consumer_group_res {
        Ok(_) => {
            println!("Created consumer group for filter {}", filter_id);
        },
        Err(e) => {
            println!("Consumer group already exists for filter {}: {:?}", filter_id, e);
        }
    }

    let opts = StreamReadOptions::default()
    .group(&consumer_group, "worker_1")
    .count(100);

    loop {
        let candids = get_candids_from_stream(&mut con, &redis_stream, &opts).await;

        if candids.len() == 0 {
            println!("Queue empty");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            continue;
        }

        let in_count = candids.len();
        println!("running filter with id {} on {} alerts", filter_id, in_count);

        let start = std::time::Instant::now();

        let out_documents = filter.run(candids, &db).await.unwrap();

        // convert the documents to a format that any other worker (even in python) can read
        // for that we can deserialize the Document to json
        let out_documents: Vec<String> = out_documents.iter().map(|doc| {
            let json = serde_json::to_string(doc).unwrap();
            json
        }).collect();
        con.lpush::<&str, &Vec<String>, isize>(&filter_results_queue, &out_documents).await.unwrap();

        println!("{}/{} alerts passed filter {} in {}s", out_documents.len(), in_count, filter_id, start.elapsed().as_secs_f64());
    }
}
