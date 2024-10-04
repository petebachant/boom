use redis::AsyncCommands;
use redis::streams::{StreamReadOptions,StreamReadReply};
use std::borrow::Borrow;
use std::{
    env, 
    error::Error,
    sync::{Arc, Mutex},
    collections::HashMap,
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

// intercepts interrupt signals and sets boolean to true
async fn sig_int_handler(v: Arc<Mutex<bool>>) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Filter worker interrupted. Finishing up...");
        let mut v = v.try_lock().unwrap();
        *v = true;
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let catalog = "ZTF";
    let args: Vec<String> = env::args().collect();
    // let mut filter_id = 1;
    // get all the filter_ids
    let mut filter_ids: Vec<i32> = Vec::new();
    for i in 1..args.len() {
        let filter_id = args[i].parse::<i32>().unwrap();
        if !filter_ids.contains(&filter_id) {
            filter_ids.push(filter_id);   
        }
    }
    println!("Starting filter worker for {} with filters {:?}", catalog, filter_ids);

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    sig_int_handler(Arc::clone(&interrupt)).await;

    // connect to mongo and redis
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();
    
    // build filters and organize by permission level
    let mut filter_table: HashMap<i64, Vec<filter::Filter>> = HashMap::new();
    for id in filter_ids.clone() {
        let filter = filter::Filter::build(id, &db).await;
        match filter {
            Ok(filter) => {
                let perms = filter.permissions.iter().max().unwrap();
                if !filter_table.contains_key(perms) {
                    filter_table.insert(perms.clone(), Vec::new());
                }
                filter_table.entry(perms.clone()).and_modify(|filters| filters.push(filter));
            },
            Err(e) => {
                println!("got error when trying to build filter {}: {}", id, e);
                return Err(e);
            }
        }
    }

    // initialize redis streams based on permission levels
    let mut redis_streams = HashMap::new();
    for filter_vec in filter_table.values() {
        for filter in filter_vec {
            let perms = filter.permissions.iter().max().unwrap();
            let stream = format!("{stream}_programid_{programid}_filter_stream",
                stream = filter.catalog, programid = perms);
            if !redis_streams.contains_key(perms) {
                redis_streams.insert(perms.clone(), stream);
            }
        }
    }

    // this uses consumer groups to more easily "resume" reading from where we might have stopped
    // as well as to allow multiple workers to read from the same stream for a given filter,
    // which let's us scale the number of workers per filter if needed in the future
    // https://medium.com/redis-with-raphael-de-lio/understanding-redis-streams-33aa96ca7206

    // create consumer groups, read_options, and output queues for each filter
    let mut read_options = HashMap::new();
    let mut filter_results_queues: HashMap<i32, String> = HashMap::new();
    for filter_vec in filter_table.values() {
        for filter in filter_vec {
            let consumer_group = format!("filter_{filter_id}_group", filter_id = filter.id);
            let consumer_group_res: Result<(), redis::RedisError> = con.xgroup_create(
                &redis_streams[filter.permissions.iter().max().unwrap()], 
                &consumer_group, "0").await;
            match consumer_group_res {
                Ok(()) => {
                    println!("Created consumer group for filter {}", filter.id);
                },
                Err(e) => {
                    println!("Consumer group already exists for filter {}: {:?}", filter.id, e);
                }
            }
            let opts = StreamReadOptions::default()
                .group(consumer_group, "worker_1")
                .count(100);
            read_options.insert(filter.id, opts);
            filter_results_queues.insert(
                filter.id, 
                format!("filter_{filter_id}_results", filter_id = filter.id));
        }
    }

    // keep track of how many streams are empty in order to take breaks
    let mut empty_stream_counter: usize = 0;

    loop {
        // check if worker has been interrupted
        match interrupt.try_lock() {
            Ok(x) => {
                if *x { return Ok(()); }
            },
            _ => {}
        };

        for (perm, filters) in &mut filter_table {
            for filter in filters {
                let candids = get_candids_from_stream(
                    &mut con,
                    &redis_streams[&perm],
                    &read_options[&filter.id]).await;
                if candids.len() == 0 {
                    empty_stream_counter += 1;
                    continue;
                }
                let in_count = candids.len();

                println!("got {} candids from redis stream for filter {}", in_count, redis_streams[&perm]);
                println!("running filter with id {} on {} alerts", filter.id, in_count);

                let start = std::time::Instant::now();

                let out_documents = filter.run(candids, &db).await.unwrap();
                if out_documents.len() == 0 {
                    continue;
                }
                // convert the documents to a format that any other worker (even in python) can read
                // for that we can deserialize the Document to json
                let out_documents: Vec<String> = out_documents.iter().map(|doc| {
                    let json = serde_json::to_string(doc).unwrap();
                    json
                }).collect();
                con.lpush::<&str, &Vec<String>, isize>(
                    &filter_results_queues[&filter.id],
                    &out_documents).await.unwrap();

                println!(
                    "{}/{} alerts passed filter {} in {}s", 
                    out_documents.len(), in_count, filter.id, start.elapsed().as_secs_f64());
            }
        }
        if empty_stream_counter == filter_ids.len() {
            println!("All streams empty");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        }
        empty_stream_counter = 0;
    }
}
