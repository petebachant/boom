use std::sync::{Mutex, Arc};
use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};

// spawns a thread which listens for interrupt signal. Sets flag to true upon signal interruption
pub async fn sig_int_handler(flag: Arc<Mutex<bool>>) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Received interrupt signal. Finishing up...");
        let mut flag = flag.try_lock().unwrap();
        *flag = true;
    });
}

// checks if a flag is set to true and, if so, exits the program
pub fn check_exit(flag: Arc<Mutex<bool>>) {
    match flag.try_lock() {
        Ok(x) => {
            if *x { std::process::exit(0) }
        },
        _ => {}
    }
}

pub async fn get_candids_from_stream(con: &mut redis::aio::MultiplexedConnection, stream: &str, options: &StreamReadOptions) -> Vec<i64> {
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