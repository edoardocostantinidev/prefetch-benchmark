use core::panic;
use std::{
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amiquip::{
    AmqpValue, Connection, ConsumerMessage, ConsumerOptions, Exchange, FieldTable, Publish,
    QueueDeclareOptions,
};
use rand::{distributions::Alphanumeric, Rng};

fn main() -> Result<(), String> {
    let role = std::env::var("ROLE").expect("you must set the ROLE variable");
    let connection_string =
        std::env::var("CONNECTION_STRING").expect("you must set the CONNECTION_STRING variable");

    let mut connection: Connection;
    loop {
        let conn_result = Connection::insecure_open(&connection_string);
        match conn_result {
            Ok(conn) => {
                connection = conn;
                break;
            }
            Err(_) => sleep(Duration::from_millis(250)),
        }
    }
    match role.as_str() {
        "PRODUCER" => start_producing(&mut connection),
        "CONSUMER" => start_consuming(&mut connection),
        _ => {
            panic!("ROLE variable must be either PRODUCER or CONSUMER");
        }
    }
    .map_err(|e| format!("Exiting... {:?}", e))?;

    connection
        .close()
        .map_err(|e| format!("could not close connection {:?}", e))?;

    println!("Connection closed, inspect network traffic now");
    sleep(Duration::from_secs(15));
    Ok(())
}

fn start_consuming(conn: &mut Connection) -> Result<(), amiquip::Error> {
    println!("starting consumer");
    let channel = conn.open_channel(None)?;
    let queue = channel.queue_declare(
        "prefetch-test",
        QueueDeclareOptions {
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::default(),
        },
    )?;

    let prefetch_count = std::env::var("PREFETCH_COUNT")
        .expect("you must set a prefetch count for a consumer")
        .parse::<u16>()
        .expect("PREFETCH_COUNT must be a valid unsigned integer");
    channel.qos(0, prefetch_count, false)?;

    let consumer_name = std::env::var("NAME").expect("you must set a name for the consumer");

    let workload_time = std::env::var("WORKLOAD_TIME")
        .expect("you must set a workload time for a consumer")
        .parse::<u64>()
        .expect("WORKLOAD_TIME must be a valid unsigned integer");
    sleep(Duration::from_secs(5));
    let consumer = queue.consume(ConsumerOptions::default())?;
    let ts = std::time::Instant::now();
    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                match body.as_ref() {
                    "start" => {
                        println!("start,{},{},{}", consumer_name, i, ts.elapsed().as_millis());
                    }
                    "done" => {
                        println!(
                            "end,{},{},{}",
                            consumer_name,
                            i - 1,
                            ts.elapsed().as_millis()
                        );
                        break;
                    }
                    _ => {
                        sleep(Duration::from_millis(workload_time));
                    }
                }
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }
    println!("DONE!");
    Ok(())
}

fn start_producing(conn: &mut Connection) -> Result<(), amiquip::Error> {
    println!("starting producer");
    let channel = conn.open_channel(None)?;
    let exchange = Exchange::direct(&channel);

    let message_len = std::env::var("MESSAGE_LEN")
        .expect("you must set a message lenght for a producer")
        .parse::<usize>()
        .expect("MESSAGE_LEN must be a valid unsigned integer");
    let time_between_sends = std::env::var("TIME_BETWEEN_MSG")
        .expect("you must set a time in between messages for a producer")
        .parse::<u64>()
        .expect("TIME_BETWEEN_MSG must be a valid unsigned integer");
    let messages_to_send = std::env::var("MESSAGE_COUNT")
        .expect("you must set a message count for a producer")
        .parse::<u64>()
        .expect("MESSAGE_COUNT must be a valid unsigned integer");
    let mut messages_sent = 1;
    let message = get_random_string(message_len);
    sleep(Duration::from_secs(3));

    exchange.publish(Publish::new("start".as_bytes(), "prefetch-test"))?;

    while messages_to_send >= messages_sent {
        exchange.publish(Publish::new(message.as_bytes(), "prefetch-test"))?;
        messages_sent += 1;
        sleep(Duration::from_millis(time_between_sends));
    }

    exchange.publish(Publish::new("done".as_bytes(), "prefetch-test"))?;
    println!("DONE!");
    loop {
        sleep(Duration::from_millis(1000));
    }
}

/// Generates a random string with a given lenght
fn get_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
