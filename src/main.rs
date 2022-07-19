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
    println!("I'm going to wait 30 seconds to let rabbitmq start");
    sleep(Duration::from_millis(30000));
    let mut connection = Connection::insecure_open(&connection_string)
        .map_err(|e| format!("could not connect to rabbit {:?}", e))?;

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

    Ok(())
}

fn start_consuming(conn: &mut Connection) -> Result<(), amiquip::Error> {
    println!("starting consumer");
    let channel = conn.open_channel(None)?;
    let queue = channel.queue_declare("prefetch-test", QueueDeclareOptions::default())?;

    let prefetch_count = std::env::var("PREFETCH_COUNT")
        .expect("you must set a prefetch count for a consumer")
        .parse::<u32>()
        .expect("PREFETCH_COUNT must be a valid unsigned integer");

    let consumer_name = std::env::var("NAME").expect("you must set a name for the consumer");

    let workload_time = std::env::var("WORKLOAD_TIME")
        .expect("you must set a workload time for a consumer")
        .parse::<u64>()
        .expect("WORKLOAD_TIME must be a valid unsigned integer");

    let mut arguments = FieldTable::new();
    arguments.insert(
        "prefetch-count".to_string(),
        AmqpValue::LongUInt(prefetch_count),
    );
    let consumer = queue.consume(ConsumerOptions {
        arguments: arguments,
        ..Default::default()
    })?;
    println!("Waiting for messages. Press Ctrl-C to exit.");

    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("impossible to get here")
                    .as_millis();
                let body = String::from_utf8_lossy(&delivery.body);
                println!("{},{},{},{}", consumer_name, i, timestamp, body.len());
                //simulate a workload
                sleep(Duration::from_millis(workload_time));
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }
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
    let mut messages_sent = 0;
    loop {
        let random_string = get_random_string(message_len);
        exchange
            .publish(Publish::new(random_string.as_bytes(), "prefetch-test"))
            .expect("error publishing");
        sleep(Duration::from_millis(time_between_sends));
        messages_sent += 1;
        if messages_sent == messages_to_send {
            break;
        }
    }
    Ok(())
}

/// Generates a random string with a given lenght
fn get_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
