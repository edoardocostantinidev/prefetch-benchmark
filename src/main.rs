use core::panic;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use amiquip::{
    AmqpValue, Connection, ConsumerMessage, ConsumerOptions, Exchange, FieldTable, Publish,
    QueueDeclareOptions,
};

fn main() -> Result<(), String> {
    let role = std::env::var("ROLE").expect("you must set the ROLE variable");

    let e = match role.as_str() {
        "PRODUCER" => start_producing(),
        "CONSUMER" => start_consuming(),
        _ => {
            panic!("ROLE variable must be either PRODUCER or CONSUMER");
        }
    };

    e.map_err(|e| format!("Exiting... {:?}", e))
}

fn start_consuming() -> Result<(), amiquip::Error> {
    println!("starting consumer");
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;
    let channel = connection.open_channel(None)?;
    let queue = channel.queue_declare("prefetch-test", QueueDeclareOptions::default())?;

    let prefetch_count = std::env::var("PREFETCH_COUNT")
        .expect("you must set a prefetch count for a consumer")
        .parse::<u32>()
        .expect("PREFETCH_COUNT must be a valid unsigned integer");

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
                println!("{},{},{}", i, timestamp, body.len());
                //simulate a workload
                std::thread::sleep(Duration::from_millis(workload_time));
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }

    connection.close()
}

fn start_producing() -> Result<(), amiquip::Error> {
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;
    let channel = connection.open_channel(None)?;
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
    [0..messages_to_send].iter().for_each(|_| {
        exchange
            .publish(Publish::new(
                get_random_string(message_len).as_bytes(),
                "prefetch-test",
            ))
            .expect("error publishing");
        std::thread::sleep(Duration::from_millis(time_between_sends));
    });

    connection.close()
}

/// Generates a random string with a given lenght
fn get_random_string(len: usize) -> String {
    [0..len]
        .iter()
        .map(|_| rand::random::<char>())
        .collect::<String>()
}
