/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashSet;
use std::sync::Mutex;

use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::model::transaction::{Transaction, TransactionResolution};
use rocketmq::Producer;

lazy_static::lazy_static! {
    static  ref MESSAGE_ID_SET: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
}

#[tokio::main]
async fn main() {
    // recommend to specify which topic(s) you would like to send message to
    // producer will prefetch topic route when start and failed fast if topic not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["transaction_test"]);

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");

    // build and start transaction producer, which has TransactionChecker
    let mut producer = Producer::new_transaction_producer(
        producer_option,
        client_option,
        Box::new(|transaction_id, message| {
            if MESSAGE_ID_SET
                .lock()
                .unwrap()
                .contains(message.message_id())
            {
                println!(
                    "commit transaction: transaction_id: {}, message_id: {}",
                    transaction_id, message.message_id()
                );
                TransactionResolution::COMMIT
            } else {
                println!(
                    "rollback transaction due to unknown message: transaction_id: {}, message_id: {}",
                    transaction_id, message.message_id()
                );
                TransactionResolution::ROLLBACK
            }
        }),
    )
    .unwrap();
    producer.start().await.unwrap();

    // build message
    let message = MessageBuilder::transaction_message_builder(
        "transaction_test",
        "hello world".as_bytes().to_vec(),
    )
    .build()
    .unwrap();

    // send message to rocketmq proxy
    let result = producer.send_transaction_message(message).await;
    if let Err(error) = result {
        eprintln!("send message failed: {:?}", error);
        return;
    }
    let transaction = result.unwrap();
    println!(
        "send message success, message_id={}, transaction_id={}",
        transaction.message_id(),
        transaction.transaction_id()
    );

    MESSAGE_ID_SET
        .lock()
        .unwrap()
        .insert(transaction.message_id().to_string());

    // commit transaction manually
    // delete following two lines so that RocketMQ server will check transaction status periodically
    let result = transaction.commit().await;
    debug_assert!(result.is_ok(), "commit transaction failed: {:?}", result);
}
