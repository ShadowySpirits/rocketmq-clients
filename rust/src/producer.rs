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
use slog::Logger;

use crate::client::Client;
use crate::conf::ClientOption;
use crate::error::ClientError;
use crate::log;

struct Producer {
    logger: Logger,
    client: Client,
}

impl Producer {
    pub async fn new<T>(option: ClientOption, topics: T) -> Result<Self, ClientError>
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let logger = log::logger(option);
        let access_url = "localhost:8081";
        let client = Client::new(&logger, access_url.to_string())?;
        for topic in topics.into_iter() {
            client
                .topic_route(topic.as_ref(), true)
                .await
                .expect("TODO: panic message");
        }

        Ok(Producer { logger, client })
    }

    pub fn start(&self) {}
}
