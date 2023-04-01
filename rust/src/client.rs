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
use std::net::ToSocketAddrs;
use std::{collections::HashMap, sync::atomic::AtomicUsize, sync::Arc};

use parking_lot::Mutex;
use slog::{debug, o, Logger};
use tokio::sync::oneshot;

use crate::command::{RocketMQCommand, RocketMQCommandType};
use crate::dispatcher::IODispatcher;
use crate::error::{ClientError, ErrorKind};
use crate::pb::{self, MessageQueue, QueryRouteRequest, Resource};
use crate::session::SessionManager;

#[derive(Debug)]
pub(crate) struct Route {
    queue: Vec<MessageQueue>,
}

#[derive(Debug)]
pub(crate) enum RouteStatus {
    Querying(Vec<oneshot::Sender<Result<Arc<Route>, ClientError>>>),
    Found(Arc<Route>),
}

#[derive(Debug)]
pub(crate) struct Client {
    // session_manager: &'a SessionManager,
    dispatcher: IODispatcher,
    logger: Logger,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    arn: String,
    id: String,
    access_point: pb::Endpoints,
}

static CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

impl Client {
    fn client_id() -> String {
        let host = match hostname::get() {
            Ok(name) => name,
            Err(_) => "localhost".into(),
        };

        let host = match host.into_string() {
            Ok(host) => host,
            Err(_) => String::from("localhost"),
        };

        format!(
            "{}@{}#{}",
            host,
            std::process::id(),
            CLIENT_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub(crate) fn new(logger: &Logger, access_url: String) -> Result<Self, ClientError> {
        let id = Self::client_id();
        let mut endpoints = pb::Endpoints {
            scheme: pb::AddressScheme::IPv4 as i32,
            addresses: vec![],
        };

        let socket_addrs = access_url.to_socket_addrs().map_err(|e| {
            ClientError::new(
                ErrorKind::Config,
                "Failed to resolve access url.".to_string(),
                "Client::new",
            )
            .with_context("access_url", access_url)
            .set_source(e)
        })?;

        for socket_addr in socket_addrs {
            if socket_addr.is_ipv4() {
                endpoints.scheme = pb::AddressScheme::IPv4 as i32;
            } else {
                endpoints.scheme = pb::AddressScheme::IPv6 as i32;
            }

            let addr = pb::Address {
                host: socket_addr.ip().to_string(),
                port: socket_addr.port() as i32,
            };
            endpoints.addresses.push(addr);
        }

        let session_manager = SessionManager::new(&logger);
        Ok(Client {
            dispatcher: IODispatcher::new(session_manager, logger),
            logger: logger.new(o!("component" => "client")),
            route_table: Mutex::new(HashMap::new()),
            arn: String::from(""),
            id,
            access_point: endpoints,
        })
    }

    pub(crate) async fn topic_route(
        &self,
        topic: &str,
        lookup_cache: bool,
    ) -> Result<Arc<Route>, ClientError> {
        const OPERATION: &str = "query_route";

        debug!(self.logger, "Query route for topic={}", topic);
        let rx = match self
            .route_table
            .lock()
            .entry(topic.to_owned())
            .or_insert_with(|| RouteStatus::Querying(Vec::new()))
        {
            RouteStatus::Found(route) => {
                if lookup_cache {
                    return Ok(Arc::clone(route));
                }
                None
            }
            RouteStatus::Querying(ref mut v) => {
                if v.is_empty() {
                    None
                } else {
                    let (tx, rx) = oneshot::channel();
                    v.push(tx);
                    Some(rx)
                }
            }
        };

        if let Some(rx) = rx {
            return match rx.await {
                Ok(route) => route,
                Err(e) => Err(ClientError::new(
                    ErrorKind::ClientInternal,
                    "Query route failed.".to_string(),
                    OPERATION,
                )
                .set_source(e)),
            };
        }

        let client = Arc::new(self);
        let endpoint = client.access_point.addresses[0].clone();

        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: client.arn.clone(),
            }),
            endpoints: Some(client.access_point.clone()),
        };

        let mut request = tonic::Request::new(request);
        client.sign(request.metadata_mut());

        let (resp_tx, resp_rx) = oneshot::channel();
        let (err_tx, err_rx) = oneshot::channel();
        let command = RocketMQCommand {
            peer: format!("http://{}:{}", endpoint.host, endpoint.port),
            err_tx,
            detail: RocketMQCommandType::QueryRoute { request, resp_tx },
        };

        self.dispatcher.dispatch(command).await?;

        tokio::select! {
            response = resp_rx => {
                let response = response.unwrap();
                let route = Route {
                    queue: response.message_queues,
                };
                let route = Arc::new(route);
                let prev = self
                    .route_table
                    .lock()
                    .insert(topic.to_owned(), RouteStatus::Found(Arc::clone(&route)));

                match prev {
                    Some(RouteStatus::Found(_)) => {}
                    Some(RouteStatus::Querying(mut v)) => {
                        for item in v.drain(..) {
                            let _ = item.send(Ok(Arc::clone(&route)));
                        }
                    }
                    None => {}
                };
                return Ok(route);
            }
            err = err_rx => {
                return Err(err.unwrap());
            }
        }
    }

    fn sign(&self, metadata: &mut tonic::metadata::MetadataMap) {
        let _ = tonic::metadata::AsciiMetadataValue::try_from(&self.id).and_then(|v| {
            metadata.insert("x-mq-client-id", v);
            Ok(())
        });

        metadata.insert(
            "x-mq-language",
            tonic::metadata::AsciiMetadataValue::from_static("RUST"),
        );
        metadata.insert(
            "x-mq-client-version",
            tonic::metadata::AsciiMetadataValue::from_static("5.0.0"),
        );
        metadata.insert(
            "x-mq-protocol-version",
            tonic::metadata::AsciiMetadataValue::from_static("2.0.0"),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use crate::command::{RocketMQCommand, RocketMQCommandType};
    use crate::dispatcher::IODispatcher;
    use crate::log::terminal_logger;
    use crate::pb::{QueryRouteRequest, Resource};
    use crate::session::SessionManager;
    use slog::debug;
    use tokio::select;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_client_query_route() {
        let logger = terminal_logger();
        let client = Client::new(&logger, "localhost:8081".to_string()).unwrap();
        match client.topic_route("DefaultCluster", true).await {
            Ok(route) => {
                debug!(logger, "route: {:?}", route);
            }
            Err(e) => {
                debug!(logger, "err: {:?}", e);
            }
        }
    }
}
