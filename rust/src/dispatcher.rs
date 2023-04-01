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
use std::sync::{Arc, Mutex};
use std::thread;

use slog::{o, Logger};
use tokio::sync::{mpsc, oneshot};

use crate::command::{RocketMQCommand, RocketMQCommandType};
use crate::error::{ClientError, ErrorKind};
use crate::pb::{QueryRouteRequest, QueryRouteResponse};
use crate::session::{Session, SessionManager};

#[derive(Debug)]
pub(crate) struct IODispatcher {
    logger: Logger,
    session_manager: Arc<Mutex<SessionManager>>,
    tx: mpsc::Sender<RocketMQCommand>,
    stop_tx: mpsc::Sender<()>,
}

impl IODispatcher {
    const OPERATION_DISPATCH: &'static str = "dispatcher.dispatch";
    const OPERATION_STOP: &'static str = "dispatcher.stop";

    pub(crate) fn new(session_manager: SessionManager, logger: &Logger) -> Self {
        let session_manager = Arc::new(Mutex::new(session_manager));
        let (tx, stop_tx) = IODispatcher::start(session_manager.clone());
        IODispatcher {
            logger: logger.new(o!("component" => "io_dispatcher")),
            session_manager,
            tx,
            stop_tx,
        }
    }

    fn start(
        session_manager: Arc<Mutex<SessionManager>>,
    ) -> (mpsc::Sender<RocketMQCommand>, mpsc::Sender<()>) {
        let (tx, mut rx) = mpsc::channel(1000);
        let (stop_tx, mut stop_rx) = mpsc::channel(1);
        // let stop_rx = &self.stop_rx;
        thread::spawn(move || {
            let local_tasks = tokio::task::LocalSet::new();
            local_tasks.spawn_local(async move {
                loop {
                    tokio::select! {
                        Some(command) = rx.recv() => {
                            let session_manager = session_manager.clone();
                            IODispatcher::handle_command(session_manager, command);
                        }
                        _ = stop_rx.recv() => {
                            break;
                        }
                    }
                }
            });
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();
            rt.block_on(local_tasks);
        });
        (tx, stop_tx)
    }

    pub(crate) async fn stop(self) -> Result<(), ClientError> {
        self.stop_tx.send(()).await.map_err(|_e| {
            ClientError::new(
                ErrorKind::ChannelSend,
                "Failed to stop dispatcher.".to_string(),
                IODispatcher::OPERATION_STOP,
            )
        })
    }

    fn handle_command(session_manager: Arc<Mutex<SessionManager>>, command: RocketMQCommand) {
        tokio::task::spawn_local(async move {
            let session = session_manager
                .lock()
                .unwrap()
                .get_session(command.peer)
                .await;
            match session {
                Ok(session) => match command.detail {
                    RocketMQCommandType::QueryRoute { request, resp_tx } => {
                        IODispatcher::query_route(session, request, resp_tx, command.err_tx).await;
                    }
                },
                Err(e) => {
                    command.err_tx.send(e).expect("TODO: panic message");
                }
            }
        });
    }

    pub(crate) async fn dispatch(&self, command: RocketMQCommand) -> Result<(), ClientError> {
        self.tx.send(command).await.map_err(|_e| {
            ClientError::new(
                ErrorKind::ChannelSend,
                "Failed to dispatch command.".to_string(),
                IODispatcher::OPERATION_DISPATCH,
            )
        })
    }

    async fn query_route(
        session: Arc<Mutex<Session>>,
        request: tonic::Request<QueryRouteRequest>,
        resp_tx: oneshot::Sender<QueryRouteResponse>,
        err_tx: oneshot::Sender<ClientError>,
    ) {
        let response = session.lock().unwrap().query_route(request).await;
        match response {
            Ok(response) => {
                resp_tx
                    .send(response.into_inner())
                    .expect("TODO: panic message");
            }
            Err(e) => {
                err_tx.send(e).expect("TODO: panic message");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use slog::debug;
    use tokio::select;
    use tokio::sync::oneshot;

    use crate::command::{RocketMQCommand, RocketMQCommandType};
    use crate::dispatcher::IODispatcher;
    use crate::log::terminal_logger;
    use crate::pb::{QueryRouteRequest, Resource};
    use crate::session::SessionManager;

    #[tokio::test]
    async fn test_dispatcher_dispatch() {
        let logger = terminal_logger();
        let mut session_manager = SessionManager::new(&logger);
        let dispatcher = IODispatcher::new(session_manager, &logger);
        let (resp_tx, resp_rx) = oneshot::channel();
        let (err_tx, err_rx) = oneshot::channel();
        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: "DefaultCluster".to_string(),
                resource_namespace: "".to_string(),
            }),
            endpoints: None,
        };
        dispatcher
            .dispatch(RocketMQCommand {
                peer: "http://localhost:8081".to_string(),
                err_tx,
                detail: RocketMQCommandType::QueryRoute {
                    request: tonic::Request::new(request),
                    resp_tx,
                },
            })
            .await
            .unwrap();

        select! {
            resp = resp_rx => {
                debug!(logger, "resp: {:?}", resp);
            }
            err = err_rx => {
                debug!(logger, "err: {:?}", err);
            }
        }
    }
}
