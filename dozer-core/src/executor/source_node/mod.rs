use std::{fmt::Debug, future::Future, pin::pin, sync::Arc, time::SystemTime};

use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_log::tokio::{
    runtime::Runtime,
    sync::mpsc::{channel, Receiver, Sender},
};
use dozer_types::{
    log::debug, models::ingestion_types::TransactionInfo, node::OpIdentifier, types::TableOperation,
};
use dozer_types::{models::ingestion_types::IngestionMessage, node::SourceState};
use futures::{future::Either, StreamExt};

use crate::{
    builder_dag::NodeKind,
    epoch::Epoch,
    errors::ExecutionError,
    executor_operation::ExecutorOperation,
    forwarder::ChannelManager,
    node::{PortHandle, Source},
};

use super::{execution_dag::ExecutionDag, node::Node, ExecutorOptions};

/// The source operation collector.
#[derive(Debug)]
pub struct SourceNode<F> {
    /// To decide when to emit `Commit`, we keep track of source state.
    sources: Vec<RunningSource>,
    /// Structs for running a source.
    source_runners: Vec<SourceRunner>,
    /// Receivers from sources.
    receivers: Vec<Receiver<(PortHandle, IngestionMessage)>>,
    /// The current epoch id.
    epoch_id: u64,
    /// The shutdown future.
    shutdown: F,
    /// The runtime to run the source in.
    runtime: Arc<Runtime>,
}

impl<F: Future + Unpin> Node for SourceNode<F> {
    fn run(mut self) -> Result<(), ExecutionError> {
        let mut handles = vec![];
        for mut source_runner in self.source_runners {
            handles.push(Some(self.runtime.spawn(async move {
                source_runner
                    .source
                    .start(source_runner.sender, source_runner.last_checkpoint)
                    .await
            })));
        }
        let mut num_running_sources = handles.len();

        let mut stream = pin!(stream::receivers_stream(self.receivers));
        loop {
            let next = stream.next();
            let next = pin!(next);
            match self
                .runtime
                .block_on(futures::future::select(self.shutdown, next))
            {
                Either::Left((_, _)) => {
                    send_to_all_nodes(&self.sources, ExecutorOperation::Terminate)?;
                    return Ok(());
                }
                Either::Right((next, shutdown)) => {
                    let next = next.expect("We return just when the stream ends");
                    self.shutdown = shutdown;
                    let index = next.0;
                    let Some((port, message)) = next.1 else {
                        debug!("[{}] quit", self.sources[index].channel_manager.owner().id);
                        match self.runtime.block_on(
                            handles[index]
                                .take()
                                .expect("Shouldn't receive message from dropped receiver"),
                        ) {
                            Ok(Ok(())) => {
                                num_running_sources -= 1;
                                if num_running_sources == 0 {
                                    send_to_all_nodes(&self.sources, ExecutorOperation::Terminate)?;
                                    return Ok(());
                                }
                                continue;
                            }
                            Ok(Err(e)) => return Err(ExecutionError::Source(e)),
                            Err(e) => {
                                panic!("Source panicked: {e}");
                            }
                        }
                    };
                    let source = &mut self.sources[index];
                    match message {
                        IngestionMessage::OperationEvent { op, id, .. } => {
                            source.state = SourceState::NonRestartable;
                            source
                                .channel_manager
                                .send_op(TableOperation { op, id, port })?;
                        }
                        IngestionMessage::TransactionInfo(info) => match info {
                            TransactionInfo::Commit { id } => {
                                if let Some(id) = id {
                                    source.state = SourceState::Restartable(id);
                                } else {
                                    source.state = SourceState::NonRestartable;
                                }

                                let source_states = Arc::new(
                                    self.sources
                                        .iter()
                                        .map(|source| {
                                            (
                                                source.channel_manager.owner().clone(),
                                                source.state.clone(),
                                            )
                                        })
                                        .collect(),
                                );
                                let epoch = Epoch::new(
                                    self.epoch_id,
                                    source_states,
                                    None,
                                    None,
                                    SystemTime::now(),
                                );
                                send_to_all_nodes(
                                    &self.sources,
                                    ExecutorOperation::Commit { epoch },
                                )?;
                                self.epoch_id += 1;
                            }
                            TransactionInfo::SnapshottingStarted => {
                                source.channel_manager.send_snapshotting_started(
                                    source.channel_manager.owner().id.clone(),
                                )?;
                            }
                            TransactionInfo::SnapshottingDone { id } => {
                                source.channel_manager.send_snapshotting_done(
                                    source.channel_manager.owner().id.clone(),
                                    id,
                                )?;
                            }
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct RunningSource {
    channel_manager: ChannelManager,
    state: SourceState,
}

#[derive(Debug)]
struct SourceRunner {
    source: Box<dyn Source>,
    last_checkpoint: Option<OpIdentifier>,
    sender: Sender<(PortHandle, IngestionMessage)>,
}

/// Returns if the operation is sent successfully.
fn send_to_all_nodes(
    sources: &[RunningSource],
    op: ExecutorOperation,
) -> Result<(), ExecutionError> {
    for source in sources {
        source.channel_manager.send_non_op(op.clone())?;
    }
    Ok(())
}

pub async fn create_source_node<F>(
    dag: &mut ExecutionDag,
    options: &ExecutorOptions,
    shutdown: F,
    runtime: Arc<Runtime>,
) -> SourceNode<F> {
    let mut sources = vec![];
    let mut source_runners = vec![];
    let mut receivers = vec![];

    let node_indices = dag.graph().node_identifiers().collect::<Vec<_>>();
    for node_index in node_indices {
        let node = dag.graph()[node_index]
            .kind
            .as_ref()
            .expect("Each node should only be visited once");
        if !matches!(node, NodeKind::Source { .. }) {
            continue;
        }
        let node = dag.node_weight_mut(node_index);
        let node_handle = node.handle.clone();
        let NodeKind::Source {
            source,
            last_checkpoint,
        } = node.kind.take().unwrap()
        else {
            continue;
        };

        let senders = dag.collect_senders(node_index);
        let record_writers = dag.collect_record_writers(node_index).await;
        let channel_manager = ChannelManager::new(
            node_handle,
            record_writers,
            senders,
            dag.error_manager().clone(),
        );
        sources.push(RunningSource {
            channel_manager,
            state: SourceState::NotStarted,
        });

        let (sender, receiver) = channel(options.channel_buffer_sz);
        // let (sender, receiver) = channel(1);
        source_runners.push(SourceRunner {
            source,
            last_checkpoint,
            sender,
        });
        receivers.push(receiver);
    }

    SourceNode {
        sources,
        source_runners,
        receivers,
        epoch_id: dag.initial_epoch_id(),
        shutdown,
        runtime,
    }
}

mod stream;
