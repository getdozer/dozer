use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use daggy::petgraph::{visit::IntoEdgesDirected, Direction};
use dozer_log::tokio::{
    runtime::Runtime,
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use dozer_types::{log::debug, node::NodeHandle};
use dozer_types::{models::ingestion_types::IngestionMessage, node::OpIdentifier};

use crate::{
    builder_dag::NodeKind,
    dag_schemas::EdgeKind,
    errors::ExecutionError,
    forwarder::SourceChannelManager,
    node::{PortHandle, Source},
};

use super::{execution_dag::ExecutionDag, node::Node, ExecutorOptions};

/// The sender half of a source in the execution DAG.
#[derive(Debug)]
pub struct SourceNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// The source.
    source: Box<dyn Source>,
    /// The sender that will be passed to the source for outputting data.
    sender: Sender<(PortHandle, IngestionMessage)>,
    /// Checkpoint for the source.
    last_checkpoint: Option<OpIdentifier>,
    /// The receiver for receiving data from source.
    receiver: Receiver<(PortHandle, IngestionMessage)>,
    /// Receiving timeout.
    timeout: Duration,
    /// If the execution DAG should be running. Used for determining if a `terminate` message should be sent.
    running: Arc<AtomicBool>,
    /// This node's output channel manager, for communicating to other sources to coordinate terminate and commit, forwarding data, writing metadata and writing port state.
    channel_manager: SourceChannelManager,
    /// The runtime to run the source in.
    runtime: Arc<Runtime>,
}

impl SourceNode {
    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }
}

/// Returns if the node should terminate.
fn send_and_trigger_commit_if_needed(
    running: &AtomicBool,
    channel_manager: &mut SourceChannelManager,
    data: DataKind,
    node_handle: &NodeHandle,
) -> Result<bool, ExecutionError> {
    // If termination was requested the or source quit, we try to terminate.
    let terminating =
        data == DataKind::NoDataBecauseOfChannelDisconnection || !running.load(Ordering::SeqCst);
    // If this commit was not requested with termination at the start, we shouldn't terminate either.
    let terminating = match data {
        DataKind::Data((port, message)) => {
            channel_manager.send_and_trigger_commit_if_needed(message, port, terminating)?
        }
        DataKind::NoDataBecauseOfTimeout | DataKind::NoDataBecauseOfChannelDisconnection => {
            channel_manager.trigger_commit_if_needed(terminating)?
        }
    };
    if terminating {
        channel_manager.terminate()?;
        debug!("[{}] Quitting", &node_handle);
    }
    Ok(terminating)
}

impl Node for SourceNode {
    fn run(mut self) -> Result<(), ExecutionError> {
        let mut source = self.source;
        let sender = self.sender;
        let last_checkpoint = self.last_checkpoint;
        let mut handle = Some(
            self.runtime
                .spawn(async move { source.start(sender, last_checkpoint).await }),
        );

        loop {
            let terminating = match self
                .runtime
                .block_on(async { timeout(self.timeout, self.receiver.recv()).await })
            {
                Ok(Some(data)) => send_and_trigger_commit_if_needed(
                    &self.running,
                    &mut self.channel_manager,
                    DataKind::Data(data),
                    &self.node_handle,
                )?,
                Err(_) => send_and_trigger_commit_if_needed(
                    &self.running,
                    &mut self.channel_manager,
                    DataKind::NoDataBecauseOfTimeout,
                    &self.node_handle,
                )?,
                Ok(None) => {
                    if let Some(handle) = handle.take() {
                        match self.runtime.block_on(handle) {
                            // Propagate the panic.
                            Err(e) => std::panic::panic_any(e),
                            // Propagate error.
                            Ok(Err(e)) => return Err(ExecutionError::Source(e)),
                            Ok(Ok(())) => (),
                        }
                    }
                    send_and_trigger_commit_if_needed(
                        &self.running,
                        &mut self.channel_manager,
                        DataKind::NoDataBecauseOfChannelDisconnection,
                        &self.node_handle,
                    )?
                }
            };
            if terminating {
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum DataKind {
    Data((PortHandle, IngestionMessage)),
    NoDataBecauseOfTimeout,
    NoDataBecauseOfChannelDisconnection,
}

pub async fn create_source_node(
    dag: &mut ExecutionDag,
    node_index: daggy::NodeIndex,
    options: &ExecutorOptions,
    running: Arc<AtomicBool>,
    runtime: Arc<Runtime>,
) -> Result<SourceNode, ExecutionError> {
    // Get the source node.
    let Some(node) = dag.node_weight_mut(node_index).take() else {
        panic!("Must pass in a node")
    };
    let node_handle = node.handle;
    let NodeKind::Source {
        source,
        last_checkpoint,
    } = node.kind
    else {
        panic!("Must pass in a source node");
    };
    let port_names = dag
        .graph()
        .edges_directed(node_index, Direction::Outgoing)
        .map(|edge| {
            let EdgeKind::FromSource { port_name, .. } = &edge.weight().edge_kind else {
                panic!("Must pass in from source edges");
            };
            (edge.weight().output_port, port_name.clone())
        })
        .collect();

    // Create channel between source and source node.
    let (sender, receiver) = channel(options.channel_buffer_sz);
    // let (sender, receiver) = channel(1);

    // Create channel manager.
    let (senders, record_writers) = dag.collect_senders_and_record_writers(node_index).await;
    let source_state = source
        .serialize_state()
        .await
        .map_err(ExecutionError::Source)?;
    let channel_manager = SourceChannelManager::new(
        node_handle.clone(),
        source_state,
        port_names,
        record_writers,
        senders,
        options.commit_sz,
        options.commit_time_threshold,
        dag.epoch_manager().clone(),
        dag.error_manager().clone(),
    );

    Ok(SourceNode {
        node_handle,
        source,
        sender,
        last_checkpoint,
        receiver,
        timeout: options.commit_time_threshold,
        running,
        channel_manager,
        runtime,
    })
}
