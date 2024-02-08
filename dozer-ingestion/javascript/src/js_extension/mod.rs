use std::{future::Future, sync::Arc};

use dozer_deno::deno_runtime::{
    deno_core::{self, anyhow::Error, extension, op2, ModuleSpecifier},
    permissions::PermissionsContainer,
    worker::{MainWorker, WorkerOptions},
};
use dozer_ingestion_connector::{
    dozer_types::{
        errors::{internal::BoxedError, types::DeserializationError},
        json_types::serde_json_to_json_value,
        models::ingestion_types::{IngestionMessage, TransactionInfo},
        serde::{Deserialize, Serialize},
        serde_json,
        thiserror::{self, Error},
        types::{Field, Operation, Record},
    },
    tokio::{runtime::Runtime, task::LocalSet},
    Ingestor,
};

#[derive(Deserialize, Serialize, Debug)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub enum MsgType {
    SnapshottingStarted,
    SnapshottingDone,
    Insert,
    Delete,
    Update,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct JsMessage {
    typ: MsgType,
    old_val: serde_json::Value,
    new_val: serde_json::Value,
}

#[op2(async)]
fn ingest(
    #[state] ingestor: &Ingestor,
    #[serde] val: JsMessage,
) -> impl Future<Output = Result<(), Error>> {
    send(ingestor.clone(), val)
}

extension!(
    dozer_extension,
    ops = [ingest],
    options = { ingestor: Ingestor },
    state = |state, options| {
        state.put(options.ingestor);
    },
);

pub struct JsExtension {
    runtime: Arc<Runtime>,
    ingestor: Ingestor,
    module_specifier: ModuleSpecifier,
}

#[derive(Debug, Error)]
pub enum JsExtensionError {
    #[error("Failed to canonicalize path {0}: {1}")]
    CanonicalizePath(String, #[source] std::io::Error),
}

impl JsExtension {
    pub fn new(
        runtime: Arc<Runtime>,
        ingestor: Ingestor,
        js_path: String,
    ) -> Result<Self, JsExtensionError> {
        let path = std::fs::canonicalize(js_path.clone())
            .map_err(|e| JsExtensionError::CanonicalizePath(js_path, e))?;
        let module_specifier =
            ModuleSpecifier::from_file_path(path).expect("we just canonicalized it");
        Ok(Self {
            runtime,
            ingestor,
            module_specifier,
        })
    }

    pub async fn run(self) -> Result<(), BoxedError> {
        let runtime = self.runtime.clone();
        runtime
            .spawn_blocking(move || {
                let local_set = LocalSet::new();
                local_set.block_on(&self.runtime, async move {
                    let mut worker = MainWorker::bootstrap_from_options(
                        self.module_specifier.clone(),
                        PermissionsContainer::allow_all(),
                        WorkerOptions {
                            module_loader: std::rc::Rc::new(
                                dozer_deno::TypescriptModuleLoader::new()?,
                            ),
                            extensions: vec![dozer_extension::init_ops(self.ingestor)],
                            ..Default::default()
                        },
                    );

                    worker.execute_main_module(&self.module_specifier).await?;
                    worker.run_event_loop(false).await
                })
            })
            .await
            .unwrap() // Propagate panics.
            .map_err(Into::into)
    }
}

async fn send(ingestor: Ingestor, val: JsMessage) -> Result<(), Error> {
    let msg = match val.typ {
        MsgType::SnapshottingStarted => {
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingStarted)
        }
        MsgType::SnapshottingDone => {
            IngestionMessage::TransactionInfo(TransactionInfo::SnapshottingDone { id: None })
        }
        MsgType::Insert | MsgType::Delete | MsgType::Update => {
            let op = map_operation(val)?;
            IngestionMessage::OperationEvent {
                table_index: 0,
                op,
                id: None,
            }
        }
    };

    // Ignore if the receiver is closed.
    let _ = ingestor.handle_message(msg).await;
    let _ = ingestor
        .handle_message(IngestionMessage::TransactionInfo(TransactionInfo::Commit {
            id: None,
        }))
        .await;
    Ok(())
}

fn map_operation(msg: JsMessage) -> Result<Operation, DeserializationError> {
    Ok(match msg.typ {
        MsgType::Insert => Operation::Insert {
            new: Record {
                values: vec![Field::Json(serde_json_to_json_value(msg.new_val)?)],
                lifetime: None,
            },
        },
        MsgType::Delete => Operation::Delete {
            old: Record {
                values: vec![Field::Json(serde_json_to_json_value(msg.old_val)?)],
                lifetime: None,
            },
        },
        MsgType::Update => Operation::Update {
            old: Record {
                values: vec![Field::Json(serde_json_to_json_value(msg.old_val)?)],
                lifetime: None,
            },
            new: Record {
                values: vec![Field::Json(serde_json_to_json_value(msg.new_val)?)],
                lifetime: None,
            },
        },
        _ => unreachable!(),
    })
}

#[cfg(test)]
mod tests;
