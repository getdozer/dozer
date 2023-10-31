use std::sync::Arc;

use dozer_log::{
    reader::{LogReader as RustLogReader, LogReaderBuilder},
    tokio::{runtime::Runtime as TokioRuntime, sync::Mutex},
};
use neon::prelude::*;

const EXTERNAL_PROPERTY_NAME: &str = "__external__";

#[derive(Debug, Clone)]
struct Runtime {
    runtime: Arc<TokioRuntime>,
    channel: Channel,
}

impl Finalize for Runtime {}

fn new_runtime(mut cx: FunctionContext) -> JsResult<JsObject> {
    // Create the object that will be returned.
    let runtime_object = JsObject::new(&mut cx);

    // Create the runtime and store it in the object.
    let runtime = match TokioRuntime::new() {
        Ok(runtime) => runtime,
        Err(error) => return cx.throw_error(error.to_string()),
    };
    let channel = Channel::new(&mut cx);
    let managed_runtime = cx.boxed(Runtime {
        runtime: Arc::new(runtime),
        channel,
    });
    runtime_object.set(&mut cx, EXTERNAL_PROPERTY_NAME, managed_runtime)?;

    // Create the `create_reader` function.
    let create_reader = JsFunction::new(&mut cx, runtime_create_reader)?;
    runtime_object.set(&mut cx, "create_reader", create_reader)?;
    Ok(runtime_object)
}

struct LogReader {
    runtime: Runtime,
    reader: Arc<Mutex<RustLogReader>>,
}

impl Finalize for LogReader {}

fn runtime_create_reader(mut cx: FunctionContext) -> JsResult<JsPromise> {
    // Extract runtime from `this`.
    let this = cx.this();
    let runtime_object = this.downcast_or_throw::<JsObject, _>(&mut cx)?;
    let runtime = runtime_object.get::<JsBox<Runtime>, _, _>(&mut cx, EXTERNAL_PROPERTY_NAME)?;

    // Extract `server_addr` from the first argument.
    let server_addr = cx.argument::<JsString>(0)?.value(&mut cx);

    // Extract `endpoint_name` from the second argument.
    let endpoint_name = cx.argument::<JsString>(1)?.value(&mut cx);

    // Create the reader.
    let (deferred, promise) = cx.promise();
    let runtime_for_reader = (**runtime).clone();
    let channel = runtime.channel.clone();
    runtime.runtime.spawn(async move {
        // Create the builder.
        let reader_builder =
            LogReaderBuilder::new(server_addr, endpoint_name, Default::default()).await;
        match reader_builder {
            Ok(reader) => {
                // Create the reader and resolve the promise.
                let reader = reader.build(0);
                deferred.settle_with(&channel, move |mut cx| {
                    new_reader(&mut cx, runtime_for_reader, reader)
                })
            }
            // Resolve the promise on error.
            Err(e) => deferred.settle_with(&channel, move |mut cx: TaskContext<'_>| {
                cx.throw_error::<_, Handle<JsObject>>(e.to_string())
            }),
        }
    });
    Ok(promise)
}

fn new_reader<'a, C: Context<'a>>(
    cx: &mut C,
    runtime: Runtime,
    reader: RustLogReader,
) -> JsResult<'a, JsObject> {
    // Create the object that will be returned.
    let reader_object = JsObject::new(cx);

    // Store the reader in the object.
    let managed_reader = cx.boxed(LogReader {
        runtime,
        reader: Arc::new(Mutex::new(reader)),
    });
    reader_object.set(cx, EXTERNAL_PROPERTY_NAME, managed_reader)?;

    // Create the `next_op` function.
    let next_op = JsFunction::new(cx, reader_next_op)?;
    reader_object.set(cx, "next_op", next_op)?;

    Ok(reader_object)
}

fn reader_next_op(mut cx: FunctionContext) -> JsResult<JsPromise> {
    // Extract reader from `this`.
    let this = cx.this();
    let reader_object = this.downcast_or_throw::<JsObject, _>(&mut cx)?;
    let reader = reader_object.get::<JsBox<LogReader>, _, _>(&mut cx, EXTERNAL_PROPERTY_NAME)?;

    // Create the promise.
    let (deferred, promise) = cx.promise();
    let runtime = &reader.runtime;
    let channel = runtime.channel.clone();
    let reader = reader.reader.clone();
    runtime.runtime.spawn(async move {
        // Read the next operation.
        let mut reader = reader.lock().await;
        let schema = reader.schema.schema.clone();
        let op = reader.read_one().await;

        // Resolve the promise.
        deferred.settle_with(&channel, move |mut cx| {
            let op = match op {
                Ok(op) => op,
                Err(error) => return cx.throw_error(error.to_string()),
            }
            .op;
            mapper::map_executor_operation(op, &schema, &mut cx)
        });
    });
    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("Runtime", new_runtime)?;
    Ok(())
}

mod mapper;
