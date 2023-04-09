use std::path::Path;

use dozer_log::ExecutorOperation;
use dozer_log::{reader::LogReader, schemas::load_schemas, tokio::runtime::Runtime};

mod mapper;
use dozer_types::crossbeam::channel::bounded;
use futures_util::StreamExt;
use mapper::{map_read_operation, ControlMessage, LogReaderWrapper};
use neon::prelude::*;

fn init(mut cx: FunctionContext) -> JsResult<JsBox<LogReaderWrapper>> {
    let name = cx.argument::<JsString>(0)?;
    let name = name.value(&mut cx);

    let home_dir = cx.argument_opt(1);
    let home_dir = match home_dir {
        Some(home_dir) => home_dir.to_string(&mut cx)?.value(&mut cx),
        None => ".dozer".to_string(),
    };

    let pipeline_path = Path::new(&home_dir).join("pipeline");

    let schemas = load_schemas(&pipeline_path).unwrap();

    let schema = schemas.get(&name).unwrap().clone();

    let path = pipeline_path.join("logs").join(&name);

    let mut log_reader = LogReader::new(&path, &name, 0, None).unwrap();

    let (sender, receiver) = bounded::<ControlMessage>(10);

    let (msg_sender, msg_receiver) = bounded::<Option<ExecutorOperation>>(10);

    let log_wrapper = LogReaderWrapper {
        sender,
        msg_receiver,
        schema,
    };

    std::thread::spawn(move || {
        Runtime::new().unwrap().block_on(async move {
            while let Ok(msg) = receiver.recv() {
                match msg {
                    ControlMessage::Stop => break,
                    ControlMessage::Read => {
                        let op = log_reader.next().await;

                        msg_sender.send(op).unwrap();
                    }
                }
            }
        });
    });
    Ok(cx.boxed(log_wrapper))
}

fn read(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let wrapper = cx.argument::<JsBox<LogReaderWrapper>>(0)?;

    let sender = wrapper.sender.clone();
    let receiver = wrapper.msg_receiver.clone();
    let schema = wrapper.schema.clone();

    let callback = cx.argument::<JsFunction>(1)?.root(&mut cx);
    let channel = cx.channel();

    std::thread::spawn(move || {
        sender.send(ControlMessage::Read).unwrap();

        let op = receiver.recv().unwrap();
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();

            let msg = op.map(|op| map_read_operation(op, &schema, &mut cx).unwrap());
            let args = match msg {
                Some(op) => vec![cx.null().upcast::<JsValue>(), op.upcast()],
                None => {
                    let err = cx.string("EOF");
                    vec![err.upcast::<JsValue>()]
                }
            };

            callback.call(&mut cx, this, args).unwrap();
            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn stop(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let wrapper = cx.argument::<JsBox<LogReaderWrapper>>(0)?;

    let sender = wrapper.sender.clone();

    std::thread::spawn(move || {
        sender.send(ControlMessage::Stop).unwrap();
    });

    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("init", init)?;
    cx.export_function("read", read)?;
    cx.export_function("stop", stop)?;
    Ok(())
}
