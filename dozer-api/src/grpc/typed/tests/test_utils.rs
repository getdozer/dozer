





// pub fn mock_event_notifier() -> channel::Receiver<PipelineRequest> {
//     let (sender, receiver) = channel::unbounded::<PipelineRequest>();
//     let _executor_thread = thread::spawn(move || loop {
//         thread::sleep(time::Duration::from_millis(1000));

//         let fake_event = PipelineRequest {
//             endpoint: "films".to_string(),
//             api_event: Some(ApiEvent::Op(Operation {
//                 typ: OperationType::Insert as i32,
//                 old: None,
//                 new: Some(Record {
//                     values: vec![
//                         Value {
//                             value: Some(value::Value::UintValue(32)),
//                         },
//                         Value {
//                             value: Some(value::Value::StringValue("description".to_string())),
//                         },
//                         Value { value: None },
//                         Value { value: None },
//                     ],
//                 }),
//                 endpoint_name: "films".to_string(),
//             })),
//         };
//         sender.try_send(fake_event).unwrap();
//     });
//     receiver
// }
