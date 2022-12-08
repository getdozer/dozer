



#[macro_export]
macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

pub(crate) fn init_log4rs() {
    // let config = Config::builder()
    //     .appender(Appender::builder().build("stdout", Box::new(ConsoleAppender::builder().build())))
    //     .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
    //     .unwrap();
    //
    // let handle = log4rs::init_config(config).unwrap();
}
