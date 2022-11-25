pub struct ProjectionProcessorFactory {
    statement: Vec<SelectItem>,
}

impl ProjectionProcessorFactory {
    /// Creates a new [`PreAggregationProcessorFactory`].
    pub fn new(statement: Vec<SelectItem>) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory for ProjectionProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(PreAggregationProcessor {
            statement: self.statement.clone(),
            input_schema: Schema::empty(),
            expressions: vec![],
            builder: ExpressionBuilder {},
        })
    }
}
