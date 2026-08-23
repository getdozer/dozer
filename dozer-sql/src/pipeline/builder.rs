impl PipelineBuilder {
    pub fn build_in_subquery_join(
        &mut self,
        left_expr: &Expr,
        subquery: &TableSubquery,
        negated: bool,
    ) -> Result<ProcessorNode, PipelineError> {
        let right_stream = self.build_subquery_pipeline(subquery)?;
        let join_type = if negated {
            JoinType::LeftAnti
        } else {
            JoinType::Inner
        };
        let join_processor = JoinProcessor::new(
            join_type,
            vec![(left_expr.clone(), Expr::Identifier(Ident::new("subquery_col")))],
        );
        self.add_processor_node(join_processor, vec![self.current_stream(), right_stream])
    }
}
