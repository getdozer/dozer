use dozer_types::tracing::{span, Subscriber};

use tracing_subscriber::{registry::LookupSpan, Layer};
pub struct DozerLayer {}

impl<S> Layer<S> for DozerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let _ = (attrs, id, ctx);

        println!("on_new_span: {:?} : {:?}", id, attrs);
    }

    fn on_record(
        &self,
        _span: &span::Id,
        _values: &span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        println!("on_record: {:?} : {:?}", _span, _values);
    }

    fn on_event(
        &self,
        _event: &dozer_types::tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        println!("on_event: {:?}", _event);
    }
}
