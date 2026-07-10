use dozer_core::appsource::AppSource;
use dozer_core::builder_dag::BuilderDag;
use dozer_core::channels::Channels;
use dozer_core::dag_impl::DagImpl;
use dozer_core::dag_schemas::DagSchemas;
use dozer_core::error_manager::ErrorManager;
use dozer_core::errors::DozerError;
use dozer_core::executor::Executor;
use dozer_core::executor_operation::ExecutorOperation;
use dozer_core::forwarder::Forwarder;
use dozer_core::hash_map_to_vec::HashMapToVec;
use dozer_core::lib::Lib;
use dozer_core::node::Node;
use dozer_core::record_store::RecordStore;
use dozer_core::shutdown::Shutdown;
use dozer_core::tests::tests;
use dozer_core::ui::UI;
use dozer_core::utils::get_config_path;

fn main() -> Result<(), DozerError> {
    let config_path = get_config_path(&args.config)?;
    let config = load_config(&config_path)?;
    setup_logging(&config.log_level)?;
    let ui = UI::new(&config)?;
    let pipeline = Pipeline::new(&config)?;
    let simple = Simple::new(&config)?;
    run(&args, &config, &ui, &pipeline, &simple)?;
    Ok(())
}
