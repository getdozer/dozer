use std::{
    collections::HashMap,
    env::{self, current_dir},
    fs::{create_dir, File},
    path::{Path, PathBuf},
};

use dozer_types::serde::Serialize;

use crate::e2e_tests::{Case, CaseKind};

pub enum RunningEnv {
    WithExpectations {
        docker_compose_path: PathBuf,
        dozer_test_client_service_name: String,
    },
    WithErrorExpectation {
        docker_compose_path: PathBuf,
        dozer_service_name: String,
        dozer_config_path: String,
    },
}

pub fn create_docker_compose_for_local_runner(case: &Case) -> Option<PathBuf> {
    let mut services = HashMap::new();

    add_source_services(&mut services);

    if services.is_empty() {
        None
    } else {
        let local_runner_path = case.case_dir.join("local_runner");
        create_dir_if_not_existing(&local_runner_path);
        let docker_compose_path = local_runner_path.join("docker-compose.yaml");
        write_docker_compose(&docker_compose_path, services);
        Some(docker_compose_path)
    }
}

pub fn create_buildkite_runner_envs(case: &Case) -> Vec<RunningEnv> {
    vec![create_buildkite_runner_env(case, true)]
}

fn create_buildkite_runner_env(case: &Case, single_dozer_container: bool) -> RunningEnv {
    let docker_compose_dir = case.case_dir.join(if single_dozer_container {
        "dozer_single_container"
    } else {
        "dozer_multi_container"
    });
    create_dir_if_not_existing(&docker_compose_dir);

    let mut services = HashMap::new();

    add_source_services(&mut services);

    let (
        dozer_api_service_name,
        dozer_service_name_for_error_expectation,
        dozer_config_path_in_container,
    ) = if single_dozer_container {
        add_dozer_service(&case.dozer_config_path, &mut services)
    } else {
        todo!()
    };

    let dozer_test_client_service_name = if let CaseKind::Expectations(_) = &case.kind {
        Some(add_dozer_test_client_service(
            dozer_api_service_name,
            &case.case_dir,
            &mut services,
        ))
    } else {
        None
    };

    let docker_compose_path = docker_compose_dir.join("docker-compose.yaml");
    write_docker_compose(&docker_compose_path, services);

    if let Some(dozer_test_client_service_name) = dozer_test_client_service_name {
        RunningEnv::WithExpectations {
            docker_compose_path,
            dozer_test_client_service_name,
        }
    } else {
        RunningEnv::WithErrorExpectation {
            docker_compose_path,
            dozer_service_name: dozer_service_name_for_error_expectation,
            dozer_config_path: dozer_config_path_in_container,
        }
    }
}

fn add_dozer_service(
    dozer_config_path: &str,
    services: &mut HashMap<String, Service>,
) -> (String, String, String) {
    let depends_on = services.keys().cloned().collect();
    let dozer_service_name = "dozer";
    let dozer_config_path_in_container = "/dozer/dozer-config.yaml";
    services.insert(
        dozer_service_name.to_string(),
        Service {
            image: Some("public.ecr.aws/k7k6x1d4/dozer".to_string()),
            build: None,
            ports: vec![],
            environment: vec![format!("ETH_WSS_URL={}", eth_wss_url())],
            volumes: vec![format!(
                "{}:{}",
                dozer_config_path, dozer_config_path_in_container
            )],
            command: Some(format!(
                "dozer --config-path {}",
                dozer_config_path_in_container
            )),
            depends_on,
        },
    );

    (
        dozer_service_name.to_string(),
        dozer_service_name.to_string(),
        dozer_config_path_in_container.to_string(),
    )
}

fn add_dozer_test_client_service(
    dozer_api_service_name: String,
    case_dir: &Path,
    services: &mut HashMap<String, Service>,
) -> String {
    let dozer_test_client_service_name = "dozer-test-client";
    let case_dir = case_dir
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {:?}", case_dir));
    let case_dir_in_container = "/case";
    let dozer_test_client_path = current_dir()
        .expect("Cannot get current dir")
        .join("target/debug/dozer-test-client");
    if !dozer_test_client_path.exists() {
        panic!(
            "dozer-test-client not found at {:?}. Did you run `cargo build --bin dozer-test-client`?",
            dozer_test_client_path
        );
    }
    let dozer_test_client_path = dozer_test_client_path
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {:?}", dozer_test_client_path));
    let dozer_test_client_path_in_container = "/dozer-test-client";
    services.insert(
        dozer_test_client_service_name.to_string(),
        Service {
            image: Some("public.ecr.aws/k7k6x1d4/dozer".to_string()),
            build: None,
            ports: vec![],
            environment: vec!["RUST_LOG=info".to_string()],
            volumes: vec![
                format!(
                    "{}:{}",
                    dozer_test_client_path, dozer_test_client_path_in_container
                ),
                format!("{}:{}", case_dir, case_dir_in_container),
            ],
            command: Some(format!(
                "{} --wait-in-millis 1000 --dozer-api-host {} --case-dir {}",
                dozer_test_client_path_in_container, dozer_api_service_name, case_dir_in_container
            )),
            depends_on: vec![dozer_api_service_name],
        },
    );
    dozer_test_client_service_name.to_string()
}

fn add_source_services(_services: &mut HashMap<String, Service>) {
    // TODO: Add source services
}

fn eth_wss_url() -> String {
    env::var("ETH_WSS_URL").expect("ETH_WSS_URL is not set")
}

fn create_dir_if_not_existing(path: &Path) {
    if !path.exists() {
        create_dir(path).unwrap_or_else(|e| panic!("Failed to create directory {:?}: {}", path, e));
    }
}

fn write_docker_compose(path: &Path, services: HashMap<String, Service>) {
    let file =
        File::create(path).unwrap_or_else(|e| panic!("Failed to create file {:?}: {}", path, e));
    dozer_types::serde_yaml::to_writer(file, &DockerCompose { services })
        .unwrap_or_else(|e| panic!("Failed to write docker compose file {:?}: {}", path, e));
}

#[derive(Serialize)]
#[serde(crate = "dozer_types::serde")]
struct DockerCompose {
    services: HashMap<String, Service>,
}

#[derive(Serialize)]
#[serde(crate = "dozer_types::serde")]
struct Service {
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    ports: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    environment: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
}
