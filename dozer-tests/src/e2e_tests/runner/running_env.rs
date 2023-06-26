use std::{
    collections::HashMap,
    env::{self, current_dir},
    fs::{create_dir, File},
    path::{Path, PathBuf},
};

use dozer_types::constants::DEFAULT_CONFIG_PATH;
use dozer_types::{
    log::info,
    models::{app_config::Config, connection::ConnectionConfig},
};

use crate::e2e_tests::{
    docker_compose::{Build, Condition, DependsOn, DockerCompose, Service},
    Case, CaseKind, Connection,
};

pub struct LocalDockerCompose {
    pub path: PathBuf,
    pub connections_healthy_service_name: String,
}

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

pub fn create_docker_compose_for_local_runner(case: &Case) -> Option<LocalDockerCompose> {
    let mut services = HashMap::new();

    let connections_healthy_service_name =
        add_connection_services(&case.connections, &mut services);

    if let Some(connections_healthy_service_name) = connections_healthy_service_name {
        let local_runner_path = case.case_dir.join("local_runner");
        create_dir_if_not_existing(&local_runner_path);
        let docker_compose_path = local_runner_path.join("docker-compose.yaml");
        write_docker_compose(&docker_compose_path, services);
        Some(LocalDockerCompose {
            path: docker_compose_path,
            connections_healthy_service_name,
        })
    } else {
        None
    }
}

pub fn create_buildkite_runner_envs(case: &Case) -> Vec<RunningEnv> {
    vec![create_buildkite_runner_env(case, true)]
}

fn create_buildkite_runner_env(case: &Case, single_dozer_container: bool) -> RunningEnv {
    let docker_compose_dir = case.case_dir.join(if single_dozer_container {
        "builkite_runner_dozer_single_container"
    } else {
        "buildkite_runner_dozer_multi_container"
    });
    create_dir_if_not_existing(&docker_compose_dir);

    let dozer_config_path = write_dozer_config_for_running_in_docker_compose(
        case.dozer_config.clone(),
        &case.connections,
        &docker_compose_dir,
    );

    let mut services = HashMap::new();

    let connections_healthy_service_name =
        add_connection_services(&case.connections, &mut services);

    let (dozer_api_service_name, dozer_service_name_for_error_expectation) =
        if single_dozer_container {
            add_dozer_service(
                &dozer_config_path,
                connections_healthy_service_name,
                &mut services,
            )
        } else {
            todo!()
        };

    let dozer_test_client_service_name = if let CaseKind::Expectations(_) = &case.kind {
        Some(add_dozer_test_client_service(
            dozer_api_service_name,
            &case.case_dir,
            &case.connections_dir,
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
            dozer_config_path,
        }
    }
}

fn add_dozer_service(
    dozer_config_path: &str,
    depends_on_service_name: Option<String>,
    services: &mut HashMap<String, Service>,
) -> (String, String) {
    let mut depends_on = HashMap::new();
    if let Some(depends_on_service_name) = depends_on_service_name {
        depends_on.insert(
            depends_on_service_name,
            DependsOn {
                condition: Condition::ServiceCompletedSuccessfully,
            },
        );
    }

    let dozer_service_name = "dozer";
    let current_dir = current_dir().expect("Cannot get current dir");
    let current_dir = current_dir.to_str().expect("Non-UTF8 path {current_dir:?}");
    services.insert(
        dozer_service_name.to_string(),
        Service {
            container_name: Some("dozer".to_string()),
            image: Some(get_dozer_image()),
            build: None,
            ports: vec![],
            environment: vec!["ETH_WSS_URL".to_string(), "ETH_HTTPS_URL".to_string()],
            volumes: vec![format!("{current_dir}:{current_dir}")],
            user: None,
            working_dir: Some(current_dir.to_string()),
            command: Some(format!(
                "DOZER_DEV=ci dozer --config-path {dozer_config_path}"
            )),
            depends_on,
            healthcheck: None,
        },
    );

    (
        dozer_service_name.to_string(),
        dozer_service_name.to_string(),
    )
}

fn add_dozer_test_client_service(
    dozer_api_service_name: String,
    case_dir: &Path,
    connections_dir: &Path,
    services: &mut HashMap<String, Service>,
) -> String {
    let dozer_test_client_service_name = "dozer-test-client";
    let case_dir = case_dir
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {case_dir:?}"));
    let connections_dir = connections_dir
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {connections_dir:?}"));
    let case_dir_in_container = "/case";
    let connections_dir_in_container = "/connections";
    let dozer_test_client_path = current_dir()
        .expect("Cannot get current dir")
        .join("target/debug/dozer-test-client");
    if !dozer_test_client_path.exists() {
        panic!(
            "dozer-test-client not found at {dozer_test_client_path:?}. Did you run `cargo build --bin dozer-test-client`?"
        );
    }
    let dozer_test_client_path = dozer_test_client_path
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {dozer_test_client_path:?}"));
    let dozer_test_client_path_in_container = "/dozer-test-client";
    services.insert(
        dozer_test_client_service_name.to_string(),
        Service {
            container_name: Some("dozer_test_client".to_string()),
            image: Some(get_dozer_tests_image()),
            build: None,
            ports: vec![],
            environment: vec![("RUST_LOG=info".to_string())],
            volumes: vec![
                format!(
                    "{dozer_test_client_path}:{dozer_test_client_path_in_container}"
                ),
                format!("{case_dir}:{case_dir_in_container}"),
                format!("{connections_dir}:{connections_dir_in_container}"),
            ],
            user: None,
            working_dir: None,
            command: Some(format!(
                "{dozer_test_client_path_in_container} --wait-in-millis 10000 --dozer-api-host {dozer_api_service_name} --case-dir {case_dir_in_container} --connections-dir {connections_dir_in_container}"
            )),
            depends_on: vec![(
                dozer_api_service_name,
                DependsOn {
                    condition: Condition::ServiceStarted,
                },
            )]
            .into_iter()
            .collect(),
            healthcheck: None,
        },
    );
    dozer_test_client_service_name.to_string()
}

fn add_connection_services(
    connections: &HashMap<String, Connection>,
    services: &mut HashMap<String, Service>,
) -> Option<String> {
    let mut depends_on = HashMap::new();

    for (name, connection) in connections {
        let mut service = connection.service.clone().unwrap_or_default();

        if connection.has_docker_file {
            let context = connection
                .directory
                .to_str()
                .unwrap_or_else(|| panic!("Non-UTF8 path: {:?}", connection.directory));
            service.build = Some(Build {
                context: context.to_string(),
                dockerfile: None,
            });
        } else {
            service.build = None;
        }

        depends_on.insert(
            name.clone(),
            DependsOn {
                condition: if connection.has_oneshot_file {
                    Condition::ServiceCompletedSuccessfully
                } else if service.healthcheck.is_some() {
                    Condition::ServiceHealthy
                } else {
                    Condition::ServiceStarted
                },
            },
        );
        services.insert(name.clone(), service);
    }

    if depends_on.is_empty() {
        return None;
    }

    let connections_healthy_service_name = "dozer-wait-for-connections-healthy";
    services.insert(
        connections_healthy_service_name.to_string(),
        Service {
            container_name: None,
            image: Some("alpine".to_string()),
            build: None,
            ports: vec![],
            environment: vec![],
            volumes: vec![],
            user: None,
            working_dir: None,
            // Give the connection some time to start.
            command: Some("sleep 10".to_string()),
            depends_on,
            healthcheck: None,
        },
    );
    Some(connections_healthy_service_name.to_string())
}

fn create_dir_if_not_existing(path: &Path) {
    if !path.exists() {
        create_dir(path).unwrap_or_else(|e| panic!("Failed to create directory {path:?}: {e}"));
    }
}

fn get_dozer_image() -> String {
    let version = env::var("DOZER_VERSION").unwrap_or_else(|_| "latest".to_string());
    let result = format!("public.ecr.aws/k7k6x1d4/dozer:{version}");
    info!("Using dozer image: {}", result);
    result
}

// This image is built by `.buildkite/build_dozer_tests/docker-compose.yaml`
fn get_dozer_tests_image() -> String {
    "dozer-tests".to_string()
}

fn write_docker_compose(path: &Path, services: HashMap<String, Service>) {
    let file = File::create(path).unwrap_or_else(|e| panic!("Failed to create file {path:?}: {e}"));
    dozer_types::serde_yaml::to_writer(file, &DockerCompose::new_v2_4(services))
        .unwrap_or_else(|e| panic!("Failed to write docker compose file {path:?}: {e}"));
}

fn write_dozer_config_for_running_in_docker_compose(
    mut config: Config,
    connections: &HashMap<String, Connection>,
    dir: &Path,
) -> String {
    let mut host_port_to_container_port = HashMap::new();
    for connection in connections.values() {
        if let Some(service) = &connection.service {
            for port in &service.ports {
                let published = port.published.unwrap_or(port.target);
                if host_port_to_container_port.contains_key(&published) {
                    panic!("Multiple connections are publishing to the same host port: {}. Confilicing connections are being used? Check the config file at {:?}.", port.target, dir.parent());
                }
                host_port_to_container_port.insert(published, port.target);
            }
        }
    }

    let map_port = |port| {
        host_port_to_container_port
            .get(&port)
            .copied()
            .unwrap_or_else(|| panic!("A connection attempts to use port {port} that's not published. Is this a connection that doesn't needs to run in the docker compose? If so, try to remove the connection from the repository"))
    };

    for connection in &mut config.connections {
        let config = connection
            .config
            .as_mut()
            .expect("Connection should always have config");

        match config {
            ConnectionConfig::Postgres(postgres) => {
                let config = postgres.replenish();
                postgres.host = Some(connection.name.clone());
                postgres.port = Some(map_port(config.port as u16) as u32);
            }
            ConnectionConfig::Ethereum(_) => (),
            ConnectionConfig::Grpc(_) => (),
            ConnectionConfig::Snowflake(_) => {
                todo!("Map snowflake host and port")
            }
            ConnectionConfig::Kafka(_) => {
                todo!("Map kafka host and port")
            }
            ConnectionConfig::S3Storage(_) => {}
            ConnectionConfig::LocalStorage(_) => {}
            ConnectionConfig::DeltaLake(_) => {}
        }
    }

    let config_path = dir.join(DEFAULT_CONFIG_PATH);
    let file = File::create(&config_path)
        .unwrap_or_else(|e| panic!("Failed to create file {config_path:?}: {e}"));
    dozer_types::serde_yaml::to_writer(file, &config)
        .unwrap_or_else(|e| panic!("Failed to write dozer config file {config_path:?}: {e}"));
    config_path
        .to_str()
        .unwrap_or_else(|| panic!("Non-UTF8 path: {config_path:?}"))
        .to_string()
}
