use crate::console_helper::get_colored_text;
use crate::errors::{CloudCredentialError, OrchestrationError};
use dozer_types::{
    log::info,
    models::{api_config::ApiConfig, api_endpoint::ApiEndpoint, app_config::Config},
    prettytable::{row, Table},
};
use reqwest::Url;
use std::io::{stdin, stdout, Write};
use std::{
    net::{TcpListener, TcpStream},
    time::Duration,
};
use termion::{clear, color, cursor, event::Key, input::TermRead, raw::IntoRawMode};

pub fn validate_config(config: &Config) -> Result<(), OrchestrationError> {
    info!("Home dir: {}", get_colored_text(&config.home_dir, "35"));
    if let Some(api_config) = &config.api {
        print_api_config(api_config)
    }

    validate_endpoints(&config.endpoints)?;

    print_api_endpoints(&config.endpoints);
    Ok(())
}

pub fn validate_endpoints(endpoints: &[ApiEndpoint]) -> Result<(), OrchestrationError> {
    if endpoints.is_empty() {
        return Err(OrchestrationError::EmptyEndpoints);
    }

    Ok(())
}

fn print_api_config(api_config: &ApiConfig) {
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Type", "IP", "Port"]);
    if let Some(rest_config) = &api_config.rest {
        table_parent.add_row(row!["REST", rest_config.host, rest_config.port]);
    }

    if let Some(grpc_config) = &api_config.grpc {
        table_parent.add_row(row!["GRPC", grpc_config.host, grpc_config.port]);
    }
    info!(
        "[API] {}\n{}",
        get_colored_text("Configuration", "35"),
        table_parent
    );
}

pub fn print_api_endpoints(endpoints: &Vec<ApiEndpoint>) {
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Path", "Name"]);
    for endpoint in endpoints {
        table_parent.add_row(row![endpoint.path, endpoint.name]);
    }
    info!(
        "[API] {}\n{}",
        get_colored_text("Endpoints", "35"),
        table_parent
    );
}

pub fn listen_with_timeout(
    address: &str,
    timeout_secs: u64,
) -> Result<TcpStream, CloudCredentialError> {
    let parse_url =
        Url::parse(address).map_err(|e| CloudCredentialError::FailedToParseUrl(Box::new(e)))?;
    let mut address = parse_url
        .host_str()
        .ok_or(CloudCredentialError::FailedToParseUrl(Box::new(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Failed to parse url"),
        )))?
        .to_owned();
    if let Some(port) = parse_url.port() {
        address.push_str(&format!(":{}", port));
    }
    let listener =
        TcpListener::bind(address).map_err(CloudCredentialError::FailedToListenToConfigPort)?;
    listener
        .set_nonblocking(true)
        .map_err(CloudCredentialError::FailedToListenToConfigPort)?;
    let mut stream_opt = None;
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(timeout_secs) && stream_opt.is_none() {
        match listener.accept() {
            Ok((stream, _)) => {
                stream_opt = Some(stream);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => return Err(CloudCredentialError::FailedToListenToConfigPort(e)),
        }
    }

    if let Some(stream) = stream_opt {
        Ok(stream)
    } else {
        Err(CloudCredentialError::FailedToListenToConfigPort(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "Timeout occurred"),
        ))
    }
}

pub fn cli_select_option(options: Vec<&str>) -> &str {
    // Set up terminal
    let stdout = stdout().into_raw_mode().unwrap();
    let mut stdout = stdout.lock();
    write!(stdout, "{}{}", clear::All, cursor::Goto(0, 1)).unwrap();
    stdout.flush().unwrap();

    // Define selected_index and enter loop to handle input
    let mut selected_index = 0;
    loop {
        // Display options
        for (i, option) in options.iter().enumerate() {
            if i == selected_index {
                writeln!(
                    stdout,
                    "{}{}[x]{}{}",
                    color::Fg(color::Red),
                    cursor::Goto(1, (i + 1) as u16),
                    option,
                    color::Fg(color::Reset)
                )
                .unwrap();
            } else {
                writeln!(stdout, "{}[ ]{}", cursor::Goto(1, (i + 1) as u16), option).unwrap();
            }
        }
        stdout.flush().unwrap();

        // Handle input
        for c in stdin().keys() {
            match c.unwrap() {
                Key::Char('\n') => {
                    return options[selected_index];
                }
                Key::Up => {
                    selected_index = selected_index.saturating_sub(1);
                }
                Key::Down => {
                    if selected_index < options.len() - 1 {
                        selected_index += 1;
                    }
                }
                _ => {}
            }

            // Redraw options
            write!(stdout, "{}", cursor::Goto(1, 1)).unwrap();
            for (i, option) in options.iter().enumerate() {
                if i == selected_index {
                    writeln!(
                        stdout,
                        "{}{}[x]{}{}",
                        color::Fg(color::Red),
                        cursor::Goto(1, (i + 1) as u16),
                        option,
                        color::Fg(color::Reset)
                    )
                    .unwrap();
                } else {
                    writeln!(stdout, "{}[ ]{}", cursor::Goto(1, (i + 1) as u16), option).unwrap();
                }
            }
            stdout.flush().unwrap();
        }
    }
}
