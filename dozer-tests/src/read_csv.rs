pub fn read_csv(folder_name: &str, name: &str) -> Result<csv::Reader<std::fs::File>, csv::Error> {
    let current_dir = std::env::current_dir().unwrap();
    let paths = vec![
        current_dir.join(format!("../target/debug/{}-data/{}.csv", folder_name, name)),
        current_dir.join(format!("./target/debug/{}-data/{}.csv", folder_name, name)),
    ];

    let mut err = None;
    for path in paths {
        let rdr = csv::Reader::from_path(path);
        match rdr {
            Ok(rdr) => return Ok(rdr),
            Err(e) => err = Some(Err(e)),
        }
    }
    err.unwrap()
}
