use dozer_types::{log::info, log4rs};

use crate::{
    sql_tests::{download, get_inserts_from_csv},
    TestFramework,
};
use std::sync::Once;

static INIT: Once = Once::new();
fn init() {
    INIT.call_once(|| {
        let path = std::env::current_dir()
            .unwrap()
            // .join("dozer-tests/log4rs.tests.yaml");
            .join("log4rs.tests.yaml");
        log4rs::init_file(path, Default::default())
            .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));
        download("actor");
    });
}
fn setup() -> TestFramework {
    let tables = vec![
        (
            "actor",
            "CREATE TABLE actor(
                actor_id integer NOT NULL,
                first_name text NOT NULL,
                last_name text,
                last_update text
            )",
        ),
        // (
        //     "film",
        //     "CREATE TABLE film (
        //         film_id integer NOT NULL,
        //         title text NOT NULL,
        //         description text,
        //         release_year text,
        //         language_id integer NOT NULL,
        //         original_language_id integer,
        //         rental_duration integer  NOT NULL,
        //         rental_rate numeric NOT NULL,
        //         length integer,
        //         replacement_cost numeric NOT NULL,
        //         rating TEXT,
        //         last_update timestamp NOT NULL,
        //         special_features text
        //     )",
        // ),
        // (
        //     "film_actor",
        //     "CREATE TABLE film_actor (
        //         actor_id integer NOT NULL,
        //         film_id integer NOT NULL,
        //         last_update timestamp NOT NULL
        //     );",
        // ),
    ];

    let framework = TestFramework::default();
    framework
        .source
        .lock()
        .unwrap()
        .create_tables(tables)
        .unwrap();
    framework
}

fn get_queries() -> Vec<&'static str> {
    vec![
        "select actor_id, first_name, last_name,last_update from actor order by actor_id",
        "select actor_id, first_name, last_name,last_update from actor where actor_id<=5",
        "select count(actor_id) from actor",
        "select actor_id, first_name, last_name,last_update from actor where actor_id in (1,5)",
        "select actor_id, first_name, last_name,last_update from actor where first_name='GUINESS'",
        "select actor_id, first_name, last_name,last_update from actor where actor_id<5 and actor_id>2",
        "select actor_id, first_name, last_name,last_update from actor where (actor_id<5 and actor_id>2) or (actor_id>50)",
        "select actor_id from actor order by actor_id",
        "select actor_id, count(actor_id) from actor group by actor_id",
    ]
}

#[test]
#[ignore]
fn nightly_long_init_queries() {
    run_tests(
        "from_csv".to_string(),
        TestInstruction::FromCsv("actor".to_string(), vec!["actor".to_string()]),
    );
}

#[test]
#[ignore]
fn nightly_long_nullable_queries() {
    let list = vec![
            (
                "actor".to_string(),
                "INSERT INTO actor(actor_id,first_name) values (1, 'mario')".to_string(),
            ),
            (
                "actor".to_string(),
                "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (2, 'dario', null, null)".to_string(),
            ),
            (
                "actor".to_string(),
                "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (3, 'luigi', null, null)".to_string(),
            ),
        ];
    run_tests("nullable".to_string(), TestInstruction::List(list));
}

#[test]
#[ignore]
fn nightly_long_changes_queries() {
    let list = vec![
        (
            "actor".to_string(),
            "INSERT INTO actor(actor_id,first_name) values (1, 'mario')".to_string(),
        ),
        (
            "actor".to_string(),
            "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (2, 'dario', null, null)".to_string(),
        ),
        (
            "actor".to_string(),
            "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (3, 'luigi', null, null)".to_string(),
        ),
        (
            "actor".to_string(),
            "UPDATE actor SET first_name ='sampras' WHERE actor_id=1".to_string(),
        ),
        ("actor".to_string(), "DELETE FROM actor WHERE actor_id=1".to_string()),
    ];
    run_tests("changes".to_string(), TestInstruction::List(list));
}

fn run_tests(test_name: String, test_instruction: TestInstruction) {
    init();

    let tests = get_queries();

    let mut results = vec![];

    for (idx, test) in tests.into_iter().enumerate() {
        let mut framework = setup();

        let list = match test_instruction {
            TestInstruction::FromCsv(ref folder_name, ref names) => {
                let mut list = vec![];

                for name in names.clone() {
                    let source = framework.source.lock().unwrap();
                    let schema = source.get_schema(&name);
                    let inserts = get_inserts_from_csv(folder_name, &name.clone(), schema).unwrap();

                    for i in inserts {
                        list.push((name.clone(), i));
                    }
                }
                list
            }
            TestInstruction::List(ref list) => list.clone(),
        };

        let result = framework.run_test(list, test.to_string());

        let success = match result {
            Ok(true) => "success",
            Ok(false) => "failed",
            Err(e) => {
                info!("---------------------------------------------");
                info!("Error in {}:{}", idx, test);
                info!("{:?}", e);
                info!("");
                "error"
            }
        };
        results.push((test, success));
    }

    info!(
        "----------------   Report: {}   ------------------",
        test_name
    );
    info!("");
    for (idx, (test, result)) in results.into_iter().enumerate() {
        info!("{}: {} - {}", idx, result, test);
    }
    info!("");
    info!("---------------------------------------------");
}

#[derive(Clone, Debug)]
enum TestInstruction {
    FromCsv(String, Vec<String>),
    List(Vec<(String, String)>),
}
