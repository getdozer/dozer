use dozer_types::{log::info, log4rs};

use crate::{
    sql_tests::{download, get_inserts_from_csv},
    TestFramework,
};
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() -> TestFramework {
    let tables = vec![
        (
            "actor",
            "CREATE TABLE actor(
                actor_id integer NOT NULL,
                first_name text NOT NULL,
                last_name text NOT NULL,
                last_update text NOT NULL
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

#[test]
#[ignore]
fn nightly_long_test_queries() {
    let path = std::env::current_dir().unwrap().join("log4rs.tests.yaml");
    log4rs::init_file(path, Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    // Downloading test files to target/debug/actor-data
    INIT.call_once(|| {
        download("actor");
    });

    // let names = vec!["actor", "film", "film_actor"];

    let tests = vec![
        "select actor_id, first_name, last_name,last_update from actor order by actor_id",
        "select actor_id, first_name, last_name,last_update from actor where actor_id<=5",
        "select count(actor_id) from actor",
        "select actor_id, first_name, last_name,last_update from actor where actor_id in (1,5)",
        "select actor_id, first_name, last_name,last_update from actor where first_name='GUINESS'",
        "select actor_id, first_name, last_name,last_update from actor where actor_id<5 and actor_id>2",
        "select actor_id, first_name, last_name,last_update from actor where (actor_id<5 and actor_id>2) or (actor_id>200)",
        "select actor_id from actor order by actor_id",
        "select actor_id, count(actor_id) from actor group by actor_id",
    ];

    let mut results = vec![];

    for (idx, test) in tests.into_iter().enumerate() {
        let mut framework = setup();
        let names = vec!["actor"];
        let mut list = vec![];

        for name in names {
            let source = framework.source.lock().unwrap();
            let schema = source.get_schema(name.to_string());
            let inserts = get_inserts_from_csv("actor", name, schema).unwrap();

            for i in inserts {
                list.push((name.to_string(), i));
            }
        }
        let result = framework.run_test(list.clone(), test.to_string());

        let success = match result {
            Ok(true) => "success",
            Ok(false) => "failed",
            Err(e) => {
                info!("---------------------------------------------");
                info!("Error in {}:{}", idx, test);
                info!("{:?}", e);
                info!("");
                "failed"
            }
        };
        results.push((test, success));
    }

    info!("----------------   Report   ------------------");
    info!("");
    for (idx, (test, result)) in results.into_iter().enumerate() {
        info!("{}: {}   {}", idx, test, result);
    }
    info!("");
    info!("---------------------------------------------");
}
