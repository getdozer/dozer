use crate::{
    init::init, sql_tests::get_inserts_from_csv, tests::sql::TestInstruction, TestFramework,
};
use dozer_types::tracing::info;

pub fn get_tables() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "actor",
            "CREATE TABLE actor(
                actor_id integer NOT NULL,
                first_name text NOT NULL,
                last_name text,
                last_update text
            )",
        ),
        (
            "film",
            "CREATE TABLE film (
                film_id integer NOT NULL,
                title text NOT NULL,
                description text,
                release_year text,
                language_id integer NOT NULL,
                original_language_id integer,
                rental_duration integer  NOT NULL,
                rental_rate numeric NOT NULL,
                length integer,
                replacement_cost numeric NOT NULL,
                rating TEXT,
                last_update timestamp NOT NULL,
                special_features text
            )",
        ),
        (
            "film_actor",
            "CREATE TABLE film_actor (
                actor_id integer NOT NULL,
                film_id integer NOT NULL,
                last_update timestamp NOT NULL
            );",
        ),
    ]
}

// Pass empty table_names to get the whole list
pub fn setup(table_names: &Vec<&'static str>) -> TestFramework {
    let tables = if table_names.len() == 0 {
        get_tables()
    } else {
        get_tables()
            .into_iter()
            .filter(|(name, _)| table_names.contains(name))
            .collect()
    };
    let framework = TestFramework::default();
    framework
        .source
        .lock()
        .unwrap()
        .create_tables(tables)
        .unwrap();

    framework
}

pub fn get_sample_queries() -> Vec<&'static str> {
    vec![
        "select actor_id, first_name, last_name,last_update from actor order by actor_id",
        "select actor_id, first_name, last_name,last_update from actor where actor_id<=5",
        "select actor_id, TRIM(first_name) from actor where actor_id<=5",
        "select actor_id, first_name, last_name,last_update from actor where last_name = 'PIPPO'",
        "select actor_id, first_name as fn, last_name as ln,last_update from actor where last_name = 'PIPPO'",
     //   "select actor_id, first_name, last_name,last_update from actor where last_name IS NULL",
        "select count(actor_id) from actor",
         // "select actor_id, first_name, last_name,last_update from actor where actor_id in (1,5)",
         "select actor_id, first_name, last_name,last_update from actor where first_name='GUINESS'",
         "select actor_id, first_name, last_name,last_update from actor where actor_id<5 and actor_id>2",
         "select actor_id, first_name, last_name,last_update from actor where (actor_id<5 and actor_id>2) or (actor_id>50)",
         "select actor_id from actor order by actor_id",
         "select actor_id, count(actor_id) from actor group by actor_id",
         "select actor_id, count(actor_id) as counts from actor group by actor_id",
    ]
}
pub fn get_sample_ops() -> Vec<(String, String)> {
    vec![
        (
            "actor".to_string(),
            "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (2, 'dario', 'GUINESS','2020-02-15 09:34:33+00')".to_string(),
        ),
        (
            "actor".to_string(),
            "INSERT INTO actor(actor_id,first_name, last_name, last_update) values (3, 'luigi', 'GUINESS','2020-02-15 09:34:33+00')".to_string(),
        ),
        (
            "actor".to_string(),
            "UPDATE actor SET first_name ='sampras' WHERE actor_id=2".to_string(),
        ),
        ("actor".to_string(), "DELETE FROM actor WHERE actor_id=2".to_string()),
    ]
}

pub fn compare_with_sqlite(
    table_names: &Vec<&'static str>,
    queries: Vec<&str>,
    test_name: String,
    test_instruction: TestInstruction,
) {
    init();

    let mut results = vec![];

    for (idx, test) in queries.into_iter().enumerate() {
        let mut framework = setup(&table_names);

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

        let result = framework.compare_with_sqlite(list, test.to_string());

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
