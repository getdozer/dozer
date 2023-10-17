use std::pin::pin;

use clap::{Parser, Subcommand};
use dozer_cache::cache::{
    begin_dump_txn, dump, expression::QueryExpression, CacheManagerOptions, LmdbRoCacheManager,
    LmdbRwCacheManager, RoCache,
};
use dozer_storage::generator::{Generator, IntoGenerator};
use dozer_tracing::Labels;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
struct Cli {
    /// Dozer cache directory.
    cache_dir: String,
    /// The endpoint name.
    endpoint: String,
    /// The build name.
    build: String,
    #[clap(subcommand)]
    command: CacheCommand,
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    /// Counts the number of records in the cache.
    Count,
    /// Dumps the cache to a file.
    Dump {
        /// The path to the file to dump to.
        path: String,
    },
    /// Restores the cache from a file.
    Restore {
        /// The path to the file to restore from.
        path: String,
    },
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    let labels = labels(cli.endpoint.clone(), cli.build.clone());

    match cli.command {
        CacheCommand::Count => {
            let cache_manager = LmdbRoCacheManager::new(CacheManagerOptions {
                path: Some(cli.cache_dir.into()),
                ..Default::default()
            })
            .unwrap();
            let cache = cache_manager
                .open_lmdb_cache(labels.to_non_empty_string().into_owned(), labels)
                .unwrap()
                .unwrap();
            let count = cache.count(&QueryExpression::with_no_limit()).unwrap();
            println!("Count: {}", count);
        }
        CacheCommand::Dump { path } => {
            let cache_manager = LmdbRoCacheManager::new(CacheManagerOptions {
                path: Some(cli.cache_dir.into()),
                ..Default::default()
            })
            .unwrap();
            let cache = &cache_manager
                .open_lmdb_cache(labels.to_non_empty_string().into_owned(), labels)
                .unwrap()
                .unwrap();
            let file = tokio::fs::File::create(path).await.unwrap();
            let mut writer = tokio::io::BufWriter::new(file);

            let txn = &begin_dump_txn(cache).unwrap();
            let generator = |context| async move { dump(cache, txn, &context).await.unwrap() };
            let generator = generator.into_generator();
            for item in pin!(generator).into_iter() {
                let item = item.unwrap();
                writer.write_all(&item).await.unwrap();
            }

            writer.flush().await.unwrap();
        }
        CacheCommand::Restore { path } => {
            let cache_manager = LmdbRwCacheManager::new(CacheManagerOptions {
                path: Some(cli.cache_dir.into()),
                ..Default::default()
            })
            .unwrap();
            let file = tokio::fs::File::open(path).await.unwrap();
            let mut reader = tokio::io::BufReader::new(file);
            cache_manager
                .restore_cache(
                    labels.to_non_empty_string().into_owned(),
                    labels,
                    Default::default(),
                    &mut reader,
                )
                .await
                .unwrap();
        }
    }
}

fn labels(endpoint: String, build: String) -> Labels {
    let mut labels = Labels::default();
    labels.push("endpoint", endpoint);
    labels.push("build", build);
    labels
}
