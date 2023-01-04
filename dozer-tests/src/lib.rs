#[cfg(test)]
mod sql_tests;
#[cfg(test)]
pub use sql_tests::TestFramework;

#[cfg(feature = "mongodb")]
#[cfg(test)]
mod cache_tests;

#[cfg(test)]
mod e2e_tests;

#[cfg(test)]
mod init;

#[cfg(test)]
mod read_csv;

#[cfg(test)]
mod tests;
