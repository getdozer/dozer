#[macro_export]
macro_rules! painted {
    ($a:expr, $b:expr) => {{
        let mut result = String::from("\u{1b}[");
        result.push_str($b);
        result.push_str(";1m"); // End token.
        result.push_str($a);
        result.push_str("\u{1b}[0m");
        result
    }};
}

pub const RED: &str = "31";
pub const GREEN: &str = "32";
pub const PURPLE: &str = "35";
pub const YELLOW: &str = "136";
pub const GREY: &str = "250";
