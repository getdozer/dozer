use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RefreshOptionsPeriod {
    #[serde(rename = "every_hour")]
    EveryHour,
    #[serde(rename = "every_day")]
    EveryDay,
    #[serde(rename = "every_week")]
    EveryWeek,
}

impl std::fmt::Display for RefreshOptionsPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                RefreshOptionsPeriod::EveryHour => "every_hour",
                RefreshOptionsPeriod::EveryDay => "every_day",
                RefreshOptionsPeriod::EveryWeek => "every_week",
            }
        )
    }
}

impl std::str::FromStr for RefreshOptionsPeriod {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "every_hour" => Ok(RefreshOptionsPeriod::EveryHour),
            "every_day" => Ok(RefreshOptionsPeriod::EveryDay),
            "every_week" => Ok(RefreshOptionsPeriod::EveryWeek),
            _ => Err(format!("'{}' is not a valid value for RefreshOptionsPeriod", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RefreshOptions {
    #[serde(rename = "after_minute")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_minute: Option<f64>,
    #[serde(rename = "period")]
    pub period: RefreshOptionsPeriod,
}

    