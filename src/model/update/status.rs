use enumflags2::{bitflags, BitFlags};
use once_cell::sync::Lazy;
use serde::de::{MapAccess, Visitor};
use serde::{de, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use strum_macros::{Display, EnumString};

#[bitflags]
#[repr(u8)]
#[derive(
    Debug, Display, EnumString, Copy, Clone, PartialEq, Eq, ToSchema, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum UpdateStep {
    Observation = 0b0001,
    Forecast = 0b0010,
    Alert = 0b0100,
}

pub type UpdateSteps = BitFlags<UpdateStep>;

#[derive(Copy, Clone, PartialEq, Eq, Hash, ToSchema)]
pub enum LocationUpdateStatus {
    InProgress(UpdateSteps),
    Succeeded,
    Failed,
}

impl Default for LocationUpdateStatus {
    fn default() -> Self {
        // Self(Left(Default::default()))
        Self::InProgress(UpdateSteps::default())
    }
}

impl fmt::Debug for LocationUpdateStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded | Self::Failed => write!(f, "{}", self),
            Self::InProgress(completed) => {
                write!(f, "InProgress[{}:{:?}]", COMPLETED_FIELD, completed)
            },
        }
    }
}

impl fmt::Display for LocationUpdateStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded => write!(f, "Completed[{}]", SUCCEEDED),
            Self::Failed => write!(f, "Completed[{}]", FAILED),
            Self::InProgress(completed) => {
                write!(f, "InProgress[{}:{}]", COMPLETED_FIELD, completed)
            },
        }
    }
}

impl LocationUpdateStatus {
    pub const fn succeeded() -> Self {
        Self::Succeeded
    }

    pub const fn failed() -> Self {
        Self::Failed
    }

    pub fn contains(&self, step: UpdateStep) -> bool {
        match self {
            Self::Succeeded | Self::Failed => true,
            Self::InProgress(completed) => completed.contains(step),
        }
    }

    pub fn advance(&mut self, step: UpdateStep) {
        match self {
            Self::Succeeded | Self::Failed => (),
            Self::InProgress(completed) => {
                let new_completed = *completed | step;
                *self = if new_completed.is_all() {
                    Self::succeeded()
                } else {
                    Self::InProgress(new_completed)
                };
            },
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        !self.is_completed()
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        match self {
            Self::Succeeded | Self::Failed => true,
            Self::InProgress(completed) => {
                completed.intersects(UpdateStep::Observation | UpdateStep::Forecast)
            },
        }
    }
}

const STATUS_FIELD: &str = "status";
const COMPLETED_FIELD: &str = "completed";
const SUCCEEDED: &str = "succeeded";
const FAILED: &str = "failed";
const IN_PROGRESS: &str = "in_progress";
const EXPECTED_STATUS: [&str; 3] = [SUCCEEDED, FAILED, IN_PROGRESS];
static EXPECTED_STATUS_REP: Lazy<String> = Lazy::new(|| {
    EXPECTED_STATUS
        .iter()
        .map(|s| format!(r##""{s}""##))
        .collect::<Vec<_>>()
        .join(", ")
});

impl Serialize for LocationUpdateStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        match self {
            Self::Succeeded => {
                map.serialize_entry(STATUS_FIELD, SUCCEEDED)?;
                map.end()
            },

            Self::Failed => {
                map.serialize_entry(STATUS_FIELD, FAILED)?;
                map.end()
            },

            Self::InProgress(completed) => {
                map.serialize_entry(STATUS_FIELD, IN_PROGRESS)?;
                let steps_rep: Vec<_> = completed.iter().collect();
                map.serialize_entry(COMPLETED_FIELD, &steps_rep)?;
                map.end()
            },
        }
    }
}

impl<'de> Deserialize<'de> for LocationUpdateStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Status,
            Completed,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(f, "`{}` or `{}`", STATUS_FIELD, COMPLETED_FIELD)
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            STATUS_FIELD => Ok(Field::Status),
                            COMPLETED_FIELD => Ok(Field::Completed),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct LocationUpdateStatusVisitor;

        impl<'de> Visitor<'de> for LocationUpdateStatusVisitor {
            type Value = LocationUpdateStatus;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("LocationUpdateStatus")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut status = None;
                let mut completed = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Status => {
                            if status.is_some() {
                                return Err(de::Error::duplicate_field(STATUS_FIELD));
                            }
                            status = Some(map.next_value()?);
                        },

                        Field::Completed => {
                            if completed.is_some() {
                                return Err(de::Error::duplicate_field(COMPLETED_FIELD));
                            }
                            completed = Some(map.next_value()?);
                        },
                    }
                }

                let status: String =
                    status.ok_or_else(|| de::Error::missing_field(STATUS_FIELD))?;

                match status.as_str() {
                    SUCCEEDED => Ok(LocationUpdateStatus::succeeded()),
                    FAILED => Ok(LocationUpdateStatus::failed()),
                    IN_PROGRESS => {
                        let steps: Vec<UpdateStep> =
                            completed.ok_or_else(|| de::Error::missing_field(COMPLETED_FIELD))?;
                        let mut wip = LocationUpdateStatus::default();
                        steps.into_iter().for_each(|s| wip.advance(s));
                        Ok(wip)
                    },
                    rep => Err(de::Error::custom(format!(
                        r##"invalid {STATUS_FIELD} value: string "{rep}", expected one of [{expected}]"##,
                        expected = EXPECTED_STATUS_REP.as_str(),
                    ))),
                }
            }
        }

        const FIELDS: &[&str] = &[STATUS_FIELD, COMPLETED_FIELD];
        deserializer.deserialize_map(LocationUpdateStatusVisitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::update::status::{
        LocationUpdateStatus, UpdateStep, COMPLETED_FIELD, FAILED, IN_PROGRESS, STATUS_FIELD,
        SUCCEEDED,
    };
    use claim::*;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_location_update_status_serde_tokens() {
        once_cell::sync::Lazy::force(&crate::setup_tracing::TEST_TRACING);
        let main_span = info_span!("test_location_update_status_serde_tokens");
        let _ = main_span.enter();

        assert_tokens(
            &LocationUpdateStatus::succeeded(),
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(SUCCEEDED),
                Token::MapEnd,
            ],
        );

        assert_tokens(
            &LocationUpdateStatus::failed(),
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(FAILED),
                Token::MapEnd,
            ],
        );

        let mut wip = LocationUpdateStatus::default();
        assert_tokens(
            &wip,
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(IN_PROGRESS),
                Token::Str(COMPLETED_FIELD),
                Token::Seq { len: Some(0) },
                Token::SeqEnd,
                Token::MapEnd,
            ],
        );

        wip.advance(UpdateStep::Forecast);
        assert_tokens(
            &wip,
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(IN_PROGRESS),
                Token::Str(COMPLETED_FIELD),
                Token::Seq { len: Some(1) },
                Token::UnitVariant { name: "UpdateStep", variant: "forecast" },
                Token::SeqEnd,
                Token::MapEnd,
            ],
        );

        wip.advance(UpdateStep::Observation);
        assert_tokens(
            &wip,
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(IN_PROGRESS),
                Token::Str(COMPLETED_FIELD),
                Token::Seq { len: Some(2) },
                Token::UnitVariant { name: "UpdateStep", variant: "observation" },
                Token::UnitVariant { name: "UpdateStep", variant: "forecast" },
                Token::SeqEnd,
                Token::MapEnd,
            ],
        );

        wip.advance(UpdateStep::Alert);
        assert_tokens(
            &wip,
            &[
                Token::Map { len: None },
                Token::Str(STATUS_FIELD),
                Token::Str(SUCCEEDED),
                Token::MapEnd,
            ],
        );
    }

    #[test]
    fn test_location_update_status_serde_json() {
        once_cell::sync::Lazy::force(&crate::setup_tracing::TEST_TRACING);
        let main_span = info_span!("test_location_update_status_serde_json");
        let _ = main_span.enter();

        let mut wip = LocationUpdateStatus::default();
        let actual = assert_ok!(serde_json::to_string(&wip));
        let expected = r##"{"status":"in_progress","completed":[]}"##;
        assert_eq!(&actual, expected);
        assert_eq!(wip, assert_ok!(serde_json::from_str(expected)));

        wip.advance(UpdateStep::Forecast);
        let actual = assert_ok!(serde_json::to_string(&wip));
        let expected = r##"{"status":"in_progress","completed":["forecast"]}"##;
        assert_eq!(&actual, expected);
        assert_eq!(wip, assert_ok!(serde_json::from_str(expected)));

        wip.advance(UpdateStep::Observation);
        let actual = assert_ok!(serde_json::to_string(&wip));
        let expected = r##"{"status":"in_progress","completed":["observation","forecast"]}"##;
        assert_eq!(&actual, expected);
        assert_eq!(wip, assert_ok!(serde_json::from_str(expected)));

        wip.advance(UpdateStep::Alert);
        let actual = assert_ok!(serde_json::to_string(&wip));
        let expected = r##"{"status":"succeeded"}"##;
        assert_eq!(&actual, expected);
        assert_eq!(wip, assert_ok!(serde_json::from_str(expected)));

        assert_err!(serde_json::from_str::<LocationUpdateStatus>(
            r##"{"status":"foobar"}"##
        ));
    }
}
