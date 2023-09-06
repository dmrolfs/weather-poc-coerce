use super::state::LocationUpdateStatus;
use crate::model::LocationZoneCode;
use multi_index_map::MultiIndexMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(MultiIndexMap, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocationStatus {
    #[multi_index(hashed_unique)]
    pub zone: LocationZoneCode,

    #[multi_index(hashed_non_unique)]
    pub status: LocationUpdateStatus,
}

impl fmt::Debug for MultiIndexLocationStatusMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(_, ls)| (&ls.zone, ls.status)))
            .finish()
    }
}

impl Clone for MultiIndexLocationStatusMap {
    fn clone(&self) -> Self {
        let mut result = MultiIndexLocationStatusMap::with_capacity(self.len());
        for (_, ls) in self.iter() {
            result.insert(ls.clone());
        }
        result
    }
}

impl PartialEq for MultiIndexLocationStatusMap {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        let lhs_iter = self.iter();
        let mut rhs_iter = other.iter();

        for lhs in lhs_iter {
            match rhs_iter.next() {
                None => return false,
                Some(rhs) if lhs.1 != rhs.1 => return false,
                Some(_) => {},
            }
        }

        true
    }
}

impl Serialize for MultiIndexLocationStatusMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (_, ls) in self.iter() {
            map.serialize_entry(&ls.zone, &ls.status)?;
        }
        map.end()
    }
}

struct MultiIndexLocationStatusMapVisitor;

impl<'de> Visitor<'de> for MultiIndexLocationStatusMapVisitor {
    type Value = MultiIndexLocationStatusMap;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("multi-index location status map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = MultiIndexLocationStatusMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((zone, status)) =
            access.next_entry::<LocationZoneCode, LocationUpdateStatus>()?
        {
            map.insert(LocationStatus { zone, status });
        }

        Ok(map)
    }
}

impl<'de> Deserialize<'de> for MultiIndexLocationStatusMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MultiIndexLocationStatusMapVisitor)
    }
}
