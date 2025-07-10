pub mod timestamp_micros {
    /// used for usIn and usOut
    ///
    /// described here https://docs.deribit.com/#response-messages
    use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn to_datetime_utc(ts: u64) -> DateTime<Utc> {
        let seconds = (ts / 10e5 as u64) as i64;
        let nanos = (ts % seconds as u64) * 1000;
        DateTime::from_timestamp(seconds, nanos as u32).unwrap()
    }

    pub fn to_deribit_timestamp(dt: NaiveDateTime) -> u64 {
        let nanos = dt.nanosecond();
        let seconds = dt.timestamp();
        (seconds as u64 * 10e5 as u64) + (nanos as u64 / 1000)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        // microseconds since the Unix epoch
        let timestamp = serde_json::Number::deserialize(deserializer)?
            .as_u64()
            .ok_or_else(|| D::Error::custom("could not coerce to int"))?;

        Ok(to_datetime_utc(timestamp))
    }

    pub fn serialize<S>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_u64(to_deribit_timestamp(dt.naive_utc()))
    }

    #[test]
    fn test_deribit_timestamp() {
        let ts = 1692895628347527;
        let dt = to_datetime_utc(ts);
        assert_eq!(dt.to_string(), "2023-08-24 16:47:08.347527 UTC");
        assert_eq!(ts, to_deribit_timestamp(dt.naive_utc()));
    }
}

pub mod timestamp_millis {
    use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn to_datetime_utc(ts: u64) -> DateTime<Utc> {
        let seconds = (ts / 10e2 as u64) as i64;
        let nanos = (ts % seconds as u64) * 1e6 as u64;
        DateTime::from_timestamp(seconds, nanos as u32).unwrap()
    }

    pub fn to_timestamp(dt: NaiveDateTime) -> u64 {
        let nanos = dt.nanosecond();
        let seconds = dt.and_utc().timestamp();
        (seconds as u64 * 10e2 as u64) + (nanos as u64 / 1e6 as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        // microseconds since the Unix epoch
        let timestamp = serde_json::Number::deserialize(deserializer)?
            .as_u64()
            .ok_or_else(|| D::Error::custom("could not coerce to int"))?;

        Ok(to_datetime_utc(timestamp))
    }

    pub fn serialize<S>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_u64(to_timestamp(dt.naive_utc()))
    }

    #[test]
    fn test_with_timestamp() {
        let ts = 1693234944812;
        let dt = to_datetime_utc(ts);
        assert_eq!(dt.to_string(), "2023-08-28 15:02:24.812 UTC");
        assert_eq!(ts, to_timestamp(dt.naive_utc()));
    }
}
