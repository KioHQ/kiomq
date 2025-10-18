pub enum ConnectionTypes<'a> {
    Sync(&'a mut redis::Connection),
    Async(&'a mut deadpool_redis::Connection),
}

impl<'a> From<&'a mut redis::Connection> for ConnectionTypes<'a> {
    fn from(value: &'a mut redis::Connection) -> Self {
        Self::Sync(value)
    }
}
impl<'a> From<&'a mut deadpool_redis::Connection> for ConnectionTypes<'a> {
    fn from(value: &'a mut deadpool_redis::Connection) -> Self {
        Self::Async(value)
    }
}
