pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
    /// data size in disk
    pub(crate) size: u32,
}