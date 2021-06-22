use rocksdb::{DB, Options};
use bincode;
use std::time::Duration;

pub struct RocksDBSessionStore {
    rocks_db: DB,
}

impl RocksDBSessionStore {

    pub fn new(database_file: &str, ttl: Duration) -> Self {

        let mut options = Options::default();
        options.create_if_missing(true);
        options.optimize_for_point_lookup(5000);
        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);

        let rocks_db =
            DB::open_with_ttl(
                &options,
                database_file,
                ttl
            )
            .unwrap();

        Self { rocks_db }
    }

    pub fn get_session_items(&self, evolving_session_id: &u128) -> Vec<u64> {

        let serialized_session_id = bincode::serialize(&evolving_session_id).unwrap();

        let existing_items = self.rocks_db.get(&serialized_session_id).unwrap();

        let session_items: Vec<u64> = match existing_items {
            Some(serialized_session_items) => {
                bincode::deserialize(&serialized_session_items).unwrap()
            }
            None => Vec::new(),
        };

        session_items
    }

    pub fn update_session_items(&self, evolving_session_id: &u128, session_items: &Vec<u64>) {

        let serialized_session_id = bincode::serialize(evolving_session_id).unwrap();
        let serialized_session_items = bincode::serialize(session_items).unwrap();

        // TODO we can try to merge in the value
        let _ = self.rocks_db.put(&serialized_session_id, &serialized_session_items).unwrap();

    }
}