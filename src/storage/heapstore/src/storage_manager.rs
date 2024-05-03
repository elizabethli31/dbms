use crate::heap_page::HeapPage;
use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
// use core::panicking::panic;
use std::collections::HashMap;
use std::hash::Hash;
use std::path::{self, Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::{fs, num};
use std::fs::{File, OpenOptions};

pub const STORAGE_DIR: &str = "heapstore";

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_dir: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
    file_paths: Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>>,
    #[serde(skip)]
    files: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let files = self.files.read().unwrap();
        if let Some(hf) = files.get(&container_id) {
            match hf.read_page_from_file(page_id) {
                Ok(page) => Some(page),
                Err(r) => None
            }
        } else {
            None
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: &Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let files = self.files.read().unwrap();
        if let Some(hf) = files.get(&container_id) {
            return hf.write_page_to_file(page);
        } else {
            return Err(CrustyError::CrustyError(format!(
                "ContainerId {} not found in the SM",
                container_id
            )));
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let files = self.files.read().unwrap();
        let hf = files.get(&container_id).unwrap().clone();
        drop(files);
        hf.num_pages()
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let files = self.files.read().unwrap();
        if let Some(hf) = files.get(&container_id) {
            let read = hf.read_count.load(Ordering::Relaxed);
            let write = hf.write_count.load(Ordering::Relaxed);
            drop(files);
            (read, write)
        } else {
            (0, 0)
        }
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_dir as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_dir for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_dir: &Path) -> Self {
        let mut map_path = storage_dir.to_path_buf();
        map_path.push("map.ser");

        let exists = map_path.try_exists().unwrap();

        let files: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        if exists {
            let f = File::open(map_path).expect("could not map file");
            let file_paths: Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>> = serde_cbor::from_reader(f).unwrap();
            let mut lock = files.write().unwrap();
            for (container_id, path) in &*file_paths.read().unwrap() {
                let hf = HeapFile::new(path.to_path_buf(), *container_id).unwrap();
                lock.insert(*container_id, Arc::new(hf));
            }
            drop(lock);
            StorageManager { storage_dir: storage_dir.to_path_buf(), is_temp: false, file_paths, files}
        } else {
            let file_paths: Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>> =
            Arc::new(RwLock::new(HashMap::new()));
            StorageManager { storage_dir: storage_dir.to_path_buf(), is_temp: false, file_paths, files}
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_dir = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_dir);
        let files: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let file_paths: Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        StorageManager { storage_dir, is_temp: true, file_paths, files}
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        let num_pages = self.get_num_pages(container_id);

        let mut completed = false;
        let mut slot_id = 0 as SlotId;
        let mut page_id = 0 as PageId;

        if num_pages > 0 {
            for pid in 0..num_pages {
                let mut page = self.get_page(container_id, pid, tid, Permissions::ReadWrite, false).unwrap();

                if let Some(slot) = page.add_value(&value) {
                    page_id = pid;
                    slot_id = slot;
                    completed = true;
                    self.write_page(container_id, &page, tid);
                    break;
                }
            }
        }

        if !completed {
            page_id = num_pages;
            let mut page = Page::new(page_id);
            slot_id = page.add_value(&value).expect("Couldn't create page");
            self.write_page(container_id, &page, tid);
        }
        return ValueId { 
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: Some(slot_id)
        };
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let container_id = id.container_id;
        if let (Some(pid), Some(slot)) = (id.page_id, id.slot_id) {
            let mut page = self.get_page(container_id, pid, tid, Permissions::ReadWrite, false).unwrap();
            page.delete_value(slot);
            self.write_page(container_id, &page, tid);
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let _ = self.delete_value(id, _tid);
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        match fs::create_dir_all(self.get_storage_path()) {
            Ok(dir) => {}
            Err(e) => {
                return Err(CrustyError::CrustyError(
                    "Error creating directory or already exists".to_string()
                ));
            }
        }
        let mut files = self.files.write().unwrap();
        let mut file_paths = self.file_paths.write().unwrap();
        if files.contains_key(&container_id) {
            return Err(CrustyError::CrustyError(
                "Container already exists".to_string()
            ));
        } else {
            let mut path = self.get_storage_path().to_path_buf();
            path.push(container_id.to_string());
            path.set_extension("hf");
            let hf = HeapFile::new(path.clone(), container_id).unwrap();
            files.insert(container_id, Arc::new(hf));
            file_paths.insert(container_id, Arc::new(path.clone()));
            drop(files);
            drop(file_paths);
            Ok(())
        }   
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut files = self.files.write().unwrap();
        let mut file_paths = self.file_paths.write().unwrap();
        if let Some(hf) = files.get(&container_id) {
            files.remove(&container_id);
            let path_buf = file_paths.get(&container_id).unwrap().clone();
            let path = path_buf.as_ref();
            file_paths.remove(&container_id);
            fs::remove_file(path)?;
            drop(files);
            drop(file_paths);
            Ok(())
        } else {
            Err(CrustyError::CrustyError(
                "Container does not exist".to_string()
            ))
        }
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let files = self.files.read().unwrap();
        let hf = files.get(&container_id).unwrap().clone();
        drop(files);
        return HeapFileIterator::new(tid, hf);
    }

    fn get_iterator_from(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
        start: ValueId,
    ) -> Self::ValIterator {
        let files = self.files.read().unwrap();
        let hf = files.get(&container_id).unwrap().clone();
        drop(files);
        return HeapFileIterator::new_from(tid, hf, start);
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();

        let page = self.get_page(container_id, page_id, tid, perm, false).unwrap();
        if let Some(values) = page.get_value(slot_id) {
            return Ok(values);
        } else {
            Err(CrustyError::CrustyError(
                "Could not find value in page".to_string()
            ))
        }
    }

    fn get_storage_path(&self) -> &Path {
        &self.storage_dir
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_dir.clone())?;
        fs::create_dir_all(self.storage_dir.clone()).unwrap();
        
        let mut files = self.files.write().unwrap();
        files.clear();
        drop(files);

        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        let mut path_name = self.storage_dir.clone();
        path_name.push("map.ser");

        let f = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path_name)
        {
            Ok(f) => f,
            Err(error) => {
                panic!("Couldn't open serialized SM {:?}", path_name)
            }
        };

        let file_paths = self.file_paths.read().unwrap();
        serde_cbor::to_writer(f, &*file_paths).unwrap();
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_dir);
            let remove_all = fs::remove_dir_all(self.storage_dir.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
