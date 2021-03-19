

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000011;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
  
  deque<Version*>* key_vals = mvcc_data_[key];
  if (!key_vals) // key doesn't exist: no possible values
    return false;
  for (deque<Version*>::iterator it = key_vals->begin();
    it != key_vals->end(); ++it) {
      // versions are sorted in decreasing order, so first version that is less than or equal is most recent
      if ((*it)->version_id_ <= txn_unique_id) {
        *result = (*it)->value_;
        if ((*it)->max_read_id_ < txn_unique_id)
          (*it)->max_read_id_ = txn_unique_id;
        return true;
      }
  }
  return false;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  deque<Version*> *key_vals = mvcc_data_[key];
  for (deque<Version*>::iterator it = key_vals->begin();
    it != key_vals->end(); ++it) {
      if ((*it)->version_id_ <= txn_unique_id) {
	
        if ((*it)->max_read_id_ > txn_unique_id)
          return false;
        else
          return true;
		  }
  }
  return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  // Note that the performance would be much better if you organize the versions in decreasing order.
  
  if (!mvcc_data_[key]) { // key doesn't exist
    mvcc_data_[key] = new std::deque<Version*>;
  }
  deque<Version*> *key_vals = mvcc_data_[key];
  Version *new_version = (Version*)malloc(sizeof(Version));
  new_version->value_ = value;
  new_version->version_id_ = txn_unique_id;
  new_version->max_read_id_ = txn_unique_id;
  
  key_vals->push_front(new_version);
}


