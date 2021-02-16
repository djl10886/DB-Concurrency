
#ifndef _STRIFE_STORAGE_H_
#define _STRIFE_STORAGE_H_

#include "txn/storage.h"
#include "txn/cluster.h"

using std::tr1::unordered_map;
using std::deque;
using std::map;

class StrifeStorage : public Storage {
 public:

  virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

  virtual void Write(Key key, Value value, int txn_unique_id = 0);

 
  virtual double Timestamp(Key key) {return 0;}
  
  virtual void InitStorage();
  
  virtual void Lock(Key key) {}
  

  virtual void Unlock(Key key) {}
  

  virtual bool CheckWrite (Key key, int txn_unique_id) {return true;}
  
  virtual ~StrifeStorage();

  virtual Cluster* getCluster(Key key);

  virtual uintptr_t getM();

 private:
 
  friend class TxnProcessor;

  unordered_map<Key, Cluster*> clusters_;

  uintptr_t M=0;

};

#endif  // _MVCC_STORAGE_H_
