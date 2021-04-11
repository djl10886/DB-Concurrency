
#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>
#include <random>
#include <utility>

#include "txn/lock_manager.h"

using namespace std;

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 7

typedef struct handler {
  TxnProcessor *p;
  vector<Txn*> *batch;
} StrifeHandler;

TxnProcessor::TxnProcessor(CCMode mode, int k_, double alpha_)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1), k(k_), alpha(alpha_) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING || mode_ == STRIFE)
    lm_ = new LockManagerB(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else if (mode_ == STRIFE) {
    storage_ = new StrifeStorage();
  } else {
    storage_ = new Storage();
  }
  
  storage_->InitStorage();

  if (mode_ == STRIFE)
    M = storage_->getM();


  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  for (int i = 0;i < 7;i++) {
    CPU_SET(i, &cpuset);
  } 
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
  
  // if (mode_ == STRIFE) {
  //   pthread_t scheduler_2;
  //   pthread_create(&scheduler_2, &attr, StartStrife, reinterpret_cast<void*>(this));
  // }
  
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING || mode_ == STRIFE)
    delete lm_;
    
  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  // cout<<"started getting result"<<endl;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  // cout<<"finished getting result"<<endl;
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler(); break;
    case STRIFE:                 RunStrifeScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  Txn* txn;
  while(tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
    // handle completed txns
    while (completed_txns_.Pop(&txn)) {
      // handle read set
      bool validation_failed = false;
      for (set<Key>::iterator it = txn->readset_.begin();
         it != txn->readset_.end(); ++it) {
           if (storage_->Timestamp(*it) > txn->occ_start_time_) {
             validation_failed = true;
           }
      }
      
      // handle write set
      if (!validation_failed) {
      for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
         if (storage_->Timestamp(*it) > txn->occ_start_time_) {
             validation_failed = true;
           }
      }
	}
      
      if (validation_failed) {
        // cleanup txn
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        
        // restart txn
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      } else {
        ApplyWrites(txn);
        // mark as committed
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      }
    }
  }
}

void TxnProcessor::ExecuteTxnParallel(Txn *txn) {
  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();
  
  //critical section
  active_set_mutex_.Lock();
  set<Txn*> active_set_copy = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
  
  //validation phase
  bool validation_failed = false;
  // handle read set
  for (set<Key>::iterator it = txn->readset_.begin();
     it != txn->readset_.end(); ++it) {
     if (storage_->Timestamp(*it) > txn->occ_start_time_) {
       validation_failed = true;
     }
  }
  if (!validation_failed) {   
  // handle write set
  for (set<Key>::iterator it = txn->writeset_.begin();
   it != txn->writeset_.end(); ++it) {
     if (storage_->Timestamp(*it) > txn->occ_start_time_) {
       validation_failed = true;
     }
  }
  }
  
  if (!validation_failed) {
  for (set<Txn*>::iterator it = active_set_copy.begin();
      it != active_set_copy.end(); ++it) {
	Txn *t = *it;
	for (set<Key>::iterator it2 = txn->writeset_.begin();
		it2 != txn->writeset_.end(); ++it2) {
		if (t->writeset_.count(*it2) > 0)
			validation_failed = true;
	}
	if (validation_failed)
		break;

	for (set<Key>::iterator it3 = txn->readset_.begin();
		it3 != txn->readset_.end(); ++it3) {
		if (t->writeset_.count(*it3) > 0)
			validation_failed = true;
	}
	if (validation_failed)
		break;
//        if (ReadWriteSetsIntersect(txn, *it))
//          validation_failed = true;
      }
  }

  if (!validation_failed) {
    ApplyWrites(txn);
    active_set_.Erase(txn);
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);
  } else if (validation_failed) {
    active_set_.Erase(txn);
    // cleanup
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
    // restart
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  Txn *txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxnParallel,
            txn));
    }
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn *txn) {
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    if (storage_->Read(*it, &result, txn->unique_id_))
      txn->reads_[*it] = result;
    storage_->Unlock(*it);
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    if (storage_->Read(*it, &result, txn->unique_id_))
      txn->reads_[*it] = result;
    storage_->Unlock(*it);
  }
  
  txn->Run();
  
  //Acquire all locks for keys in the write_set_
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
        storage_->Lock(*it);
  }
  
  //Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  bool all_passed = true;
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
        if (!storage_->CheckWrite(*it, txn->unique_id_))
          all_passed = false;
  }
  
  if (all_passed) {
    ApplyWrites(txn);
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
          storage_->Unlock(*it);
    }
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);
  } else {
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
          storage_->Unlock(*it);
    }
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;
    
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
}

void TxnProcessor::RunMVCCScheduler() {
  //
  // Implement this method!
  
  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute. 
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn. 
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  Txn *txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::MVCCExecuteTxn,
            txn));
    }
  }
}

bool CAS(Cluster **p, Cluster* old, Cluster *new_) {
  if (*p != old)
    return false;
  *p = new_;
  return true;
}

void Compress(Cluster *rec, Cluster *root) {
  while (rec->address < root->address) {
    Cluster *child = rec, *parent = rec->parent;
    if (parent->address < root->address) {
      child->lock.Lock();
      CAS(&(child->parent), parent, root);
      child->lock.Unlock();
    }
    rec = rec->parent;
  }
}

Cluster* Find(Cluster *r) {
  Cluster *root = r;
  while (root != root->parent)
    root = root->parent;
  Compress(r, root);
  return root;
}

Cluster* TxnProcessor::Union(Cluster *r1, Cluster *r2) {
  while(true) {
    Cluster *parent = Find(r1);
    Cluster *child = Find(r2);
    if (parent == child)
      return parent;
    else if (parent->address > M and child->address > M)
      return parent;
    
    if (parent->address < child->address) {
      Cluster *temp = parent;
      parent = child;
      child = temp;
    }
    child->lock.Lock();
    if (CAS(&(child->parent), child, parent)) {
      child->lock.Unlock();
      return parent;
    }
    child->lock.Unlock();
  }
}

void cartesian_product(set<Cluster*> *s1, set<Cluster*> *s2, set<pair<Cluster*, Cluster*> > *result) {
  for (auto it = s1->begin(); it != s1->end(); ++it) {
    for (auto it2 = s2->begin(); it2 != s2->end(); ++it2) {
      result->insert(make_pair(*it, *it2));
    }
  }
}

void TxnProcessor::StrifePrepare(vector<Txn*> *batch, atomic_int *counter) {
  int size=batch->size();
  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    for (auto it = t->writeset_.begin(); it != t->writeset_.end(); ++it) {
      Cluster *c = storage_->getCluster(*it);
      c->parent = c;
      c->count = 0;
      c->address = reinterpret_cast<uintptr_t>(c);
    }
    for (auto it = t->readset_.begin(); it != t->readset_.end(); ++it) {
      Cluster *c = storage_->getCluster(*it);
      c->parent = c;
      c->count = 0;
      c->address = reinterpret_cast<uintptr_t>(c);
    }
  }
  (*counter)++;
}

void TxnProcessor::StrifeFuse(vector<Txn*> *batch, atomic_int *counter, atomic_int *count) {
  int size = batch->size();

  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    set<Cluster*> C,S;
    for (auto it=t->writeset_.begin(); it != t->writeset_.end(); ++it) {
      Cluster *root = Find(storage_->getCluster(*it));
      if (root->address > M) {
        S.insert(root);
      } else {
        C.insert(root);
      }
    }
    if (S.size() <= 1) {
      Cluster *c;
      if (S.size() == 0) {
        if (C.size()>0) {
          auto first = C.begin();
          c = *first;
          C.erase(first);
        }
      } else {
        c = *(S.begin());
      }
      for (auto other = C.begin(); other != C.end(); ++other)
        c = Union(c, *other);
      if (c)
        (c->count)++;
    } else {
      set<pair<Cluster*, Cluster*> > SxS;
      cartesian_product(&S, &S, &SxS);
      for (auto it=SxS.begin(); it != SxS.end(); ++it) {
        int c1_id = it->first->id, c2_id = it->second->id;
        (*((count+c1_id*k) + c2_id))++;  // equivalent to doing count[c1_id][c2_id]++
      }
    }
  }

  (*counter)++;
}

void TxnProcessor::StrifeAllocate(vector<Txn*> *batch, atomic_int *counter, unordered_map<Cluster*, AtomicQueue<Txn*> > *worklist, AtomicQueue<Txn*> *residuals) {
  int size = batch->size();
  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    set<Cluster*> C;
    for (auto it=t->writeset_.begin(); it != t->writeset_.end(); ++it) {
      C.insert(Find(storage_->getCluster(*it)));
    }
    for (auto it=t->readset_.begin(); it != t->readset_.end(); ++it) {
      C.insert(Find(storage_->getCluster(*it)));
    }
    if (C.size() == 1) {
      Cluster *c = *(C.begin());
      if (!worklist->count(c)) {
        mutex_.Lock();
        (*worklist)[c].Push(t);
        mutex_.Unlock();
      } else 
        (*worklist)[c].Push(t);
    } else {
      residuals->Push(t);
    }
  }
  (*counter)++;
}

void TxnProcessor::StrifeConflictFree(queue<Txn*> *cluster, atomic_int *counter) {
  // cout<<"started conflict free"<<endl;
  while (!cluster->empty()) {
    Txn *txn = cluster->front();
    cluster->pop();
    ExecuteTxn(txn);
    // Commit/abort txn according to program logic's commit/abort decision.
    if (txn->Status() == COMPLETED_C) {
      ApplyWrites(txn);
      txn->status_ = COMMITTED;
    } else if (txn->Status() == COMPLETED_A) {
      txn->status_ = ABORTED;
    } else {
      // Invalid TxnStatus!
      DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
    }
    completed_txns_.Pop(&txn);
    txn_results_.Push(txn);
  }
  (*counter)++;
}

void TxnProcessor::StrifeResidual(queue<Txn*> *residuals) {
  Txn* txn;
  int txns_remaining = residuals->size();
  while (txns_remaining>0) {

    if (!residuals->empty()) {
      txn = residuals->front();
      residuals->pop();
    // Start processing the next transaction request.
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
            it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
              it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        // mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        // txn_requests_.Push(txn);
        // mutex_.Unlock();
        residuals->push(txn);
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed residual Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
      txns_remaining--;
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::StrifeExecuteBatch(vector<Txn*> *batch) {
  // cout<<"started batch"<<endl;

  //split batch into equal chunks for prepare, fuse, allocate steps
  vector<Txn*> chunks[THREAD_COUNT];
  int size = batch->size();
  // cout<<"batch size: "<<size<<endl;

  // int limit = size/THREAD_COUNT + 1;
  // for (int i=0; i<THREAD_COUNT; i++)
  //   chunks[i].reserve(limit);

  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    chunks[i%THREAD_COUNT].push_back(t);
  }

  double t1 = GetTime();
  atomic_int counter(0); // used to keep track of how many subtasks in the parallel steps have finished

  //PREPARE
  for (int i=0; i<THREAD_COUNT; i++) {
    tp_.RunTask(new Method<TxnProcessor, void, vector<Txn*>*, atomic_int*>(
            this,
            &TxnProcessor::StrifePrepare,
            &(chunks[i]), &counter));
  }

  while (counter < THREAD_COUNT);

  //SPOT
double t2 = GetTime();
  random_device dev;
  mt19937 rng(dev());
  uniform_int_distribution<mt19937::result_type> gen(0,size-1);

  int i=0, addr_counter = 1;
  set<Cluster*> special;

  for (int a=0; a<k; a++) {
    Txn *t = batch->at(gen(rng));
    set<Cluster*> C,S;
    for (auto it=t->writeset_.begin(); it != t->writeset_.end(); ++it) {
      Cluster *root = Find(storage_->getCluster(*it));
      if (root->address > M) {
        S.insert(root);
        break;
      } else {
        C.insert(root);
      }
    }

    if (S.size() == 0 and C.size()>0) {
      auto first = C.begin();
      
      Cluster *c = *first;
      C.erase(first);
      for (auto it=C.begin(); it!=C.end(); ++it)
        c = Union(c, *it);
      c->id = i++;
      (c->count)++;
      c->address = M + (sizeof(Cluster*)*(addr_counter++));
      special.insert(c);
    }
  }
double t3 = GetTime();
  //FUSE
  counter = 0;
  atomic_int count[k][k] = {};
  for (int i=0; i<THREAD_COUNT; i++) {
    tp_.RunTask(new Method<TxnProcessor, void, vector<Txn*>*, atomic_int*, atomic_int*>(
            this,
            &TxnProcessor::StrifeFuse,
            &(chunks[i]), &counter, (atomic_int*)count));
  }

  while (counter < THREAD_COUNT);
double t4 = GetTime();
  //MERGE
  set<pair<Cluster*, Cluster*> > SpecialxSpecial;
  cartesian_product(&special, &special, &SpecialxSpecial);
  for (auto it=SpecialxSpecial.begin(); it != SpecialxSpecial.end(); ++it) {
    Cluster *c1 = it->first, *c2 = it->second;
    int n1 = count[c1->id][c2->id];
    int n2 = c1->count + c2->count + n1;
    if (n1 >= alpha*n2)
      Union(c1, c2);
  }

  // cout<<"The root of 10 is "<<Find(storage_->getCluster(10))->value<<endl;
  // cout<<"The root of 20 is "<<Find(storage_->getCluster(20))->value<<endl;
  // cout<<"The root of 30 is "<<Find(storage_->getCluster(30))->value<<endl;
  // cout<<"The root of 40 is "<<Find(storage_->getCluster(40))->value<<endl;
  // cout<<"The root of 50 is "<<Find(storage_->getCluster(50))->value<<endl;
  // cout<<"The root of 60 is "<<Find(storage_->getCluster(60))->value<<endl;
  // cout<<"The root of 70 is "<<Find(storage_->getCluster(70))->value<<endl;
  // cout<<"The root of 80 is "<<Find(storage_->getCluster(80))->value<<endl;
  // cout<<"The root of 90 is "<<Find(storage_->getCluster(90))->value<<endl;
double t5 = GetTime();
  //ALLOCATE
  counter = 0;
  unordered_map<Cluster*, AtomicQueue<Txn*> > worklist;
  // unordered_map<Cluster*, deque<Txn*> > worklist;
  AtomicQueue<Txn*> residuals;

  for (int i=0; i<THREAD_COUNT; i++) {
    tp_.RunTask(new Method<TxnProcessor, void, vector<Txn*>*, atomic_int*, unordered_map<Cluster*, AtomicQueue<Txn*> > *, AtomicQueue<Txn*> *>(
            this,
            &TxnProcessor::StrifeAllocate,
            &(chunks[i]), &counter, &worklist, &residuals));
  }

  while (counter < THREAD_COUNT);

  double t6 = GetTime();
  //CONFLICT FREE

  
  counter = 0;
  int worklist_size = worklist.size();

  for (auto it=worklist.begin(); it != worklist.end(); ++it) {
    tp_.RunTask(new Method<TxnProcessor, void, queue<Txn*>*, atomic_int*>(
        this,
        &TxnProcessor::StrifeConflictFree,
        &(it->second.queue_), &counter));
  }
  
  while (counter < worklist_size);
  double t7 = GetTime();

  
  //RESIDUALS
  
  StrifeResidual(&(residuals.queue_));
  double t8 = GetTime();
  // processing_time += (t6-t1);
  // prev_batch_finished = true;

  // double prepare = t2-t1;
  // double spot = t3-t2;
  // double fuse = t4-t3;
  // double merge = t5-t4;
  // double allocate = t6-t5;
  // double conflict = t7-t6;
  // double residual = t8-t7;
  // double total = t8-t1;

  // cout<<"prepare: "<<prepare<<endl<<flush;
  // cout<<"spot: "<<spot<<endl<<flush;
  // cout<<"fuse: "<<fuse<<endl<<flush;
  // cout<<"merge: "<<merge<<endl<<flush;
  // cout<<"allocate: "<<allocate<<endl<<flush;
  // cout<<"conflict free: "<<conflict<<endl<<flush;
  // cout<<"residuals: "<<residual<<endl<<flush;
  // cout<<"-----------------"<<endl<<flush;

}

void TxnProcessor::HandleBatches() {
  vector<Txn*> *batch;
  while(tp_.Active()) {
    if (batch_list.Pop(&batch)) {
      StrifeExecuteBatch(batch);
      delete batch;
    }
  }
}

void* TxnProcessor::StartStrife(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->HandleBatches();
  return NULL;
}

void* TxnProcessor::StartBatch(void *arg) {
  StrifeHandler *h = reinterpret_cast<StrifeHandler*>(arg);
  TxnProcessor *p = h->p;
  vector<Txn*> *batch = h->batch;
  p->StrifeExecuteBatch(batch);
  return NULL;
}

void TxnProcessor::RunStrifeScheduler() {

  // vector<Txn*> *batch = new vector<Txn*>();
  // double duration = 0.01, startTime = GetTime();
  // Txn *txn;
  // while (tp_.Active()) {
  //   if (txn_requests_.Pop(&txn))
  //     batch->push_back(txn);
  //   if (batch->size() > 0 and GetTime()-startTime >= duration) {
  //     batch_list.Push(batch);
  //     batch = new vector<Txn*>();
  //     startTime = GetTime();
  //   }
  //   if (prev_batch_finished and batch_list.Size()>0) {
  //     vector<Txn*> *next_batch;
  //     batch_list.Pop(&next_batch);
  //     prev_batch_finished = false;
  //     pthread_t start_batch;
  //     StrifeHandler h;
  //     h.batch = next_batch;
  //     h.p = this;
  //     pthread_create(&start_batch, NULL, StartBatch, reinterpret_cast<void*>(&h));
  //   }
  // }

  // vector<Txn*> *batch = new vector<Txn*>();
  // double duration=0.0001, startTime = GetTime();
  // Txn *txn;
  // while (tp_.Active()) {
  //   if (txn_requests_.Pop(&txn))
  //     batch->push_back(txn);
  //   if (batch->size() > 0 and GetTime() - startTime >= duration) {
  //     batch_list.Push(batch);
  //     startTime = GetTime();
  //     batch = new vector<Txn*>();
  //   }
  // }



  vector<Txn*> batch;
  double duration = 0.01;
  double startTime = GetTime();
  Txn *txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      batch.push_back(txn);
    } 
    // else
    //   StrifeExecuteBatch2(&batch);
    if (batch.size()>0 && GetTime() - startTime >= duration) {
      StrifeExecuteBatch(&batch);
      batch.clear();
      startTime = GetTime();
    }
  }
  
}

