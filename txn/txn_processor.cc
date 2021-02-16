
#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

using namespace std;

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else if (mode_ == STRIFE) {
    storage_ = new StrifeStorage();
    k = 23;
    alpha = 0.2;
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
  
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
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

void TxnProcessor::StrifePrepare(deque<Txn*> *batch, atomic_int *counter) {
  int size=batch->size();
  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    for (auto it = t->writeset_.begin(); it != t->writeset_.end(); ++it) {
      Cluster *c = storage_->getCluster(*it);
      c->parent = c;
      c->count = 0;
    }
    for (auto it = t->readset_.begin(); it != t->readset_.end(); ++it) {
      Cluster *c = storage_->getCluster(*it);
      c->parent = c;
      c->count = 0;
    }
  }
  (*counter)++;
}

void TxnProcessor::StrifeExecuteBatch(deque<Txn*> *batch) {

  //split batch into equal chunks for prepare, fuse, allocate steps
  deque<Txn*> chunks[THREAD_COUNT];
  int size = batch->size();
  for (int i=0; i<size; i++) {
    Txn *t = batch->at(i);
    chunks[i%THREAD_COUNT].push_back(t);
  }

  atomic_int counter(0);
  //PREPARE
  for (int i=0; i<THREAD_COUNT; i++) {
    tp_.RunTask(new Method<TxnProcessor, void, deque<Txn*>*, atomic_int*>(
            this,
            &TxnProcessor::StrifePrepare,
            &(chunks[i]), &counter));
  }

  while (counter < THREAD_COUNT);
  //SPOT


  //FUSE


  //MERGE


  //ALLOCATE



  while (!batch->empty()) {
    Txn *t = batch->front();
    batch->pop_front();
    txn_results_.Push(t);
  }

}

void TxnProcessor::RunStrifeScheduler() {
  // cout<<"started strife"<<endl;
  deque<Txn*> batch;
  double duration = 0.0001;
  double startTime = GetTime();

  Txn *txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      batch.push_back(txn);
    }
    if (batch.size()>0 && GetTime() - startTime >= duration) {
      StrifeExecuteBatch(&batch);
      startTime = GetTime();
    }
  }
  
}

