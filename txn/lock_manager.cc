
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  //
  // Implement this method!

  // vector<Txn*> owners;
  // LockMode status = LockManagerA::Status(key, &owners);
  LockRequest *new_request = new LockRequest(EXCLUSIVE, txn);
  
  deque<LockRequest> *lock_requests;
  if (lock_table_[key])
		lock_requests = lock_table_[key];
  else
		lock_requests = new std::deque<LockRequest>;
		
	LockMode status = lock_requests->size() > 0 ? EXCLUSIVE : UNLOCKED;

  lock_requests->push_back(*new_request);
  lock_table_[key] = lock_requests;

  if (status != UNLOCKED) {
		txn_waits_[txn] += 1;
		return false;
  }
  return true;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  //
  // Implement this method!

  if (lock_table_[key]) {
	if (lock_table_[key]->size() > 0) {
		deque<LockRequest> *lock_requests = lock_table_[key];
		// first request is the one being released
		if (lock_requests->at(0).txn_ == txn) {
			lock_requests->pop_front();

			if (lock_requests->size() > 0) {
				LockRequest *next_req = &(lock_requests->at(0));
				txn_waits_[next_req->txn_] -= 1;
				if (txn_waits_[next_req->txn_] == 0)
					ready_txns_->push_back(next_req->txn_);
			}
		} else { // removing from somewhere in the middle of the deque
			deque<LockRequest> *new_deque = new deque<LockRequest>;
			for (uint index=0; index < lock_requests->size(); index++) {
				if (lock_requests->at(index).txn_ != txn)
					new_deque->push_back(lock_requests->at(index));
			}
			lock_table_[key] = new_deque;
			txn_waits_[txn] -= 1;
		}
	}
  }

//  bool was_first = false;
//  if (lock_table_[key]) {
//	// remove txn from deque
//	if (lock_table_[key]->size() > 0) {
//		was_first = lock_table_[key]->at(0).txn_ == txn;
//		std::deque<LockRequest> *new_deque = new std::deque<LockRequest>;
//		for (uint index=0; index < lock_table_[key]->size(); index++) {
//			if (lock_table_[key]->at(index).txn_ != txn) {
//				new_deque->push_back(lock_table_[key]->at(index));
//			}
//		}
//		lock_table_[key] = new_deque;
//	}
//	// grant next request in queue if txn prev held it; if txn has now acquired all locks, add to ready_txns_
//	// decrement number of locks txn is waiting on
//	if (was_first && lock_table_[key]->size() > 0) {
//		Txn *next_txn = lock_table_[key]->at(0).txn_;
//		txn_waits_[next_txn] -= 1;
//		// if num waits is 0 then txn is ready
//		if (txn_waits_[next_txn] == 0) {
//			ready_txns_->push_back(next_txn);
//		}
//	}
//  }
  return;
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  //
  // Implement this method!
  if (owners)
		owners->clear();
  else
		owners = new vector<Txn*>;
  std::deque<LockRequest>* lock_requests = lock_table_[key];
  if (!lock_requests || lock_requests->size() == 0)
		return UNLOCKED;
  owners->push_back(lock_requests->at(0).txn_);
  return EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  //
  // Implement this method!
  // vector<Txn*> owners;
  // LockMode status = LockManagerB::Status(key, &owners);
  LockRequest *new_request = new LockRequest(EXCLUSIVE, txn);

  deque<LockRequest> *lock_requests;
  if (lock_table_[key])
		lock_requests = lock_table_[key];
  else
		lock_requests = new std::deque<LockRequest>;
		
	LockMode status = lock_requests->size() > 0 ? EXCLUSIVE : UNLOCKED;

  lock_requests->push_back(*new_request);
  lock_table_[key] = lock_requests;

  if (status != UNLOCKED) {
		txn_waits_[txn] += 1;
		return false;
  }
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  //
  // Implement this method!
  LockRequest *new_request = new LockRequest(SHARED, txn);
  deque<LockRequest> *lock_requests;
  if (lock_table_[key])
		lock_requests = lock_table_[key];
  else
		lock_requests = new std::deque<LockRequest>;

  lock_requests->push_back(*new_request);
  lock_table_[key] = lock_requests;

  // if there is any exclusive lock for this key, the read lock will be false; true otherwise
  for (uint index=0; index<lock_requests->size(); index++) {
		if (lock_requests->at(index).mode_ == EXCLUSIVE) {
			txn_waits_[txn] += 1;
			return false;
		}
  }
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  //
  // Implement this method!
  
  if (lock_table_[key]) {
  	if (lock_table_[key]->size() > 0) {
  		
  		deque<LockRequest> *lock_requests = lock_table_[key];
  		
  		// if first request is the one being released
  		if (lock_requests->at(0).txn_ == txn) {
  			LockMode prev_mode = lock_requests->at(0).mode_;
  			lock_requests->pop_front();
  			
  			if (lock_requests->size() > 0) {
  				if (prev_mode == EXCLUSIVE) {
  					if (lock_requests->at(0).mode_ == EXCLUSIVE) {
  						LockRequest *next_req = &(lock_requests->at(0));
  						txn_waits_[next_req->txn_] -= 1;
  						if (txn_waits_[next_req->txn_] == 0)
  							ready_txns_->push_back(next_req->txn_);
  					} else if (lock_requests->at(0).mode_ == SHARED) {
  						for (uint index=0; index<lock_requests->size(); index++) {
  							LockRequest *curr_request = &(lock_requests->at(index));
  							if (curr_request->mode_ == EXCLUSIVE)
  								return;
  							else if (curr_request->mode_ == SHARED) {
  								txn_waits_[curr_request->txn_] -= 1;
  								if (txn_waits_[curr_request->txn_] == 0)
  									ready_txns_->push_back(curr_request->txn_);
  							}
  						}
  					}
  				} else if (prev_mode == SHARED) {
  					if (lock_requests->at(0).mode_ == EXCLUSIVE) {
  						LockRequest *next_req = &(lock_requests->at(0));
  						txn_waits_[next_req->txn_] -= 1;
  						if (txn_waits_[next_req->txn_] == 0)
  							ready_txns_->push_back(next_req->txn_);
  					}
  				}
  			}
  		} else { // release key is somewhere in middle of deque
  			deque<LockRequest> *new_deque = new deque<LockRequest>;
  			LockMode prev_mode = UNLOCKED;
  			
  			for (uint index=0; index<lock_requests->size(); index++) {
  				LockRequest *curr_request = &(lock_requests->at(index));
  				if (curr_request->txn_ != txn)
  					new_deque->push_back((*curr_request));
  				else
  					prev_mode = curr_request->mode_;
  			}
  			vector<Txn*> prev_owners;
			  Status(key, &prev_owners);
			  // construct map of previous owners
			  unordered_map<Txn*, bool> previous_owners;
			  for (size_t i =0; i<prev_owners.size(); i++)
					previous_owners[prev_owners[i]] = true;
				lock_table_[key] = new_deque;
				
				if (prev_mode == EXCLUSIVE) {
					txn_waits_[txn] -= 1;
					for (uint index=0; index<new_deque->size(); index++) {
						LockRequest *curr_request = &(new_deque->at(index));
						if (curr_request->mode_ == EXCLUSIVE)
							return;
						if (!previous_owners[curr_request->txn_]) {
							txn_waits_[curr_request->txn_] -= 1;
							if (txn_waits_[curr_request->txn_] == 0)
								ready_txns_->push_back(curr_request->txn_);
						}
					}
				} else if (prev_mode == SHARED) {
					if (!previous_owners[txn])
						txn_waits_[txn] -= 1;
				}
  		}
  	}
  }
  
  
  
  
  
  // vector<Txn*> prev_owners;
  // LockManagerB::Status(key, &prev_owners);
  // // construct map of previous owners
  // unordered_map<Txn*, bool> previous_owners;
  // for (size_t i =0; i<prev_owners.size(); i++)
		// previous_owners[prev_owners[i]] = true;
	
  // LockMode prev_mode = UNLOCKED;
  // if (lock_table_[key]) {
		// if (lock_table_[key]->size() > 0) {
			
		// 	deque<LockRequest> *lock_requests = lock_table_[key];
		// 	// if key to remove is first
		// 	if (lock_requests->at(0).txn_ == txn) {
		// 		prev_mode = lock_requests->at(0).mode_;
		// 		lock_requests->pop_front();
				
		// 		if (lock_requests->size() > 0) {
		// 			LockRequest *next_req = &(lock_table_[key]->at(0));
		// 			if (prev_mode == EXCLUSIVE) {
		// 				if (next_req->mode_ == EXCLUSIVE) {
		// 					txn_waits_[next_req->txn_] -= 1;
		// 					if (txn_waits_[next_req->txn_] == 0)
		// 						ready_txns_->push_back(next_req->txn_);
		// 				} else if (next_req->mode_ == SHARED) {
		// 					for (uint index=0; index < lock_requests->size(); index++) {
		// 						LockRequest *curr_request = &(lock_requests->at(index));
		// 						if (curr_request->mode_ == EXCLUSIVE)
		// 							return;
		// 						txn_waits_[curr_request->txn_] -= 1;
		// 						if (txn_waits_[curr_request->txn_] == 0)
		// 							ready_txns_->push_back(curr_request->txn_);
		// 					}
		// 				}
		// 			} else if (prev_mode == SHARED) {
		// 				if (next_req->mode_ == EXCLUSIVE) {
		// 					txn_waits_[next_req->txn_] -= 1;
		// 					if (txn_waits_[next_req->txn_] <= 0)
		// 						ready_txns_->push_back(next_req->txn_);
		// 				}
		// 			}
		// 		}
		// 		return;
		// 	}
			
			
			
			
		// 	std::deque<LockRequest> *new_deque = new std::deque<LockRequest>;
		// 	std::deque<LockRequest> *curr_deque = lock_table_[key];

		// 	for (uint index=0; index<curr_deque->size(); index++) {
		// 		if (curr_deque->at(index).txn_ != txn)
		// 			new_deque->push_back(curr_deque->at(index));
		// 		else
		// 			prev_mode = curr_deque->at(index).mode_;
		// 	}
		// 	lock_table_[key] = new_deque;
			
		// 	if (prev_mode == EXCLUSIVE) {
				
		// 		for (uint index = 0; index < lock_table_[key]->size(); index++) {
		// 			LockRequest *curr_request = &(lock_table_[key]->at(index));
		// 			if (curr_request->mode_ == EXCLUSIVE)
		// 				return;
		// 			else if (curr_request->mode_ == SHARED) {
		// 				if (!previous_owners[curr_request->txn_]) {
		// 					txn_waits_[curr_request->txn_] -= 1;
		// 					if (txn_waits_[curr_request->txn_])
		// 						ready_txns_->push_back(curr_request->txn_);
		// 				}
		// 			}
		// 		}
		// 	}
		// }
		// // if (lock_table_[key]->size() > 0) {
		// // 	LockRequest *next_req = &(lock_table_[key]->at(0));
		// // 	if (prev_mode == EXCLUSIVE) {
				
		// // 		for (uint index = 0; index < lock_table_[key]->size(); index++) {
		// // 			LockRequest *curr_request = &(lock_table_[key]->at(index));
		// // 			if (curr_request->mode_ == EXCLUSIVE)
		// // 				return;
		// // 			else if (curr_request->mode_ == SHARED) {
		// // 				if (!previous_owners[curr_request->txn_]) {
		// // 					txn_waits_[curr_request->txn_] -= 1;
		// // 					if (txn_waits_[curr_request->txn_])
		// // 						ready_txns_->push_back(curr_request->txn_);
		// // 				}
		// // 			}
		// // 		}
		// // 	}
				
				
				
				
		// // 	// grant to all successive shared or next exclusive
		// // 		if (next_req->mode_ == EXCLUSIVE && !previous_owners[next_req->txn_]) {
		// // 			txn_waits_[next_req->txn_] -= 1;
		// // 			if (txn_waits_[next_req->txn_] == 0) {
		// // 				ready_txns_->push_back(next_req->txn_);
		// // 			}
		// // 		} else if (next_req->mode_ == SHARED) {
		// // 			uint index = 0;
		// // 			while (index < lock_table_[key]->size()) {
		// // 				next_req = &(lock_table_[key]->at(index));
		// // 				if (next_req->mode_ == EXCLUSIVE)
		// // 					break;
		// // 				else if (next_req->mode_ == SHARED && !previous_owners[next_req->txn_]) {
		// // 					txn_waits_[next_req->txn_] -= 1;
		// // 					if (txn_waits_[next_req->txn_]==0)
		// // 						ready_txns_->push_back(next_req->txn_);
		// // 				}
		// // 				index++;
		// // 			}
		// // 		}
		// // 	} else if (prev_mode == SHARED) {
		// // 	// only grant next request if its exclusive
		// // 		if (next_req->mode_ == EXCLUSIVE) {
		// // 			txn_waits_[next_req->txn_] -= 1;
		// // 			if (txn_waits_[next_req->txn_]==0)
		// // 				ready_txns_->push_back(next_req->txn_);
		// // 		}
		// // 	}
		// // }
  // }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  //
  // Implement this method!
  if (owners)
		owners->clear();
  else
		owners = new vector<Txn*>;
  std::deque<LockRequest> *lock_requests = lock_table_[key];
  if (!lock_requests || lock_requests->size() == 0) {
		return UNLOCKED;
  }
  LockMode first_lock = lock_requests->at(0).mode_;
  if (first_lock == EXCLUSIVE) {
		owners->push_back(lock_requests->at(0).txn_);
		return EXCLUSIVE;
  }
  for (uint index=0; index < lock_requests->size(); index++) {
		LockRequest req = lock_requests->at(index);
		if (req.mode_ == EXCLUSIVE)
			return SHARED;
		else {
			owners->push_back(req.txn_);
		}
  }
  return SHARED;
}
