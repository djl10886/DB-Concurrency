
#ifndef _TXN_TYPES_H_
#define _TXN_TYPES_H_

#include <map>
#include <set>
#include <string>
#include <stdlib.h>
#include <time.h>

#include "txn/txn.h"
#include "txn/zipfian.h"

using namespace std;

// Immediately commits.
class Noop : public Txn {
 public:
  Noop() {}
  virtual void Run() { COMMIT; }

  Noop* clone() const {             // Virtual constructor (copying)
    Noop* clone = new Noop();
    this->CopyTxnInternals(clone);
    return clone;
  }
};

// Reads all keys in the map 'm', if all results correspond to the values in
// the provided map, commits, else aborts.
class Expect : public Txn {
 public:
  Expect(const map<Key, Value>& m) : m_(m) {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
      readset_.insert(it->first);
  }

  Expect* clone() const {             // Virtual constructor (copying)
    Expect* clone = new Expect(map<Key, Value>(m_));
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    Value result;
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) {
      if (!Read(it->first, &result) || result != it->second) {
        ABORT;
      }
    }
    COMMIT;
  }

 private:
  map<Key, Value> m_;
};

// Inserts all pairs in the map 'm'.
class Put : public Txn {
 public:
  Put(const map<Key, Value>& m) : m_(m) {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
      writeset_.insert(it->first);
  }

  Put* clone() const {             // Virtual constructor (copying)
    Put* clone = new Put(map<Key, Value>(m_));
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
      Write(it->first, it->second);
    COMMIT;
  }

 private:
  map<Key, Value> m_;
};

// Read-modify-write transaction.
class RMW : public Txn {
 public:
  explicit RMW(double time = 0) : time_(time) {}
  RMW(const set<Key>& writeset, double time = 0) : time_(time) {
    writeset_ = writeset;
  }
  RMW(const set<Key>& readset, const set<Key>& writeset, double time = 0)
      : time_(time) {
    readset_ = readset;
    writeset_ = writeset;
  }

  // Constructor with randomized read/write sets
  RMW(int dbsize, int readsetsize, int writesetsize, double time = 0)
      : time_(time) {
    // Make sure we can find enough unique keys.
    DCHECK(dbsize >= readsetsize + writesetsize);

    // Find readsetsize unique read keys.
    for (int i = 0; i < readsetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key));
      readset_.insert(key);
    }

    // Find writesetsize unique write keys.
    for (int i = 0; i < writesetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key) || writeset_.count(key));
      writeset_.insert(key);
    }
  }

  RMW* clone() const {             // Virtual constructor (copying)
    RMW* clone = new RMW(time_);
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    Value result;
    // Read everything in readset.
    for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it)
      Read(*it, &result);

    // Increment length of everything in writeset.
    for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end();
         ++it) {
      result = 0;
      Read(*it, &result);
      Write(*it, result + 1);
    }

    // Run while loop to simulate the txn logic(duration is time_).
    double begin = GetTime();
    while (GetTime() - begin < time_) {
      for (int i = 0;i < 1000; i++) {
        int x = 100;
        x = x + 2;
        x = x*x;
      }
    }
    
    COMMIT;
  }

 private:
  double time_;
};


class TPCC : public Txn {
 public:
  explicit TPCC(double time = 0) : time_(time) {}
  TPCC(const set<Key>& writeset, double time = 0) : time_(time) {
    writeset_ = writeset;
  }
  TPCC(const set<Key>& readset, const set<Key>& writeset, double time = 0)
      : time_(time) {
    readset_ = readset;
    writeset_ = writeset;
  }

  // Constructor with randomized read/write sets
  TPCC(int dbsize, double Time = 0)
      : time_(Time), dbsize_(dbsize) {

    // srand(time(NULL));
    int txn_type = rand() % 100 + 1;
    customer_ = dbsize * 0.05, history_ = customer_, oorder_ = customer_;
    item_ = dbsize * 0.17, stock_ = item_, neworder_ = dbsize*0.01, orderline_ = dbsize*0.5;

    if (txn_type >= 1 and txn_type <= 45)
      NewOrder();
    else if (txn_type >= 46 and txn_type <= 88)
      Payment();
    else if (txn_type >= 89 and txn_type <= 92)
      OrderStatus();
    else if (txn_type >= 93 and txn_type <= 96)
      Delivery();
    else
      StockLevel();
  }

  TPCC* clone() const {             // Virtual constructor (copying)
    TPCC* clone = new TPCC(time_);
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    Value result;
    // Read everything in readset.
    for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it)
      Read(*it, &result);

    // Increment length of everything in writeset.
    for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end();
         ++it) {
      result = 0;
      Read(*it, &result);
      Write(*it, result + 1);
    }

    // Run while loop to simulate the txn logic(duration is time_).
    double begin = GetTime();
    while (GetTime() - begin < time_) {
      for (int i = 0;i < 1000; i++) {
        int x = 100;
        x = x + 2;
        x = x*x;
      }
    }
    
    COMMIT;
  }

 private:
  double time_;
  int dbsize_, customer_, history_, oorder_, item_, stock_, neworder_, orderline_;

  void NewOrder() {
    readset_.insert(rand()%customer_);
    readset_.insert(1000010);
    Key district_key = rand() % 10 + dbsize_;
    readset_.insert(district_key);
    writeset_.insert(rand() % neworder_ + (dbsize_*0.27));
    writeset_.insert(district_key-1);
    writeset_.insert(rand() % oorder_ + (dbsize_ * 0.28));
    readset_.insert(rand() % item_ + (dbsize_ * 0.1));
    Key stock_key = rand() % stock_ + (dbsize_ * 0.83);
    readset_.insert(stock_key);
    writeset_.insert(stock_key-1);
    writeset_.insert(rand() % orderline_ + (dbsize_*0.33));
  }

  void Payment() {
    writeset_.insert(1000010);
    // readset_.insert(1000010);
    Key district_key = rand() % 10 + dbsize_;
    writeset_.insert(district_key);
    readset_.insert(district_key-1);
    Key customer_key = rand()%customer_;
    writeset_.insert(customer_key);
    readset_.insert(customer_key+1);
    writeset_.insert(rand()%history_ + (dbsize_*0.05));
  }

  void OrderStatus() {
    readset_.insert(rand()%oorder_ + (dbsize_*0.28));
    int num_orderline = rand() % 11 + 5;
    set<Key> orderline_keys;
    Key orderline_key;
    for (int i=0; i<num_orderline; i++) {
      do {
        orderline_key = rand() % orderline_ + (dbsize_*0.33);
      } while (orderline_keys.count(orderline_key));
      orderline_keys.insert(orderline_key);
    }
    readset_.insert(orderline_keys.begin(), orderline_keys.end());
    readset_.insert(rand()%customer_);
  }

  void Delivery() {
    Key neworder_key = rand() % neworder_ + (dbsize_*0.27);
    writeset_.insert(neworder_key);
    readset_.insert(neworder_key+1);
    Key oorder_key = rand() % oorder_ + (dbsize_*0.28);
    writeset_.insert(oorder_key);
    readset_.insert(oorder_key+1);
    int num_orderline = rand() % 11 + 5;
    set<Key> orderline_keys;
    Key orderline_key;
    for (int i=0; i<num_orderline; i++) {
      do {
        orderline_key = rand() % orderline_ + (dbsize_*0.33);
      } while (orderline_keys.count(orderline_key));
      orderline_keys.insert(orderline_key);
    }
    // readset_.insert(orderline_keys.begin(), orderline_keys.end());
    writeset_.insert(orderline_keys.begin(), orderline_keys.end());
    writeset_.insert(rand() % customer_);
  }

  void StockLevel() {
    readset_.insert(rand() % 10 + dbsize_);
    int num_orderline = rand() % 21 + 1;
    set<Key> orderline_keys;
    Key orderline_key;
    for (int i=0; i<num_orderline; i++) {
      do {
        orderline_key = rand() % orderline_ + (dbsize_*0.33);
      } while (orderline_keys.count(orderline_key));
      orderline_keys.insert(orderline_key);
    }
    readset_.insert(orderline_keys.begin(), orderline_keys.end());
    readset_.insert(rand() % stock_ + (dbsize_*0.83));
  }
};

class YCSB : public Txn {
 public:
  explicit YCSB(double time = 0) : time_(time) {}
  YCSB(const set<Key>& writeset, double time = 0) : time_(time) {
    writeset_ = writeset;
  }
  YCSB(const set<Key>& readset, const set<Key>& writeset, double time = 0)
      : time_(time) {
    readset_ = readset;
    writeset_ = writeset;
  }

  // Constructor with randomized read/write sets
  YCSB(int dbsize, int txn_size, char workload_type, double Time = 0)
      : dbsize_(dbsize), txn_size_(txn_size), workload_type_(workload_type), time_(Time) {
        if (workload_type == 'A')
          WorkloadA();
        else if (workload_type == 'B')
          WorkloadB();
        else if (workload_type == 'C')
          WorkloadC();
        else if (workload_type == 'E')
          WorkloadE();
  }

  TPCC* clone() const {             // Virtual constructor (copying)
    TPCC* clone = new TPCC(time_);
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    Value result;
    // Read everything in readset.
    for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it)
      Read(*it, &result);

    // Increment length of everything in writeset.
    for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end();
         ++it) {
      result = 0;
      Read(*it, &result);
      Write(*it, result + 1);
    }

    // Run while loop to simulate the txn logic(duration is time_).
    double begin = GetTime();
    while (GetTime() - begin < time_) {
      for (int i = 0;i < 1000; i++) {
        int x = 100;
        x = x + 2;
        x = x*x;
      }
    }
    
    COMMIT;
  }

 private:
  int dbsize_, txn_size_;
  char workload_type_;
  double time_;
  
  

  void WorkloadA() {
    int num = rand() % 100 + 1;
    set<Key> *s;
    if (num >= 1 and num <= 50)
      s = &readset_;
    else
      s = &writeset_;
    for (int i=0; i<txn_size_; i++) {
      Key k;
      do {
        k = zipf(0.99, dbsize_);
      } while (s->count(k));
      s->insert(k);
    }
  }

  void WorkloadB() {
    int num = rand() % 100 + 1;
    set<Key> *s;
    if (num >= 1 and num <= 95)
      s = &readset_;
    else
      s = &writeset_;
    for (int i=0; i<txn_size_; i++) {
      Key k;
      do {
        k = zipf(0.99, dbsize_);
      } while (s->count(k));
      s->insert(k);
    }
  }

  void WorkloadC() {
    for (int i=0; i<txn_size_; i++) {
      Key k;
      do {
        k = zipf(0.99, dbsize_);
      } while (readset_.count(k));
      readset_.insert(k);
    }
  }

  void WorkloadE() {
    Key start = zipf(0.99, dbsize_);
    int range = rand() % txn_size_ + 1;
    for (int i=1; i<=range and (int)(start+i)<dbsize_; i++) {
      int num = rand() % 100 + 1;
      if (num >= 1 and num <= 95)
        readset_.insert(start + i);
      else
        writeset_.insert(start + i);
    }
  }
};

#endif  // _TXN_TYPES_H_

