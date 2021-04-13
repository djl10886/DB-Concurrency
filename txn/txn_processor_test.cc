
#include "txn/txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"

using namespace std;

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode) {
  switch (mode) {
    case SERIAL:                 return " Serial   ";
    case LOCKING_EXCLUSIVE_ONLY: return " Locking A";
    case LOCKING:                return " Locking B";
    case OCC:                    return " OCC      ";
    case P_OCC:                  return " OCC-P    ";
    case MVCC:                   return " MVCC     ";
    case STRIFE:                 return " Strife   ";
    default:                     return "INVALID MODE";
  }
}

class LoadGen {
 public:
  virtual ~LoadGen() {}
  virtual Txn* NewTxn() = 0;
};

class RMWLoadGen : public LoadGen {
 public:
  RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class TPCCLoadGen : public LoadGen {
 public:
  TPCCLoadGen(int dbsize, double wait_time)
    : dbsize_(dbsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new TPCC(dbsize_, wait_time_);
  }

 private:
  int dbsize_;
  double wait_time_;
};

class YCSBLoadGen : public LoadGen {
 public:
  YCSBLoadGen(int dbsize, int txn_size, char workload_type, double wait_time)
    : dbsize_(dbsize), txn_size_(txn_size), workload_type_(workload_type),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new YCSB(dbsize_, txn_size_, workload_type_, wait_time_);
  }

 private:
  int dbsize_, txn_size_;
  char workload_type_;
  double wait_time_;
};

class RMWLoadGen2 : public LoadGen {
 public:
  RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    // 80% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    if (rand() % 100 < 80)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWLoadGen3 : public LoadGen {
 public:
  RMWLoadGen3(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    if (rand() % 100 < 90)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

void Benchmark(const vector<LoadGen*>& lg, int num_txns) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = num_txns;
  deque<Txn*> doneTxns;

  // For each MODE...
  for (CCMode mode = LOCKING;
      mode <= LOCKING;
      mode = static_cast<CCMode>(mode+1)) {
    // Print out mode name.
    // cout << ModeToString(mode) << flush;

    // For each experiment, run 3 times and get the average.
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      double throughput[2];
      for (uint32 round = 0; round < 2; round++) {

        int txn_count = 0;

        // Create TxnProcessor in next mode.
        TxnProcessor* p;
        if (mode == STRIFE)
          p = new TxnProcessor(mode, 27, 0.2);
        else
          p = new TxnProcessor(mode);

        // Record start time.
        double start = GetTime();
        // Start specified number of txns running.
        for (int i = 0; i < active_txns; i++)
          p->NewTxnRequest(lg[exp]->NewTxn());
        // Keep active txns at all times for the first full second.
        while (GetTime() < start + 1) {
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
          p->NewTxnRequest(lg[exp]->NewTxn());
        }
        // Wait for all of them to finish.
        for (int i = 0; i < active_txns; i++) {
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
        }

        // double start = GetTime();
        // for (int i=1; i<=num_txns; i++) {
        //   p->NewTxnRequest(lg[exp]->NewTxn());
        // }
        // for (int i=0; i<num_txns; i++) {
        //   Txn *t = p->GetTxnResult();
        //   delete t;
        // }
        // double end = GetTime();
        // throughput[round] = num_txns / (end-start);
        // cout<<"total exec time: "<<(end-start)<<endl<<flush;

        // Record end time.
        double end = GetTime();
        throughput[round] = txn_count / (end-start);

        for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it) {
          // cout<<*it<<endl;
            delete *it;
        }

        doneTxns.clear();
        delete p;
      }

      // Print throughput
      cout << "\t\t" << (throughput[0] + throughput[1]) / 2 << flush;
    }

    
  }
  cout << endl;
}

void Benchmark2(const vector<LoadGen*>& lg, int num_txns) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = num_txns;
  deque<Txn*> doneTxns;

  // For each MODE...
  for (CCMode mode = LOCKING;
      mode <= P_OCC;
      mode = static_cast<CCMode>(mode+1)) {
    // Print out mode name.
    cout << ModeToString(mode) << endl<< flush;

    for (uint32 exp = 0; exp < lg.size(); exp++) {

        int txn_count = 0;

        // Create TxnProcessor in next mode.
        TxnProcessor* p;
        if (mode == STRIFE)
          p = new TxnProcessor(mode, 50, 0.5);
        else
          p = new TxnProcessor(mode);

        // Record start time.
        double start = GetTime();
        // Start specified number of txns running.
        for (int i = 0; i < active_txns; i++)
          p->NewTxnRequest(lg[exp]->NewTxn());
        // keep running txns for a number of seconds and print throughput each second
        double prev=GetTime(), curr, end = start + 3;
        while ((curr = GetTime()) < end) {
          if (curr - prev >= 1) {
            double throughput = txn_count / (curr-start);
            cout<<"throughput: "<<throughput<<endl;
            prev = curr;
          }
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
          p->NewTxnRequest(lg[exp]->NewTxn());
        }
        double throughput = txn_count / (curr-start);
        cout<<"throughput: "<<throughput<<endl;
        // Wait for all of them to finish.
        for (int i = 0; i < active_txns; i++) {
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
        }

        for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it) {
          // cout<<*it<<endl;
            delete *it;
        }

        doneTxns.clear();
        delete p;
    }

    cout << endl;
  }
}

void TestStrife(const vector<LoadGen*>& lg, int num_txns) {
  double max_throughput = 0, best_alpha=0.0;
  int best_k=0;
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    for (int k=5; k<=50; k+=5) {
      // for (double alpha = 0.1; alpha <= 0.91; alpha+=0.1) {
        deque<Txn*> doneTxns;
        int txn_count=0;
        TxnProcessor *p = new TxnProcessor(STRIFE, k, 0.2);
        // int num_txns = 1000;
        double start = GetTime();
        for (int i = 0; i < num_txns; i++)
          p->NewTxnRequest(lg[exp]->NewTxn());
        // Keep active txns at all times for the first full second.
        while (GetTime() < start + 1) {
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
          p->NewTxnRequest(lg[exp]->NewTxn());
        }
        // Wait for all of them to finish.
        for (int i = 0; i < num_txns; i++) {
          Txn* txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
        }
        double end = GetTime();
        double throughput = txn_count/(end-start);
        cout<<"k: "<<k<<", throughput: "<<throughput<<endl<<flush;
        if (throughput > max_throughput) {
          max_throughput = throughput;
          best_k = k;
          // best_alpha = alpha;
        }
        delete p;
      // }
    }
  }
  cout<<"best k: "<<best_k<<", best throughput: "<<max_throughput<<endl<<flush;
}

int main(int argc, char** argv) {
  // cout << "\t\t\t    Average Transaction Duration" << endl;
  // cout << "\t\t0.1ms\t\t1ms\t\t10ms";
  // cout << endl;

  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(7, &cs);
  int ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }

  cout << "workload\t\tLocking\t\tOCC\t\tOCC-P\t\tMVCC\t\tStrife"<<endl;

  // TxnProcessor *p = new TxnProcessor(OCC, 50, 0.5);
  // int num_txns = 5000;
  // double start = GetTime();
  // for (int i=1; i<=num_txns; i++) {
  //   Txn *t = new TPCC(1000000, 0, 5, 0.0001);
  //   p->NewTxnRequest(t);
  // }
  // for (int i=0; i<num_txns; i++) {
  //   // cout<<"reached here"<<endl;
  //   Txn *t = p->GetTxnResult();
  //   delete t;
  // }
  // double end = GetTime();
  // // cout<<"total time: "<<(end-start)<<endl;
  // // cout<<"processing time: "<<p->processing_time<<endl;
  // cout<<"throughput: "<<num_txns/(end-start)<<endl<<flush;
  // delete p;


  vector<LoadGen*> lg;

  // cout << "Low contention read-only (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));

  // lg.push_back(new TPCCLoadGen(1000000, 0.0001));
  // cout<<"TPCC";
  // // Benchmark(lg, 15000);
  // TestStrife(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  // cout<<"write-only";
  // Benchmark(lg, 5000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();







  // vector<LoadGen*> lg;

  // cout << "'Low contention' Read only (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  // // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  // // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));
  // cout<<"Read-only (5 records)";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "'Low contention' Read only (30 records) " << endl;
  // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.0001));
  // // // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.001));
  // // // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.01));
  // cout<<"Read-only";
  // Benchmark(lg, 5000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "'High contention' Read only (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
  // // // lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
  // // // lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));
  // cout<<"Read-only";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "'High contention' Read only (30 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 30, 0, 0.0001));
  // // lg.push_back(new RMWLoadGen(100, 30, 0, 0.001));
  // // lg.push_back(new RMWLoadGen(100, 30, 0, 0.01));
  // cout<<"Read-only";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "Low contention read-write (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  // // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  // // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

  // cout<<"Write-only (5 records)";
  // Benchmark(lg, 5000);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "Low contention read-write (10 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 0, 30, 0.0001));
  // // // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
  // // // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));
  // cout<<"Write-only";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "High contention read-write (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
  // // // lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
  // // // lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));
  // cout<<"Write-only";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "High contention read-write (10 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 0, 30, 0.0001));
  // // // lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
  // // // lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));
  // cout<<"Write-only";
  // TestStrife(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // // 80% of transactions are READ only transactions and run for the full
  // // transaction duration. The rest are very fast (< 0.1ms), high-contention
  // // updates.

  // lg.push_back(new RMWLoadGen2(1000000, 30, 30, 0.0001));
  // cout<<"Mixed";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "High contention mixed read only/read-write " << endl;
  // lg.push_back(new RMWLoadGen2(100, 30, 30, 0.0001));
  // cout<<"Mixed-read";
  // Benchmark(lg, 15000);
  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();


  lg.push_back(new RMWLoadGen3(100, 10, 10, 0.0001));
  cout<<"Mixed-write";
  Benchmark(lg, 15000);
  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

}
