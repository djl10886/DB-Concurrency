
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
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

void Benchmark(const vector<LoadGen*>& lg) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = 100;
  deque<Txn*> doneTxns;

  // For each MODE...
  for (CCMode mode = STRIFE;
      mode <= STRIFE;
      mode = static_cast<CCMode>(mode+1)) {
    // Print out mode name.
    cout << ModeToString(mode) << flush;

    // For each experiment, run 3 times and get the average.
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      double throughput[2];
      for (uint32 round = 0; round < 2; round++) {

        int txn_count = 0;

        // Create TxnProcessor in next mode.
        TxnProcessor* p = new TxnProcessor(mode);

        // Record start time.
        double start = GetTime();

        // Start specified number of txns running.
        for (int i = 0; i < active_txns; i++)
          p->NewTxnRequest(lg[exp]->NewTxn());

        // Keep 100 active txns at all times for the first full second.
        while (GetTime() < start + 0.5) {
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

        // Record end time.
        double end = GetTime();

        throughput[round] = txn_count / (end-start);

        for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it) {
            delete *it;
        }

        doneTxns.clear();
        delete p;
      }

      // Print throughput
      cout << "\t" << (throughput[0] + throughput[1]) / 2 << "\t" << flush;
    }

    cout << endl;
  }
}

int main(int argc, char** argv) {
  cout << "\t\t\t    Average Transaction Duration" << endl;
  cout << "\t\t0.1ms\t\t1ms\t\t10ms";
  cout << endl;

  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(7, &cs);
  int ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }



  TxnProcessor *p = new TxnProcessor(STRIFE);

  Txn *t1 = new RMW(0, 0, 0, 0.0001);
  t1->writeset_.insert(10);
  t1->writeset_.insert(20);
  t1->writeset_.insert(30);
  t1->writeset_.insert(40);

  Txn *t2 = new RMW(0, 0, 0, 0.0001);
  t2->writeset_.insert(60);
  t2->writeset_.insert(70);
  t2->writeset_.insert(80);
  t2->writeset_.insert(90);

  Txn *t3 = new RMW(0, 0, 0, 0.0001);
  t3->writeset_.insert(30);
  t3->writeset_.insert(40);
  t3->writeset_.insert(50);
  t3->writeset_.insert(60);

  p->NewTxnRequest(t1);
  p->NewTxnRequest(t2);
  p->NewTxnRequest(t3);
  t1 = p->GetTxnResult();
  t2 = p->GetTxnResult();
  t3 = p->GetTxnResult();

  delete t1;
  delete t2;
  delete t3;
  delete p;


  // vector<LoadGen*> lg;

  // cout << "Low contention read-write (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  // // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  // // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];

  // lg.clear();











  // vector<LoadGen*> lg;

  // cout << "'Low contention' Read only (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];

  // lg.clear();

  // cout << "'Low contention' Read only (30 records) " << endl;
  // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.0001));
  // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.001));
  // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "'High contention' Read only (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
  // lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
  // lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "'High contention' Read only (30 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 30, 0, 0.0001));
  // lg.push_back(new RMWLoadGen(100, 30, 0, 0.001));
  // lg.push_back(new RMWLoadGen(100, 30, 0, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "Low contention read-write (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "Low contention read-write (10 records)" << endl;
  // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
  // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
  // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "High contention read-write (5 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
  // lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
  // lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // cout << "High contention read-write (10 records)" << endl;
  // lg.push_back(new RMWLoadGen(100, 0, 10, 0.0001));
  // lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
  // lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();

  // // 80% of transactions are READ only transactions and run for the full
  // // transaction duration. The rest are very fast (< 0.1ms), high-contention
  // // updates.
  // cout << "High contention mixed read only/read-write " << endl;
  // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.0001));
  // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.001));
  // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.01));

  // Benchmark(lg);

  // for (uint32 i = 0; i < lg.size(); i++)
  //   delete lg[i];
  // lg.clear();
}
