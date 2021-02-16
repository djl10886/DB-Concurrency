#ifndef _CLUSTER_H_
#define _CLUSTER_H_

#include <cstdint>
#include <atomic>

using namespace std;

typedef struct cluster {
    Value value;
    struct cluster *parent;
    uintptr_t address;
    atomic_int count;
    int id;
    Mutex lock;
} Cluster;

#endif
