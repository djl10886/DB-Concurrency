#include "txn/strife_storage.h"



bool StrifeStorage::Read(Key key, Value* result, int txn_unique_id) {
    if (clusters_.count(key)) {
        *result = clusters_[key]->value;
        return true;
    } else
        return false;
}

void StrifeStorage::Write(Key key, Value value, int txn_unique_id) {
    clusters_[key]->value = value;
}

Cluster* StrifeStorage::getCluster(Key key) {
    return clusters_[key];
}

uintptr_t StrifeStorage::getM() {
    return M;
}

void StrifeStorage::InitStorage() {
    for (int i = 0; i < 1000011;i++) {
        Cluster *c = new Cluster();
        c->value = i;
        c->parent = c;
        c->address = reinterpret_cast<uintptr_t>(c);
        if (c->address > M)
            M = c->address;
        c->count = 0;
        clusters_[i] = c;
    } 
}

StrifeStorage::~StrifeStorage() {
    for (auto it = clusters_.begin(); it != clusters_.end(); ++it) {
        delete it->second;
    }
    clusters_.clear();
}


