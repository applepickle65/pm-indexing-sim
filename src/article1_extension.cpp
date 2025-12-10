// article1_extension.cpp
// Extension: mixed read/write workload for sorted vs unsorted B+-tree leaves.
// This is a simplified simulator, not a full B+-tree.

#include <iostream>
#include <random>
#include <chrono>
#include <cstdint>
#include <vector>
#include <string>

struct Stats {
    uint64_t Nw   = 0; // writes
    uint64_t Nclf = 0; // cache line flushes
    uint64_t Nmf  = 0; // memory fences
};

// Very simple "unsorted leaf" model: appends are cheap, reads scan more.
struct UnsortedLeaf {
    std::vector<uint64_t> keys;

    void insert(uint64_t key, Stats& s) {
        // cheap append
        keys.push_back(key);
        s.Nw += 1;
        s.Nclf += 1;
        s.Nmf += 1;
    }

    bool search(uint64_t key, Stats& s) const {
        // linear scan (more read cost, but no extra writes)
        for (auto k : keys) {
            if (k == key) return true;
        }
        // no extra persistence cost for reads here
        return false;
    }
};

// "Sorted leaf" model: inserts are more expensive (shifts), but reads are cheaper.
struct SortedLeaf {
    std::vector<uint64_t> keys;

    void insert(uint64_t key, Stats& s) {
        // insert in sorted order -> shifts many entries
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        keys.insert(it, key);
        // approximate: more writes/flushes than unsorted
        s.Nw   += 4;
        s.Nclf += 2;
        s.Nmf  += 1;
    }

    bool search(uint64_t key, Stats& s) const {
        // binary search
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        if (it != keys.end() && *it == key) {
            return true;
        }
        return false;
    }
};

struct MixedResult {
    double throughput_ops_sec;
    Stats stats;
};

// Generic driver for mixed workloads
template<typename LeafType>
MixedResult run_mixed_workload(uint64_t prefill,
                               uint64_t num_ops,
                               double write_ratio) {
    LeafType leaf;
    Stats stats;

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> dist_key(1, 1'000'000'000ULL);
    std::uniform_real_distribution<double> dist01(0.0, 1.0);

    // Pre-fill leaf
    for (uint64_t i = 0; i < prefill; ++i) {
        leaf.insert(dist_key(rng), stats);
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < num_ops; ++i) {
        double r = dist01(rng);
        uint64_t key = dist_key(rng);

        if (r < write_ratio) {
            leaf.insert(key, stats);
        } else {
            (void) leaf.search(key, stats);
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = t1 - t0;

    MixedResult res;
    res.throughput_ops_sec = static_cast<double>(num_ops) / elapsed.count();
    res.stats = stats;
    return res;
}

int main() {
    const uint64_t PREFILL = 5000;
    const uint64_t OPS     = 100000;

    std::vector<double> write_ratios = {0.9, 0.5, 0.1, 0.0};

    std::cout << "variant,write_ratio,ops,throughput_ops_sec,Nw,Nclf,Nmf\n";

    for (double wr : write_ratios) {
        // Unsorted leaf
        {
            MixedResult r = run_mixed_workload<UnsortedLeaf>(PREFILL, OPS, wr);
            std::cout << "unsorted_leaf,"
                      << wr << ","
                      << OPS << ","
                      << r.throughput_ops_sec << ","
                      << r.stats.Nw << ","
                      << r.stats.Nclf << ","
                      << r.stats.Nmf << "\n";
        }

        // Sorted leaf
        {
            MixedResult r = run_mixed_workload<SortedLeaf>(PREFILL, OPS, wr);
            std::cout << "sorted_leaf,"
                      << wr << ","
                      << OPS << ","
                      << r.throughput_ops_sec << ","
                      << r.stats.Nw << ","
                      << r.stats.Nclf << ","
                      << r.stats.Nmf << "\n";
        }
    }

    return 0;
}
