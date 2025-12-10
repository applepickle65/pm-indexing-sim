// article3_extension.cpp
// Extension: mixed read/write workload for a simplified BzTree-like leaf.
// This is *not* a full BzTree; it's a microbenchmark-style simulator.

#include <iostream>
#include <random>
#include <chrono>
#include <cstdint>
#include <vector>

struct Stats {
    uint64_t Nw   = 0;
    uint64_t Nclf = 0;
    uint64_t Nmf  = 0;
};

// Simplified BzTree leaf model:
// - insert uses PMwCAS-style multi-word update (more fences)
// - search is latch-free, read-only
struct BzLeaf {
    std::vector<uint64_t> keys;

    void insert(uint64_t key, Stats& s) {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        keys.insert(it, key);
        // simulate PMwCAS: few writes, but more fences/flushes per logical op
        s.Nw   += 3;
        s.Nclf += 3;
        s.Nmf  += 2;
    }

    bool search(uint64_t key, Stats& s) const {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        return it != keys.end() && *it == key;
    }
};

// Baseline comparator for same workload (e.g., simple B+-style leaf)
struct SimpleLeaf {
    std::vector<uint64_t> keys;

    void insert(uint64_t key, Stats& s) {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        keys.insert(it, key);
        // simpler persistence model: fewer fences/flushes
        s.Nw   += 4;
        s.Nclf += 2;
        s.Nmf  += 1;
    }

    bool search(uint64_t key, Stats& s) const {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        return it != keys.end() && *it == key;
    }
};

struct MixedResult {
    double throughput_ops_sec;
    Stats stats;
};

template<typename LeafType>
MixedResult run_mixed_workload(uint64_t prefill,
                               uint64_t num_ops,
                               double write_ratio) {
    LeafType leaf;
    Stats stats;

    std::mt19937_64 rng(321);
    std::uniform_int_distribution<uint64_t> dist_key(1, 1'000'000'000ULL);
    std::uniform_real_distribution<double> dist01(0.0, 1.0);

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
        {
            MixedResult r = run_mixed_workload<SimpleLeaf>(PREFILL, OPS, wr);
            std::cout << "simple_leaf,"
                      << wr << ","
                      << OPS << ","
                      << r.throughput_ops_sec << ","
                      << r.stats.Nw << ","
                      << r.stats.Nclf << ","
                      << r.stats.Nmf << "\n";
        }
        {
            MixedResult r = run_mixed_workload<BzLeaf>(PREFILL, OPS, wr);
            std::cout << "bztree_leaf,"
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
