#include <bits/stdc++.h>
#include <chrono>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace std::chrono;

/* =========================================================
   Fake Persistent Memory Counters (same style as other sims)
   ========================================================= */
struct Stats {
    uint64_t Nw   = 0;  // word writes
    uint64_t Nclf = 0;  // cache-line flushes
    uint64_t Nmf  = 0;  // fences
};

inline void pcm_write(Stats &s, uint64_t w=1) { s.Nw += w; }
inline void pcm_flush(Stats &s)               { s.Nclf++; }
inline void pcm_fence(Stats &s)               { s.Nmf++; }

/* =========================================================
   Toy PMwCAS (this is the heart of BzTree)
   ========================================================= */
struct PMwCAS_Entry {
    uint64_t* addr;
    uint64_t  new_val;
};

struct PMwCAS_Descriptor {
    vector<PMwCAS_Entry> entries;
};

bool pmwcas(PMwCAS_Descriptor &desc, Stats &s) {
    /* In real BzTree:
       - descriptor is installed
       - helpers assist
       - all updates appear atomic
       Here:
       - we simulate cost + atomicity
    */
    // persist descriptor metadata
    pcm_write(s, 2);
    pcm_flush(s);
    pcm_fence(s);

    // apply all updates "atomically"
    for (auto &e : desc.entries) {
        *(e.addr) = e.new_val;
        pcm_write(s);
    }

    // persist final state
    pcm_flush(s);
    pcm_fence(s);
    return true;
}

/* =========================================================
   Simplified Leaf Node (append-only like BzTree delta nodes)
   ========================================================= */
static const int CAP = 32;

struct LeafNode {
    uint64_t keys[CAP];
    int count = 0;
};

/* =========================================================
   Simplified BzTree Leaf Insert
   ========================================================= */
void bztree_insert(LeafNode &leaf, uint64_t key, Stats &s) {
    if (leaf.count >= CAP) return;

    PMwCAS_Descriptor d;
    d.entries.push_back({ &leaf.keys[leaf.count], key });
    d.entries.push_back({ (uint64_t*)&leaf.count,
                          (uint64_t)(leaf.count + 1) });

    pmwcas(d, s);
}

/* =========================================================
   Search (no wear)
   ========================================================= */
bool search_leaf(const LeafNode &leaf, uint64_t key) {
    for (int i = 0; i < leaf.count; i++)
        if (leaf.keys[i] == key) return true;
    return false;
}

/* =========================================================
   Benchmark harness
   ========================================================= */
int main() {
    mkdir("results", 0777);

    const int PREFILL = CAP * 7 / 10;  // ~70% full
    const int OPS     = 100000;

    mt19937_64 rng(123);
    uniform_int_distribution<uint64_t> dist(1, 1'000'000'000ULL);

    LeafNode leaf;
    Stats stats;

    // Prefill phase (matches paper setup)
    for (int i = 0; i < PREFILL; i++)
        bztree_insert(leaf, dist(rng), stats);

    vector<uint64_t> ops(OPS);
    for (auto &k : ops) k = dist(rng);

    // Insert benchmark
    auto t0 = high_resolution_clock::now();
    for (auto k : ops)
        bztree_insert(leaf, k, stats);
    auto t1 = high_resolution_clock::now();

    double secs = duration<double>(t1 - t0).count();
    double throughput = OPS / secs;

    // Validate correctness
    int hits = 0;
    for (int i = 0; i < 5000; i++)
        if (search_leaf(leaf, ops[i])) hits++;

    // Output
    ofstream csv("results/bztree_metrics.csv");
    csv << "variant,throughput_ops_sec,Nw,Nclf,Nmf,search_hits\n";
    csv << "bztree_sim,"
        << throughput << ","
        << stats.Nw << ","
        << stats.Nclf << ","
        << stats.Nmf << ","
        << hits << "\n";
    csv.close();

    cout << "BzTree (PMwCAS) throughput: " << throughput << " ops/sec\n";
    cout << "Search hits: " << hits << " / 5000\n";
    cout << " BzTree simulation complete\n";

    return 0;
}
