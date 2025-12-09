#include <bits/stdc++.h>
#include <chrono>
#include <random>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace std::chrono;

// ========== Fake PCM / NVM metrics ==========
struct Stats {
    uint64_t Nw   = 0; // word writes (8 bytes)
    uint64_t Nclf = 0; // cache-line flushes
    uint64_t Nmf  = 0; // memory fences
};

inline void pcm_write(Stats &s, uint64_t words = 1) { s.Nw   += words; }
inline void pcm_flush(Stats &s)                    { s.Nclf += 1;      }
inline void pcm_fence(Stats &s)                    { s.Nmf  += 1;      }

// We'll pretend each leaf node is ~8 cache lines, capacity 32 entries
static const int CAP = 32;

// ========== Variant 1: Volatile main-memory B+-Tree leaf ==========
struct LeafBTreeVolatile {
    uint64_t keys[CAP];
    int count = 0;

    void insert(uint64_t k, Stats &s) {
        if (count >= CAP) return; // ignore overflow for simplicity
        int pos = 0;
        while (pos < count && keys[pos] < k) ++pos;

        // shift keys to keep sorted order
        for (int i = count; i > pos; --i) {
            keys[i] = keys[i - 1];
            pcm_write(s); // one word copied
        }
        keys[pos] = k;
        ++count;
        pcm_write(s); // write new key
        // No flush/fence: non-persistent baseline
    }

    bool search(uint64_t k, Stats &s) const {
        // assume reads only, no wear
        int lo = 0, hi = count - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (keys[mid] == k) return true;
            if (keys[mid] < k)  lo = mid + 1;
            else                hi = mid - 1;
        }
        return false;
    }
};

// ========== Variant 2: B+-Tree with undo/redo logging ==========
struct LeafBTreeLog {
    uint64_t keys[CAP];
    int count = 0;

    void insert(uint64_t k, Stats &s) {
        if (count >= CAP) return;

        // 1) Write a log record (node_id, op_type, key, pos)
        pcm_write(s, 4);  // pretend 4 words in the log record
        pcm_flush(s);     // flush log
        pcm_fence(s);     // fence to ensure durability

        // 2) Do the in-place update, same as volatile B+-Tree
        int pos = 0;
        while (pos < count && keys[pos] < k) ++pos;

        for (int i = count; i > pos; --i) {
            keys[i] = keys[i - 1];
            pcm_write(s);
        }
        keys[pos] = k;
        ++count;
        pcm_write(s);

        // 3) Flush updated node and fence
        pcm_flush(s);
        pcm_fence(s);
    }

    bool search(uint64_t k, Stats &s) const {
        int lo = 0, hi = count - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (keys[mid] == k) return true;
            if (keys[mid] < k)  lo = mid + 1;
            else                hi = mid - 1;
        }
        return false;
    }
};

// ========== Variant 3: Simplified wB+-Tree-style leaf ==========
// Idea: slot/payload indirection in the paper minimizes NVM writes;
// here we approximate that by *append + small metadata update*.
struct LeafWBTree {
    uint64_t keys[CAP];
    int count = 0;

    void insert(uint64_t k, Stats &s) {
        if (count >= CAP) return;
        int idx = count;          // append position
        keys[idx] = k;
        ++count;

        // In a real wB+-Tree, you'd update a small slot array + version.
        // Model that as just 2 word writes plus a single flush+fence:
        pcm_write(s, 2);
        pcm_flush(s);
        pcm_fence(s);
    }

    bool search(uint64_t k, Stats &s) const {
        // simple linear search (like bitmap/unsorted leaf cost)
        for (int i = 0; i < count; ++i)
            if (keys[i] == k) return true;
        return false;
    }
};

// ========== Generic benchmarking function ==========
template<typename LeafType>
double run_insert_benchmark(LeafType &leaf, Stats &stats,
                            const vector<uint64_t> &keys) {
    auto t0 = high_resolution_clock::now();
    for (auto k : keys)
        leaf.insert(k, stats);
    auto t1 = high_resolution_clock::now();
    double secs = duration<double>(t1 - t0).count();
    return keys.size() / secs;
}

int main() {
    // --- Parameters (small-scale version of the paper) ---
    const int PREFILL = CAP * 7 / 10; // ~70% full node
    const int OPS     = 100000;       // 100K inserts (paper uses 100K/500K)

    mt19937_64 rng(123);
    uniform_int_distribution<uint64_t> dist(1, 1'000'000'000ULL);

    // Pre-fill keys
    vector<uint64_t> prefill(PREFILL);
    for (auto &k : prefill) k = dist(rng);

    // Benchmark keys
    vector<uint64_t> bench(OPS);
    for (auto &k : bench) k = dist(rng);

    mkdir("results", 0777);

    ofstream csv("results/wbtree_insert_metrics.csv");
    csv << "variant,throughput_ops_sec,Nw,Nclf,Nmf\n";

    // 1) Volatile B+-Tree leaf
    {
        LeafBTreeVolatile leaf;
        Stats s;
        for (auto k : prefill) leaf.insert(k, s);
        double tp = run_insert_benchmark(leaf, s, bench);
        csv << "btree_volatile," << tp << "," << s.Nw
            << "," << s.Nclf << "," << s.Nmf << "\n";
        cout << "btree_volatile throughput: " << tp << " ops/s\n";
    }

    // 2) B+-Tree with logging
    {
        LeafBTreeLog leaf;
        Stats s;
        for (auto k : prefill) leaf.insert(k, s);
        double tp = run_insert_benchmark(leaf, s, bench);
        csv << "btree_log," << tp << "," << s.Nw
            << "," << s.Nclf << "," << s.Nmf << "\n";
        cout << "btree_log throughput: " << tp << " ops/s\n";
    }

    // 3) Simplified wB+-Tree
    {
        LeafWBTree leaf;
        Stats s;
        for (auto k : prefill) leaf.insert(k, s);
        double tp = run_insert_benchmark(leaf, s, bench);
        csv << "wbtree_simplified," << tp << "," << s.Nw
            << "," << s.Nclf << "," << s.Nmf << "\n";
        cout << "wbtree_simplified throughput: " << tp << " ops/s\n";
    }

    csv.close();
    cout << "Results written to results/wbtree_insert_metrics.csv\n";
    return 0;
}
