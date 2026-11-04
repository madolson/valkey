/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* pqsort is the partial quicksort used by SORT BY … LIMIT.  Its left
 * partition recurses without a depth bound; an adversarial comparison
 * function can drive it to O(n) depth and exhaust the stack.  This test
 * uses the McIlroy adversary [McIlroy 1999, "A Killer Adversary for
 * Quicksort"] to produce that pattern and asserts that the total
 * comparison count stays within O(n log n) — the signature of a bounded
 * recursion.  Without a depth limit the adversary produces O(n^2)
 * comparisons (one comparison per recursion level per remaining element),
 * so the threshold distinguishes the two behaviours.
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "pqsort.h"
}

/* McIlroy adversary state.  A single global is fine; these tests are
 * single-threaded.  adv_val[i] is -1 until element i is frozen, then
 * holds its comparison value.  adv_gas is the "gas" element that defers
 * freezing and acts as a pivot-killer. */
struct AdvState {
    int *base;      /* pointer to first element of the array */
    int *val;       /* val[i]: -1 = unfrozen, ≥0 = frozen value */
    int gas;        /* index of the current gas element */
    int ncand;      /* next small value to assign */
    int nhigh;      /* next large value to assign (starts at n) */
    long long ncmp; /* total comparison invocations */
};

static AdvState adv;

static int adv_cmp(const void *x, const void *y) {
    int i = (const int *)x - adv.base;
    int j = (const int *)y - adv.base;
    int vi = adv.val[i];
    int vj = adv.val[j];

    adv.ncmp++;

    if (vi >= 0 && vj >= 0) return vi - vj;

    /* One or both unfrozen: freeze the non-gas one with a small value;
     * the gas defers to keep its value unknown to the pivot selection. */
    if (vi >= 0 && vj < 0) {
        if (j == adv.gas)
            adv.val[j] = adv.nhigh++;
        else
            adv.val[j] = adv.ncand++;
        return vi - adv.val[j];
    }
    if (vi < 0 && vj >= 0) {
        if (i == adv.gas)
            adv.val[i] = adv.nhigh++;
        else
            adv.val[i] = adv.ncand++;
        return adv.val[i] - vj;
    }

    /* Both unfrozen: freeze j with a small value, i becomes the new gas */
    if (i == adv.gas) {
        adv.val[j] = adv.ncand++;
        return 1; /* gas > j */
    }
    if (j == adv.gas) {
        adv.val[i] = adv.ncand++;
        return -1; /* i < gas */
    }
    /* Neither is gas yet: make j the new gas, freeze i */
    adv.gas = j;
    adv.val[i] = adv.ncand++;
    return -1;
}

/* Initialise an array [0..n-1] with identity mapping and a fresh
 * adversary state.  Caller owns arr and val (freed after use). */
static void adv_init(int n, int *arr, int *val) {
    for (int i = 0; i < n; i++) {
        arr[i] = i;
        val[i] = -1;
    }
    adv.base = arr;
    adv.val = val;
    adv.gas = 0;
    adv.ncand = 0;
    adv.nhigh = n;
    adv.ncmp = 0;
}

/* ------------------------------------------------------------------ */

/* n log2(n) helper for the comparison threshold */
static long long nlogn(int n) {
    int bits = 0;
    int tmp = n;
    while (tmp > 1) {
        tmp >>= 1;
        bits++;
    }
    return (long long)n * bits;
}

/* With an unbounded left-partition recursion the adversary drives
 * pqsort to O(n) depth and O(n^2) total comparisons.  A 2*log2(n)
 * depth limit (introsort-style) keeps comparisons within O(n log n).
 * Threshold is 8 * n * log2(n) — far above the fixed bound (~3×) and
 * far below the adversarial case (~n/8 for n=5000). */
TEST(PqsortTest, AdversaryComparisonCountBounded) {
    const int N = 5000;
    const int K = 10; /* LIMIT 0 K-1 */

    int arr[N];
    int val[N];

    adv_init(N, arr, val);

    pqsort(arr, N, sizeof(int), adv_cmp, 0, K - 1);

    long long limit = 8 * nlogn(N);
    EXPECT_LE(adv.ncmp, limit)
        << "pqsort made " << adv.ncmp << " comparisons for N=" << N
        << "; limit is " << limit << " (8 * n * log2(n))."
                                     " Unbounded recursion produces ~n^2/2 ≈ 12.5 M comparisons.";
}
