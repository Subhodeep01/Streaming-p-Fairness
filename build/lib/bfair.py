"""bfair -- minimal, dependency-free implementation of BFairReOrder.

Reorders a stream of grouped items to MAXIMIZE the number of fair size-`s`
sliding blocks under a target group distribution. Pure Python standard library;
drop it into any pipeline.

Fairness.  Given target proportions ``p_i`` (summing to 1) and block size ``s``,
a size-`s` block is *fair* iff every group ``i`` appears between ``floor(p_i*s)``
and ``ceil(p_i*s)`` times in it.  The objective is the number of fair blocks among
the ``N - s + 1`` contiguous size-`s` windows of the output.

Guarantee.  The output is OPTIMAL: it attains ``max(0, L_max - s + 1)`` fair
sliding blocks -- the maximum over every reordering of the same multiset -- where
``L_max`` is the length of the longest all-fair region realizable from the item
counts.  Proven for ``N`` a multiple of ``s`` (the usual window+landmark contract);
see PROOF.tex.  Two regimes:
  * NO LEFTOVER  -- an exact all-fair tiling exists: every one of the ``N-s+1``
    sliding blocks is fair (``L_max = N``).
  * LEFTOVER     -- emit the longest all-fair region, then the leftover items
    (the "defect") concentrated at one end, where they spoil the fewest blocks.
Runs in ``O(N + (N/s)*L*log L*R)`` time (``L`` = #groups, ``R`` = round-ups per
block), i.e. near-linear in ``N``.

Usage:
    from bfair import bfair_reorder
    out = bfair_reorder(list("AABBBCCCC"), {"A": .3, "B": .3, "C": .4}, 3)

    # items of any type -- give attr_fn to read the group off an item:
    out = bfair_reorder(rows, {"m": .5, "f": .5}, 4, attr_fn=lambda r: r["gender"])
"""
from __future__ import annotations

import math
from collections import deque
from typing import Any, Callable, Dict, Hashable, List, Sequence

__all__ = ["bfair_reorder", "is_block_fair", "sliding_fair_count"]


# ===========================================================================
# Public API
# ===========================================================================

def bfair_reorder(
    stream: Sequence[Any],
    fairness_constraint: Dict[Hashable, float],
    block_size: int,
    attr_fn: Callable[[Any], Hashable] = lambda x: x,
) -> List[Any]:
    """Return a reordering of ``stream`` maximizing fair size-`block_size` blocks.

    stream               sequence of items of any type.
    fairness_constraint  ``{group: target_proportion}``; proportions must sum to 1,
                         and every item's group (via ``attr_fn``) must be a key.
    block_size           the window size ``s`` (>= 1).
    attr_fn              maps an item to its group (default: the item itself).

    The returned list is a permutation of ``stream``.
    """
    s = block_size
    if s < 1:
        raise ValueError("block_size must be a positive integer")
    groups = list(fairness_constraint)
    props = [fairness_constraint[g] for g in groups]
    if abs(sum(props) - 1.0) > 1e-6:
        raise ValueError("fairness_constraint proportions must sum to 1")

    index_of = {g: i for i, g in enumerate(groups)}
    buckets: Dict[Hashable, deque] = {g: deque() for g in groups}
    counts = [0] * len(groups)
    for item in stream:
        g = attr_fn(item)
        if g not in index_of:
            raise KeyError(f"item group {g!r} is not in fairness_constraint")
        buckets[g].append(item)
        counts[index_of[g]] += 1

    N = sum(counts)
    if N == 0:
        return []
    F = [math.floor(p * s) for p in props]   # per-block floors
    C = [math.ceil(p * s) for p in props]    # per-block ceilings

    if N % s == 0 and _tileable(counts, N // s, F, C):
        template = _tiling_arrangement(counts, s, N // s, F, C)   # all-fair, length N
    else:
        template = _longest_region(counts, s, F, C)              # length L_max

    out = [buckets[groups[i]].popleft() for i in template]
    for g in groups:                         # the defect: any leftover items, one end
        out.extend(buckets[g])
    return out


# ===========================================================================
# No leftover: exact tiling + base / aligned-extra arrangement
# ===========================================================================

def _tileable(counts: Sequence[int], M: int, F: Sequence[int], C: Sequence[int]) -> bool:
    """Whether the counts admit an exact tiling into ``M`` fair blocks (M = N/s):
    tileable iff ``M*F_i <= counts_i <= M*C_i`` for every group."""
    return all(M * F[i] <= counts[i] <= M * C[i] for i in range(len(counts)))


def _tiling_arrangement(
    counts: Sequence[int], s: int, M: int, F: Sequence[int], C: Sequence[int]
) -> List[int]:
    """Realize ``M`` fair configurations (most-needed-first greedy) as one word in
    which EVERY sliding block is fair, and return it as group indices.

    Each block is ``pat(F)`` in fixed base columns plus its ``R`` round-up groups in
    the extra columns; a round-up shared with the previous block keeps its column, so
    no shared group can double up across a junction.
    """
    ell = len(counts)
    R = s - sum(F)
    Phi = [i for i in range(ell) if C[i] > F[i]]
    need = [counts[i] - M * F[i] for i in range(ell)]      # round-ups owed per group
    base = [i for i in range(ell) for _ in range(F[i])]    # pat(F): the fixed base
    out: List[int] = []
    prev_col: Dict[int, int] = {}
    for _ in range(M):
        S = sorted(Phi, key=lambda i: (-need[i], i))[:R]   # this block's round-ups
        for i in S:
            need[i] -= 1
        col: Dict[int, int] = {}
        used = set()
        for i in S:                                        # shared coords keep column
            if i in prev_col:
                col[i] = prev_col[i]
                used.add(prev_col[i])
        free = [c for c in range(R) if c not in used]
        for i, c in zip((i for i in S if i not in col), free):
            col[i] = c
        extra = [0] * R
        for i, c in col.items():
            extra[c] = i
        out.extend(base)
        out.extend(extra)
        prev_col = col
    return out


# ===========================================================================
# Leftover: the longest all-fair region (backward algorithm) + defect
# ===========================================================================

def _longest_region(
    counts: Sequence[int], s: int, F: Sequence[int], C: Sequence[int]
) -> List[int]:
    """The longest all-fair region realizable within ``counts``, as group indices.

    ``L_max`` depends only on the counts (not on ordering/columns/alignment), so it is
    an arithmetic maximum computed by ``_lmax_plan``; ``_backward_region`` realizes it,
    all-fair by construction.  See PROOF.tex.
    """
    plan = _lmax_plan(counts, s, F, C)
    if plan is None or plan[0] < s:
        return []
    return _backward_region(counts, s, F, C, plan)


def _lmax_plan(counts: Sequence[int], s: int, F: Sequence[int], C: Sequence[int]):
    """``L_max`` exactly, plus the data ``(L, nu, b, S_T, q)`` realizing it, or None.

    For each block count ``q``, with ``a_i = counts_i - q*F_i`` (the items past the
    floors), the best tail length is found by a SORT over round-up degrees ``nu``:
    each Phi-coordinate supplies free capacity ``min(cap_i, a_i - F_i)`` and then costs
    one per further unit, so cheap coordinates go first (PROOF.tex, "the maximization
    is a sort").  ``L_max = max over q, nu of q*s + r``.
    """
    ell = len(counts)
    R = s - sum(F)
    Phi = [i for i in range(ell) if C[i] > F[i]]
    N = sum(counts)
    best = None
    for q in range(1, N // s + 1):
        a = [counts[i] - q * F[i] for i in range(ell)]
        if any(x < 0 for x in a):
            continue
        cap = {i: min(q, a[i]) for i in Phi}
        d = {i: a[i] - F[i] for i in Phi}
        if sum(cap.values()) < q * R:
            continue
        elig = [i for i in Phi if d[i] >= 1]
        order = sorted(elig, key=lambda i: (1 if cap[i] >= d[i] else 0,
                                            cap[i] - min(cap[i], d[i] - 1)))
        for kappa in range(0, min(R, len(elig)) + 1):
            K = set(order[:kappa])
            newcap = {i: (min(cap[i], d[i] - 1) if i in K else cap[i]) for i in Phi}
            if sum(newcap.values()) < q * R:
                continue
            nu = [0] * ell
            left = q * R
            for i in Phi:                       # free units first
                take = min(left, max(0, min(newcap[i], d[i])))
                nu[i] += take
                left -= take
            for i in Phi:                       # then whatever capacity remains
                take = min(left, newcap[i] - nu[i])
                nu[i] += take
                left -= take
            if left > 0:
                continue
            z = [a[i] - nu[i] for i in range(ell)]
            if any(x < 0 for x in z):
                continue
            S_T = [i for i in Phi if z[i] > F[i]][:R]
            b = [min(F[i], z[i] - (1 if i in S_T else 0)) for i in range(ell)]
            if any(x < 0 for x in b):
                continue
            r = min(s - 1, sum(b) + len(S_T))
            excess = sum(b) + len(S_T) - r      # trim the tail down to exactly r
            for i in range(ell):
                if excess <= 0:
                    break
                take = min(b[i], excess)
                b[i] -= take
                excess -= take
            while excess > 0 and S_T:
                S_T.pop()
                excess -= 1
            if excess > 0:
                continue
            L = q * s + r
            if L <= N and (best is None or L > best[0]):
                best = (L, nu, b, S_T, q)
    return best


def _realize_degrees(
    need: Sequence[int], q: int, R: int, Phi: Sequence[int], ell: int
) -> List[List[int]]:
    """Round-up sets ``S_1..S_q`` (subsets of Phi, size R) realizing the degrees
    ``need_i = #{t : i in S_t}``, most-needed-first (provably complete when
    ``sum(need) == q*R`` and ``need_i <= q``; PROOF.tex)."""
    nd = list(need)
    out: List[List[int]] = []
    for _ in range(q):
        S = sorted(Phi, key=lambda i: (-nd[i], i))[:R]
        for i in S:
            nd[i] -= 1
        out.append(S)
    return out


def _backward_region(
    counts: Sequence[int], s: int, F: Sequence[int], C: Sequence[int], plan
) -> List[int]:
    """Realize ``_lmax_plan``'s data as an all-fair region by building the alignment
    chain BACKWARD from the tail (PROOF.tex, "Normal form").

    Backward pins the coordinates shared with the tail to LIVE columns first, so the
    truncated last block stays fair -- no verification needed.  Returns group indices.
    """
    L, nu, b, S_T, q = plan
    ell = len(counts)
    R = s - sum(F)
    Phi = [i for i in range(ell) if C[i] > F[i]]
    r = L - q * s
    e = len(S_T)
    S = _realize_degrees(nu, q, R, Phi, ell)
    # column plan: e extras inside [0, r), the other R-e at/after r
    E = list(range(e)) + list(range(r, r + (R - e)))
    Eset = set(E)
    B = [c for c in range(s) if c not in Eset]
    base_syms = [i for i in range(ell) for _ in range(b[i])]           # the tail's share
    base_syms += [i for i in range(ell) for _ in range(F[i] - b[i])]   # the rest of F
    pis: List[Dict[int, int]] = [None] * (q + 1)
    pis[q] = {i: E[k] for k, i in enumerate(S_T)}       # E[0..e-1] are the live columns
    for t in range(q - 1, -1, -1):
        nxt = pis[t + 1]
        col: Dict[int, int] = {}
        used = set()
        for i in S[t]:                                  # shared coords keep their column
            if i in nxt:
                col[i] = nxt[i]
                used.add(nxt[i])
        free = [c for c in E if c not in used]
        for i, c in zip((i for i in S[t] if i not in col), free):
            col[i] = c
        pis[t] = col
    out: List[int] = []
    for t in range(q + (1 if r else 0)):
        blk: List[Any] = [None] * s
        for i, c in pis[t].items():
            blk[c] = i
        for c, sym in zip(B, base_syms):
            blk[c] = sym
        out.extend(blk if t < q else blk[:r])
    return out


# ===========================================================================
# Fairness helpers (optional; for checking / testing an arrangement)
# ===========================================================================

def is_block_fair(
    block: Sequence[Any],
    fairness_constraint: Dict[Hashable, float],
    attr_fn: Callable[[Any], Hashable] = lambda x: x,
) -> bool:
    """Whether one size-`s` block meets the ``[floor, ceil]`` bound for every group."""
    s = len(block)
    counts: Dict[Hashable, int] = {}
    for x in block:
        g = attr_fn(x)
        counts[g] = counts.get(g, 0) + 1
    for g, p in fairness_constraint.items():
        t = p * s
        if not (math.floor(t) <= counts.get(g, 0) <= math.ceil(t)):
            return False
    return True


def sliding_fair_count(
    stream: Sequence[Any],
    fairness_constraint: Dict[Hashable, float],
    block_size: int,
    attr_fn: Callable[[Any], Hashable] = lambda x: x,
) -> int:
    """Number of fair sliding blocks among the ``N - s + 1`` windows (the objective)."""
    s = block_size
    return sum(is_block_fair(stream[o:o + s], fairness_constraint, attr_fn)
               for o in range(len(stream) - s + 1))


if __name__ == "__main__":
    constraint = {"A": 0.3, "B": 0.3, "C": 0.4}
    stream = list("A" * 2 + "B" * 5 + "C" * 5)       # N=12, not tileable -> leftover
    s = 3
    out = bfair_reorder(stream, constraint, s)
    fair = sliding_fair_count(out, constraint, s)
    print("in :", "".join(stream))
    print("out:", "".join(out))
    print(f"fair sliding blocks: {fair} / {len(out) - s + 1}")
    assert sorted(out) == sorted(stream), "output must be a permutation of the input"
