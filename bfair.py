"""bfair -- minimal, dependency-free implementation of BFairReOrder."""
from __future__ import annotations

import math
from collections import deque
from typing import Any, Callable, Dict, Hashable, List, Sequence

__all__ = ["bfair_reorder", "is_block_fair", "sliding_fair_count"]


def bfair_reorder(
    stream: Sequence[Any],
    fairness_constraint: Dict[Hashable, float],
    block_size: int,
    attr_fn: Callable[[Any], Hashable] = lambda x: x,
) -> List[Any]:
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
    F = [math.floor(p * s) for p in props]
    C = [math.ceil(p * s) for p in props]

    if N % s == 0 and _tileable(counts, N // s, F, C):
        template = _tiling_arrangement(counts, s, N // s, F, C)
    else:
        template = _longest_region(counts, s, F, C)

    out = [buckets[groups[i]].popleft() for i in template]
    for g in groups:
        out.extend(buckets[g])
    return out


def _tileable(counts, M, F, C):
    return all(M * F[i] <= counts[i] <= M * C[i] for i in range(len(counts)))


def _tiling_arrangement(counts, s, M, F, C):
    ell = len(counts)
    R = s - sum(F)
    Phi = [i for i in range(ell) if C[i] > F[i]]
    need = [counts[i] - M * F[i] for i in range(ell)]
    base = [i for i in range(ell) for _ in range(F[i])]
    out: List[int] = []
    prev_col: Dict[int, int] = {}
    for _ in range(M):
        S = sorted(Phi, key=lambda i: (-need[i], i))[:R]
        for i in S:
            need[i] -= 1
        col: Dict[int, int] = {}
        used = set()
        for i in S:
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


def _longest_region(counts, s, F, C):
    plan = _lmax_plan(counts, s, F, C)
    if plan is None or plan[0] < s:
        return []
    return _backward_region(counts, s, F, C, plan)


def _lmax_plan(counts, s, F, C):
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
            for i in Phi:
                take = min(left, max(0, min(newcap[i], d[i])))
                nu[i] += take
                left -= take
            for i in Phi:
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
            excess = sum(b) + len(S_T) - r
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


def _realize_degrees(need, q, R, Phi, ell):
    nd = list(need)
    out: List[List[int]] = []
    for _ in range(q):
        S = sorted(Phi, key=lambda i: (-nd[i], i))[:R]
        for i in S:
            nd[i] -= 1
        out.append(S)
    return out


def _backward_region(counts, s, F, C, plan):
    L, nu, b, S_T, q = plan
    ell = len(counts)
    R = s - sum(F)
    Phi = [i for i in range(ell) if C[i] > F[i]]
    r = L - q * s
    e = len(S_T)
    S = _realize_degrees(nu, q, R, Phi, ell)
    E = list(range(e)) + list(range(r, r + (R - e)))
    Eset = set(E)
    B = [c for c in range(s) if c not in Eset]
    base_syms = [i for i in range(ell) for _ in range(b[i])]
    base_syms += [i for i in range(ell) for _ in range(F[i] - b[i])]
    pis: List[Dict[int, int]] = [None] * (q + 1)
    pis[q] = {i: E[k] for k, i in enumerate(S_T)}
    for t in range(q - 1, -1, -1):
        nxt = pis[t + 1]
        col: Dict[int, int] = {}
        used = set()
        for i in S[t]:
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


def is_block_fair(block, fairness_constraint, attr_fn=lambda x: x):
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


def sliding_fair_count(stream, fairness_constraint, block_size, attr_fn=lambda x: x):
    s = block_size
    return sum(is_block_fair(stream[o:o + s], fairness_constraint, attr_fn)
               for o in range(len(stream) - s + 1))
