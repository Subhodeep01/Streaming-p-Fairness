"""Baseline reordering strategies ported from bettergreedyswapper.py.

bettergreedyswapper.py prototyped three binary (0/1) block-fairness
reordering heuristics plus a brute-force ground truth. This module extracts
the three non-brute-force heuristics, generalized from a single binary
category to an arbitrary number of categories (using the same
`{category: min_count_required_per_block}` fairness-dict convention as
`naive_reorder`/`bfair_reorder_variant` in consumer_editable_bfair_performance.py),
and exposes them with the same `reorder_fn(seq, fairness, window_size,
block_size) -> list` signature so they drop straight into that module's
`STRATEGIES` dict.

Their reordering mechanism is intentionally left exactly as suboptimal/
greedy as the original -- these are baselines meant to lose to BFairReOrder,
not competitors. Specifically preserved:
  - internal_swap_reorder donates from the first later block that's already
    fair, with no re-check that the donor block stays fair afterward.
  - greedy_swap_reorder / weighted_greedy_swap_reorder swap in one landmark
    item at a time, picking an arbitrary donor of the needed category
    (`donor_positions[-1]`, mirroring the original's `onepos.pop()`).

One deliberate deviation: the original's landmark-swap recipient candidates
range over the *entire* remaining window (`input[first_unfair:-timeout]`).
Combined with the original's O(block_size)-per-call deficit scoring, that's
fine at the toy window sizes (15-80 items) bettergreedyswapper.py was
written for, but is combinatorially infeasible at the window/landmark sizes
this baseline gets tested at here (e.g. window=1000, landmark=500 -> billions
of operations per call). Two fixes, both here: (1) _bit_score takes an O(1)
per-block prefix-count lookup instead of rescanning window[pos:block_end];
(2) recipient candidates are restricted to the currently-deficient block
rather than the whole remaining window -- still greedy/suboptimal/single-bit
at a time, just scoped to the block actually being fixed (which also avoids
the original's habit of disturbing an already-fair later block while
patching an earlier one).

Algorithms (see bettergreedyswapper.py for the original binary versions):
  1. internal_swap_reorder          <- makefair / internal_swap
  2. greedy_swap_reorder            <- greedy_swapper_func / deficitsum /
                                        count_deficit (runs 1. first)
  3. weighted_greedy_swap_reorder   <- same loop as 2., but candidate bits
                                        are scored with wtd_deficitsum's
                                        recency-weighted deficit instead of
                                        deficitsum's plain deficit (runs 1.
                                        and reuses 2.'s swap loop)
"""

from __future__ import annotations

from typing import Any, Dict, Hashable, List, Sequence


# ---------------------------------------------------------------------------
# Shared helpers: per-category prefix-sum sketch and block fairness checks
# ---------------------------------------------------------------------------

def _prefix_counts(window: Sequence[Hashable], categories: Sequence[Hashable]) -> List[Dict[Hashable, int]]:
    sketch = []
    running = {c: 0 for c in categories}
    for item in window:
        running[item] += 1
        sketch.append(dict(running))
    return sketch


def _block_counts(sketch, start: int, end: int, categories: Sequence[Hashable]) -> Dict[Hashable, int]:
    end_c = sketch[end - 1]
    start_c = sketch[start - 1] if start > 0 else {c: 0 for c in categories}
    return {c: end_c[c] - start_c[c] for c in categories}


def _is_fair(counts: Dict[Hashable, int], fairness: Dict[Hashable, int]) -> bool:
    return all(counts.get(c, 0) >= need for c, need in fairness.items())


def _deficient(counts: Dict[Hashable, int], fairness: Dict[Hashable, int]) -> Dict[Hashable, int]:
    return {c: need - counts.get(c, 0) for c, need in fairness.items() if counts.get(c, 0) < need}


def _first_unfair_block_start(window, block_size, fairness, categories):
    sketch = _prefix_counts(window, categories)
    for start in range(0, len(window), block_size):
        end = start + block_size
        if end > len(window):
            break
        if not _is_fair(_block_counts(sketch, start, end, categories), fairness):
            return start
    return None


def _block_deficits(window, start, block_size, fairness, categories):
    sketch = _prefix_counts(window, categories)
    end = start + block_size
    return _deficient(_block_counts(sketch, start, end, categories), fairness)


# ---------------------------------------------------------------------------
# Algorithm 1: internal swap (bettergreedyswapper.makefair / internal_swap)
# ---------------------------------------------------------------------------

def _fix_block_from_later_donor(window, start, end, window_size, block_size, deficits, categories, fairness):
    """One pass of internal_swap: for each deficient category, borrow from
    the first later block that's already fair. No re-check that the donor
    block stays fair afterward -- matches the original's behavior."""
    progressed = False
    for cat in sorted(deficits, key=str):
        need = deficits[cat]
        sketch = _prefix_counts(window, categories)
        donor_start = None
        j = end
        while j + block_size <= window_size:
            if _is_fair(_block_counts(sketch, j, j + block_size, categories), fairness):
                donor_start = j
                break
            j += block_size
        if donor_start is None:
            continue
        out_positions = [k for k in range(start, end) if window[k] != cat]
        in_positions = [k for k in range(donor_start, donor_start + block_size) if window[k] == cat]
        while need > 0 and out_positions and in_positions:
            out_idx = out_positions.pop(0)
            in_idx = in_positions.pop()
            window[out_idx], window[in_idx] = window[in_idx], window[out_idx]
            need -= 1
            progressed = True
    return progressed


def internal_swap_reorder(seq: Sequence[Any], fairness: Dict[Hashable, int], window_size: int, block_size: int) -> List[Any]:
    """Generalized bettergreedyswapper.makefair/internal_swap.

    Only touches seq[:window_size] -- the landmark tail (seq[window_size:])
    is never used as a donor and is appended back unchanged, exactly as in
    the original (algorithm 1 never reaches into timeout bits).
    """
    window = list(seq[:window_size])
    landmark = list(seq[window_size:])
    categories = list(fairness.keys())

    start = 0
    while start < window_size:
        end = start + block_size
        if end > window_size:
            break
        counts = _block_counts(_prefix_counts(window, categories), start, end, categories)
        deficits = _deficient(counts, fairness)
        if not deficits:
            start += block_size
            continue
        if end == window_size:
            break  # last block: no later donor block can exist -- bail, matches original
        progressed = _fix_block_from_later_donor(window, start, end, window_size, block_size, deficits, categories, fairness)
        if not progressed:
            break  # no viable donor anywhere -- bail, matches original's implicit stall
        # stays at the same `start` and re-checks next loop iteration, mirroring
        # makefair's outer while loop (it doesn't advance i after internal_swap).

    return window + landmark


# ---------------------------------------------------------------------------
# Algorithms 2 & 3: greedy landmark swapper (plain / recency-weighted deficit)
# ---------------------------------------------------------------------------

def _category_prefix(window, cat):
    """prefix[i] = count of `cat` in window[:i]; O(1) block-count lookups
    via prefix[end]-prefix[start], instead of window[start:end].count(cat)
    (O(block_size) per call -- the original's approach, which is fine at the
    toy scale bettergreedyswapper.py was written for but blows up combined
    with the (timeout+1)-offset scan at the window/landmark sizes this
    baseline gets tested at here)."""
    prefix = [0] * (len(window) + 1)
    for i, v in enumerate(window):
        prefix[i + 1] = prefix[i] + (1 if v == cat else 0)
    return prefix


def _bit_score(prefix, bit_pos, timeout, block_size, need, weighted, window_len):
    """Generalized bettergreedyswapper.count_deficit / wtd_deficitsum.

    Sums, over each of the (timeout+1) possible landmark-consumption
    offsets, the deficit of the block-aligned segment that would precede
    this bit under that offset -- an estimate of how many (future) blocks
    fixing this bit would help. `weighted=True` reproduces wtd_deficitsum's
    `deficit * (timeout - offset)` recency weighting. Takes a precomputed
    per-category prefix-count array (see _category_prefix) instead of
    re-scanning window[pos:block_end] on every call.
    """
    total = 0
    for offset in range(timeout + 1):
        pos = offset
        while pos + block_size <= bit_pos:
            pos += block_size
        block_end = pos + block_size
        if block_end > window_len:
            continue
        have = prefix[block_end] - prefix[pos]
        if have < need:
            deficit = need - have
            total += deficit * (timeout - offset) if weighted else deficit
    return total


def _greedy_landmark_fill(seq, fairness, window_size, block_size, weighted):
    combined = internal_swap_reorder(seq, fairness, window_size, block_size)
    window = combined[:window_size]
    landmark = combined[window_size:]
    categories = list(fairness.keys())
    timeout = len(landmark)

    if timeout == 0:
        return window + landmark

    for _ in range(timeout + 1):  # each iteration swaps in exactly one landmark item
        start = _first_unfair_block_start(window, block_size, fairness, categories)
        if start is None:
            break
        end = start + block_size
        deficits = _block_deficits(window, start, block_size, fairness, categories)
        if not deficits:
            break

        made_swap = False
        for cat in sorted(deficits, key=str):
            donor_positions = [k for k, v in enumerate(landmark) if v == cat]
            if not donor_positions:
                continue
            # Recipient candidates restricted to the deficient block itself
            # (not the whole remaining window, unlike the original's
            # input[first_unfair:-timeout] range) -- both for tractability at
            # window sizes far beyond bettergreedyswapper.py's toy scale, and
            # to avoid this "fix" disturbing a later block that's already fair.
            candidate_positions = [k for k in range(start, end) if window[k] != cat]
            if not candidate_positions:
                continue
            prefix = _category_prefix(window, cat)
            need = fairness[cat]
            scored = sorted(
                candidate_positions,
                key=lambda pos: (-_bit_score(prefix, pos, timeout, block_size, need, weighted, window_size), pos),
            )
            best_pos = scored[0]
            donor_idx = donor_positions[-1]  # arbitrary donor, mirrors onepos.pop()
            window[best_pos], landmark[donor_idx] = landmark[donor_idx], window[best_pos]
            made_swap = True
            break  # one bit swapped per outer iteration, mirrors the original's single-bit greedy step

        if not made_swap:
            break  # no deficient category has a landmark donor -- bail, matches original's "not enough ones"

    return window + landmark


def greedy_swap_reorder(seq: Sequence[Any], fairness: Dict[Hashable, int], window_size: int, block_size: int) -> List[Any]:
    """Generalized bettergreedyswapper.greedy_swapper_func (runs internal_swap_reorder first)."""
    return _greedy_landmark_fill(seq, fairness, window_size, block_size, weighted=False)


def weighted_greedy_swap_reorder(seq: Sequence[Any], fairness: Dict[Hashable, int], window_size: int, block_size: int) -> List[Any]:
    """Same swap loop as greedy_swap_reorder, scored with wtd_deficitsum's recency-weighted deficit."""
    return _greedy_landmark_fill(seq, fairness, window_size, block_size, weighted=True)


BASELINE_STRATEGIES = {
    "internal_swap": internal_swap_reorder,
    "greedy_swap": greedy_swap_reorder,
    "weighted_greedy_swap": weighted_greedy_swap_reorder,
}
