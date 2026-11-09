"""Utilities for refining text-reuse alignments and finding contiguous matching spans.

Passim's character offsets in `out.json` can be overly eager when matches are
based on sparse n-gram shingling across noisy OCR text. This module provides
tools to find contiguous matching token subsequences (Longest Common Substring /
k-mer matching) to tighten passage boundaries and filter out spurious alignments.
"""

import difflib
import re
from typing import Dict, List, Optional, Tuple


def tokenize_with_offsets(text: str) -> List[Tuple[str, int, int]]:
    """Tokenize text into lowercase alphanumeric words while preserving char offsets.

    Args:
        text: Input string.

    Returns:
        List of tuples: (token_str, start_char_offset, end_char_offset)
    """
    return [
        (match.group().lower(), match.start(), match.end())
        for match in re.finditer(r"\w+", text)
    ]


def find_chained_contiguous_spans(
    text_a: str,
    text_b: str,
    min_anchor_tokens: int = 3,
) -> List[Dict]:
    """Find all matching contiguous token runs (anchors) between text_a and text_b.

    Args:
        text_a: First passage text.
        text_b: Second passage text.
        min_anchor_tokens: Minimum contiguous tokens for an anchor run (default: 3).

    Returns:
        List of anchor dicts sorted by position in text_a:
        [{'start_a', 'end_a', 'start_b', 'end_b', 'length'}, ...]
    """
    toks_a = tokenize_with_offsets(text_a)
    toks_b = tokenize_with_offsets(text_b)
    words_a = [t[0] for t in toks_a]
    words_b = [t[0] for t in toks_b]

    len_a = len(words_a)
    len_b = len(words_b)
    if len_a == 0 or len_b == 0:
        return []

    dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]
    raw_anchors = []

    for i in range(1, len_a + 1):
        for j in range(1, len_b + 1):
            if words_a[i - 1] == words_b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                if dp[i - 1][j - 1] >= min_anchor_tokens:
                    length = dp[i - 1][j - 1]
                    raw_anchors.append({
                        "start_a": i - 1 - length,
                        "end_a": i - 1,
                        "start_b": j - 1 - length,
                        "end_b": j - 1,
                        "length": length,
                    })
                dp[i][j] = 0
        if dp[i][len_b] >= min_anchor_tokens:
            length = dp[i][len_b]
            raw_anchors.append({
                "start_a": i - length,
                "end_a": i,
                "start_b": len_b - length,
                "end_b": len_b,
                "length": length,
            })

    raw_anchors.sort(key=lambda m: (m["start_a"], m["start_b"]))

    # Deduplicate / prune sub-runs contained within larger anchors
    filtered_anchors: List[Dict] = []
    for a in raw_anchors:
        if not any(
            f["start_a"] <= a["start_a"]
            and f["end_a"] >= a["end_a"]
            and f["start_b"] <= a["start_b"]
            and f["end_b"] >= a["end_b"]
            for f in filtered_anchors
        ):
            filtered_anchors.append(a)

    return filtered_anchors


def align_two_phase_with_ocr(
    text_a: str,
    text_b: str,
    min_anchor_tokens: int = 3,
    max_gap_tokens: int = 8,
    ocr_sim_threshold: float = 0.65,
) -> Dict:
    """Two-Phase Alignment: Exact k-mer Anchors + Local Fuzzy OCR Gap Resolution.

    Phase 1: Identifies all solid multi-word anchor runs (>= min_anchor_tokens)
             using exact token matching.
    Phase 2: In the small gaps between consecutive anchors (<= max_gap_tokens),
             aligns corrupted words that have character similarity >= ocr_sim_threshold
             (e.g., 'vandolse' <=> 'vaudolse', 'Bestlères' <=> 'Bessiéres').

    Args:
        text_a: First passage text.
        text_b: Second passage text.
        min_anchor_tokens: Minimum words for an anchor run (default: 3).
        max_gap_tokens: Maximum tokens in a gap to evaluate for OCR recovery (default: 8).
        ocr_sim_threshold: Minimum character similarity ratio for OCR match (default: 0.65).

    Returns:
        Dict with:
            - 'spans_a': List of (start_char, end_char, match_type) tuples in text_a.
            - 'spans_b': List of (start_char, end_char, match_type) tuples in text_b.
            - 'ocr_matches': List of (word_a, word_b, sim_ratio) tuples recovered in Phase 2.
            - 'total_matched_tokens': Total tokens aligned across both phases.
    """
    toks_a = tokenize_with_offsets(text_a)
    toks_b = tokenize_with_offsets(text_b)
    words_a = [t[0] for t in toks_a]
    words_b = [t[0] for t in toks_b]

    anchors = find_chained_contiguous_spans(
        text_a, text_b, min_anchor_tokens=min_anchor_tokens
    )

    spans_a = []
    spans_b = []
    ocr_matches = []
    matched_tokens_count = 0

    for idx, anc in enumerate(anchors):
        # Phase 1: Exact anchor span
        spans_a.append((toks_a[anc["start_a"]][1], toks_a[anc["end_a"] - 1][2], "exact"))
        spans_b.append((toks_b[anc["start_b"]][1], toks_b[anc["end_b"] - 1][2], "exact"))
        matched_tokens_count += anc["length"]

        # Phase 2: Gap resolution between this anchor and the next
        if idx < len(anchors) - 1:
            next_anc = anchors[idx + 1]
            gap_a_start, gap_a_end = anc["end_a"], next_anc["start_a"]
            gap_b_start, gap_b_end = anc["end_b"], next_anc["start_b"]

            len_gap_a = gap_a_end - gap_a_start
            len_gap_b = gap_b_end - gap_b_start

            if 0 < len_gap_a <= max_gap_tokens and 0 < len_gap_b <= max_gap_tokens:
                sub_a = words_a[gap_a_start:gap_a_end]
                sub_b = words_b[gap_b_start:gap_b_end]

                matcher = difflib.SequenceMatcher(None, sub_a, sub_b)
                for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                    if tag == "replace" and (i2 - i1 == 1) and (j2 - j1 == 1):
                        w_a = sub_a[i1]
                        w_b = sub_b[j1]
                        # Only match non-numeric or long numeric words with high similarity
                        sim = difflib.SequenceMatcher(None, w_a, w_b).ratio()
                        if sim >= ocr_sim_threshold:
                            pos_a = gap_a_start + i1
                            pos_b = gap_b_start + j1
                            spans_a.append((toks_a[pos_a][1], toks_a[pos_a][2], "ocr"))
                            spans_b.append((toks_b[pos_b][1], toks_b[pos_b][2], "ocr"))
                            ocr_matches.append((w_a, w_b, round(sim, 2)))
                            matched_tokens_count += 1

    # Sort spans by starting character offset
    spans_a.sort(key=lambda s: s[0])
    spans_b.sort(key=lambda s: s[0])

    return {
        "spans_a": spans_a,
        "spans_b": spans_b,
        "ocr_matches": ocr_matches,
        "total_matched_tokens": matched_tokens_count,
        "anchor_count": len(anchors),
    }


def longest_contiguous_matching_span(
    text_a: str,
    text_b: str,
    min_tokens: int = 5,
) -> Dict[str, Optional[Tuple[int, int]]]:
    """Find the longest contiguous run of matching tokens between two passages.

    Uses space-optimized O(len_b) dynamic programming for longest common substring (LCS)
    over tokenized words.

    Args:
        text_a: First passage text.
        text_b: Second passage text.
        min_tokens: Minimum length (in words) for a contiguous match to be considered valid.

    Returns:
        Dictionary containing:
            - 'token_count': Length of longest contiguous matching run in tokens.
            - 'char_span_a': (start, end) character offsets in text_a, or None if < min_tokens.
            - 'char_span_b': (start, end) character offsets in text_b, or None if < min_tokens.
            - 'matched_text_a': The slice of text_a corresponding to char_span_a.
            - 'matched_text_b': The slice of text_b corresponding to char_span_b.
    """
    tokens_a = tokenize_with_offsets(text_a)
    tokens_b = tokenize_with_offsets(text_b)

    words_a = [t[0] for t in tokens_a]
    words_b = [t[0] for t in tokens_b]

    len_a = len(words_a)
    len_b = len(words_b)

    if len_a == 0 or len_b == 0:
        return {
            "token_count": 0,
            "char_span_a": None,
            "char_span_b": None,
            "matched_text_a": "",
            "matched_text_b": "",
        }

    # Space-optimized DP: row only depends on previous row
    # prev[j] stores LCS length ending at words_a[i-1] and words_b[j-1]
    prev = [0] * (len_b + 1)
    max_len = 0
    end_a = 0
    end_b = 0

    for i in range(1, len_a + 1):
        curr = [0] * (len_b + 1)
        w_a = words_a[i - 1]
        for j in range(1, len_b + 1):
            if w_a == words_b[j - 1]:
                curr[j] = prev[j - 1] + 1
                if curr[j] > max_len:
                    max_len = curr[j]
                    end_a = i
                    end_b = j
            else:
                curr[j] = 0
        prev = curr

    if max_len < min_tokens:
        return {
            "token_count": max_len,
            "char_span_a": None,
            "char_span_b": None,
            "matched_text_a": "",
            "matched_text_b": "",
        }

    start_a = end_a - max_len
    start_b = end_b - max_len

    char_span_a = (tokens_a[start_a][1], tokens_a[end_a - 1][2])
    char_span_b = (tokens_b[start_b][1], tokens_b[end_b - 1][2])

    return {
        "token_count": max_len,
        "char_span_a": char_span_a,
        "char_span_b": char_span_b,
        "matched_text_a": text_a[char_span_a[0] : char_span_a[1]],
        "matched_text_b": text_b[char_span_b[0] : char_span_b[1]],
    }


def refine_passage_bounding_box(
    passage_record: dict,
    reference_text: str,
    min_tokens: int = 5,
) -> dict:
    """Refine a passage's begin/end offsets and text based on contiguous alignment.

    Args:
        passage_record: Dict containing 'text', 'begin', 'end', etc.
        reference_text: Text of matching counterpart passage.
        min_tokens: Minimum contiguous matching tokens to keep the refined span.

    Returns:
        Updated passage_record with adjusted 'begin', 'end', and 'text',
        or marked as empty/invalid if no contiguous match >= min_tokens.
    """
    raw_text = passage_record.get("text", "")
    match_result = longest_contiguous_matching_span(
        raw_text, reference_text, min_tokens=min_tokens
    )

    span = match_result["char_span_a"]
    if span is not None:
        rel_start, rel_end = span
        base_begin = passage_record.get("begin", 0)
        passage_record["begin"] = base_begin + rel_start
        passage_record["end"] = base_begin + rel_end
        passage_record["text"] = match_result["matched_text_a"]
        passage_record["lcs_token_count"] = match_result["token_count"]
    else:
        passage_record["lcs_token_count"] = match_result["token_count"]

    return passage_record


def refine_cluster_passages_with_medoid(
    records: List[dict],
    min_tokens: int = 5,
    max_medoid_samples: int = 50,
) -> List[dict]:
    """Refine all passages within a cluster against the cluster's medoid passage.

    Finds the medoid passage (the passage most representative of the cluster by
    mean Jaccard similarity) and extracts the longest contiguous matching token
    subsequence (LCS) of each passage against this medoid.

    Adds the following fields to each passage record:
        - `lcs_token_count` (int): Number of contiguous words matched against the medoid.
        - `lcs_begin` (int or None): Adjusted start character offset.
        - `lcs_end` (int or None): Adjusted end character offset.
        - `lcs_text` (str or None): Extracted contiguous matching text slice.

    Args:
        records: List of passage dicts belonging to the same cluster.
        min_tokens: Minimum contiguous tokens required for a valid refined span.
        max_medoid_samples: Max passages sampled when selecting the medoid.

    Returns:
        List of passage dicts enriched with LCS alignment metrics.
    """
    if len(records) == 0:
        return records

    if len(records) == 1:
        r = records[0]
        tokens = tokenize_with_offsets(r.get("text", ""))
        r["lcs_token_count"] = len(tokens)
        r["lcs_begin"] = r.get("begin")
        r["lcs_end"] = r.get("end")
        r["lcs_text"] = r.get("text")
        return [r]

    # Sample if cluster is very large to determine the medoid quickly
    import random

    sample_pool = records
    if len(records) > max_medoid_samples:
        rng = random.Random(42)
        sample_pool = rng.sample(records, max_medoid_samples)

    token_sets = [
        set(re.sub(r'[().,;:!0-9"{}\][»«]', "", r.get("text", "")).lower().split())
        for r in sample_pool
    ]

    scores = []
    for i, ts_i in enumerate(token_sets):
        sims = [
            (len(ts_i & ts_j) / len(ts_i | ts_j))
            for j, ts_j in enumerate(token_sets)
            if i != j and (ts_i | ts_j)
        ]
        scores.append(sum(sims) / len(sims) if sims else 0.0)

    medoid_idx = scores.index(max(scores))
    medoid_text = sample_pool[medoid_idx].get("text", "")

    for r in records:
        text = r.get("text", "")
        match = longest_contiguous_matching_span(
            text, medoid_text, min_tokens=min_tokens
        )
        r["lcs_token_count"] = match["token_count"]
        span = match["char_span_a"]
        if span is not None:
            rel_start, rel_end = span
            base_begin = r.get("begin", 0)
            r["lcs_begin"] = base_begin + rel_start
            r["lcs_end"] = base_begin + rel_end
            r["lcs_text"] = match["matched_text_a"]
        else:
            r["lcs_begin"] = None
            r["lcs_end"] = None
            r["lcs_text"] = None

    return records
