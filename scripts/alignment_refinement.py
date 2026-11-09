"""Utilities for refining text-reuse alignments and finding contiguous matching spans.

Passim's character offsets in `out.json` can be overly eager when matches are
based on sparse n-gram shingling across noisy OCR text. This module provides
tools to find contiguous matching token subsequences (Longest Common Substring /
k-mer matching) to tighten passage boundaries and filter out spurious alignments.
"""

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
