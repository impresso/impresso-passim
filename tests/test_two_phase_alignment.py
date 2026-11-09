import pytest
from scripts.alignment_refinement import (
    align_two_phase_with_ocr,
    find_chained_contiguous_spans,
)


def test_find_chained_contiguous_spans():
    text_a = "la commission fédérale a décidé d'examiner le rapport annuel hier matin."
    # Insert extra words between two anchor phrases
    text_b = "la commission fédérale a décidé de suspendre la séance et d'examiner le rapport annuel hier matin."

    anchors = find_chained_contiguous_spans(text_a, text_b, min_anchor_tokens=3)
    # Should identify 2 distinct anchors:
    # 1: "la commission fédérale a décidé"
    # 2: "d examiner le rapport annuel hier matin"
    assert len(anchors) == 2
    assert anchors[0]["length"] >= 5
    assert anchors[1]["length"] >= 6


def test_align_two_phase_with_ocr():
    # Construct an example where an anchor is followed by an OCR typo, then another anchor
    text_a = "Banque cantonale vandolse et ses Agences dans le canton de Genève."
    text_b = "Banque cantonale vaudolse et ses Agences dans le canton de Genève."

    res = align_two_phase_with_ocr(
        text_a, text_b, min_anchor_tokens=2, max_gap_tokens=5, ocr_sim_threshold=0.70
    )

    # Check that 'vandolse' <=> 'vaudolse' is recognized as an OCR match
    assert len(res["ocr_matches"]) == 1
    w_a, w_b, sim = res["ocr_matches"][0]
    assert w_a == "vandolse"
    assert w_b == "vaudolse"
    assert sim >= 0.85

    # Total matched spans should contain both 'exact' and 'ocr' types
    types = [span[2] for span in res["spans_a"]]
    assert "exact" in types
    assert "ocr" in types


def test_align_two_phase_disjoint_gaps_not_matched():
    # If the gap contains completely different words, they should NOT be matched as OCR
    text_a = "Banque cantonale pour les communes et ses Agences."
    text_b = "Banque cantonale avec plusieurs filiales et ses Agences."

    res = align_two_phase_with_ocr(text_a, text_b, min_anchor_tokens=2)
    # "pour les communes" vs "avec plusieurs filiales" are real text differences, not OCR typos
    assert len(res["ocr_matches"]) == 0
