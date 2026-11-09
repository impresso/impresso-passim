import pytest
from scripts.alignment_refinement import (
    longest_contiguous_matching_span,
    refine_passage_bounding_box,
    tokenize_with_offsets,
)


def test_tokenize_with_offsets():
    text = "Le 12 mars, à Lausanne."
    tokens = tokenize_with_offsets(text)
    assert len(tokens) == 5
    assert tokens[0] == ("le", 0, 2)
    assert tokens[1] == ("12", 3, 5)
    assert tokens[2] == ("mars", 6, 10)
    assert tokens[3] == ("à", 12, 13)
    assert tokens[4] == ("lausanne", 14, 22)


def test_longest_contiguous_matching_span_identical():
    text = "Rapport de gestion et comptes pour l'exercice 1928 approuvé à l'unanimité"
    res = longest_contiguous_matching_span(text, text, min_tokens=5)
    assert res["token_count"] == 13
    assert res["char_span_a"] == (0, len(text))
    assert res["matched_text_a"] == text


def test_longest_contiguous_matching_span_scattered_formula_rejection():
    # Like in cluster c307, only short fragmented stopwords match
    text_a = "Ebauches S. A Neuchatel Convocation Deuxième Assemblée Générale Ordinaire le samedi 23 mars 1929"
    text_b = "CONVOCATION Mesdames, Messieurs les Actionnaires The Swatch Group Assemblée générale ordinaire le mercredi"
    # Longest contiguous run is "Assemblée générale ordinaire le" = 4 tokens
    res = longest_contiguous_matching_span(text_a, text_b, min_tokens=5)
    # min_tokens=5 should reject this eager match
    assert res["char_span_a"] is None
    assert res["token_count"] == 4


def test_longest_contiguous_matching_span_valid_embedded_reuse():
    # Long matching quote embedded within divergent contextual text
    context_a = "Hier matin le journal annonçait que: La séance d'ouverture du parlement aura lieu vendredi matin dans la grande salle du conseil. Tout le monde applaudit."
    context_b = "Selon les dernières nouvelles: La séance d'ouverture du parlement aura lieu vendredi matin dans la grande salle du conseil. Les débats s'annoncent vifs."

    res = longest_contiguous_matching_span(context_a, context_b, min_tokens=7)
    assert res["token_count"] >= 15
    assert "La séance d'ouverture du parlement aura lieu vendredi matin dans la grande salle du conseil" in res["matched_text_a"]


def test_refine_passage_bounding_box():
    passage = {
        "id": "doc1@100:250",
        "begin": 100,
        "end": 250,
        "text": "Intro text before quote: La séance d'ouverture du parlement aura lieu vendredi matin. Outro text after.",
    }
    reference = "La séance d'ouverture du parlement aura lieu vendredi matin sans délai."

    refined = refine_passage_bounding_box(passage, reference, min_tokens=5)
    assert refined["lcs_token_count"] == 10
    assert refined["begin"] > 100
    assert refined["end"] < 250
    assert refined["text"] == "La séance d'ouverture du parlement aura lieu vendredi matin"
