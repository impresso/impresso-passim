import pytest
from scripts.postprocess_textreuse_clusters import lexicaloverlap


def test_lexicaloverlap_empty_or_single():
    assert lexicaloverlap([]) == 0.0
    assert lexicaloverlap(["Single text passage here"]) == 100.0


def test_lexicaloverlap_identical():
    texts = [
        "Le Conseil d'administration de la Société Générale convoque les actionnaires.",
        "Le Conseil d'administration de la Société Générale convoque les actionnaires."
    ]
    assert lexicaloverlap(texts) == 100.0


def test_lexicaloverlap_near_identical_multiple():
    # In the old implementation with N-way intersection, adding 1 variation per document
    # would quickly collapse the intersection to 0. With pairwise Jaccard, similarity remains high.
    texts = [
        "Le Conseil d'administration de la Société convoque les actionnaires le 12 mai à Paris.",
        "Le Conseil d'administration de la Société convoque les actionnaires le 13 mai à Paris.",
        "Le Conseil d'administration de la Société convoque les actionnaires le 14 mai à Paris.",
    ]
    overlap = lexicaloverlap(texts)
    assert overlap > 70.0


def test_lexicaloverlap_disjoint():
    texts = [
        "pomme poire banane fraise cerise ananas",
        "camion voiture train avion velo bateau"
    ]
    assert lexicaloverlap(texts) == 0.0


def test_lexicaloverlap_drift_chain():
    # Tests a transitive drift chain where consecutive texts share tokens,
    # but distant ones share almost nothing.
    chain = [
        "alpha beta gamma delta epsilon",
        "beta gamma delta epsilon zeta",
        "gamma delta epsilon zeta eta",
        "delta epsilon zeta eta theta",
        "iota kappa lambda mu nu"  # completely drifted
    ]
    overlap = lexicaloverlap(chain)
    # Pairwise overlap should be low but non-zero, capturing weak cohesion
    assert 0.0 < overlap < 50.0
