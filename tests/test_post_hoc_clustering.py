import pytest
from scripts.post_hoc_clustering import (
    split_by_temporal_episodes,
    partition_by_lcs_communities,
    recluster_monster_component,
)


def test_split_by_temporal_episodes():
    passages = [
        {"id": "doc1", "date": "1873-03-27"},
        {"id": "doc2", "date": "1873-04-01"},
        {"id": "doc3", "date": "1873-04-05"},
        # 5-month gap (> 90 days)
        {"id": "doc4", "date": "1873-09-12"},
        {"id": "doc5", "date": "1873-09-15"},
        # 40-year gap
        {"id": "doc6", "date": "1913-05-20"},
    ]

    episodes = split_by_temporal_episodes(passages, max_gap_days=90)
    assert len(episodes) == 3
    assert [p["id"] for p in episodes[0]] == ["doc1", "doc2", "doc3"]
    assert [p["id"] for p in episodes[1]] == ["doc4", "doc5"]
    assert [p["id"] for p in episodes[2]] == ["doc6"]


def test_partition_by_lcs_communities():
    # Construct two independent stories falsely linked by a shared boilerplate phrase
    # Story A: 3 documents about parliamentary debate
    story_a_1 = {
        "id": "A1",
        "text": "La séance du grand conseil vaudois s'est ouverte ce matin avec une vive discussion sur les impôts cantonaux.",
    }
    story_a_2 = {
        "id": "A2",
        "text": "Au grand conseil vaudois la séance s'est ouverte ce matin avec une vive discussion sur les impôts cantonaux et communaux.",
    }

    # Story B: 2 documents about a train derailment
    story_b_1 = {
        "id": "B1",
        "text": "Un déraillement de train de marchandises a causé l'interruption complète de la ligne ferroviaire entre Lausanne et Genève.",
    }
    story_b_2 = {
        "id": "B2",
        "text": "Le déraillement de train de marchandises a causé l'interruption complète de la ligne ferroviaire ce matin.",
    }

    # Passim mistakenly chained them because both contain isolated words like 'ce matin'
    passages = [story_a_1, story_a_2, story_b_1, story_b_2]
    communities = partition_by_lcs_communities(passages, min_tokens=6)

    # Should cleanly split into 2 separate communities
    assert len(communities) == 2
    c_ids = [set(p["id"] for p in c) for c in communities]
    assert {"A1", "A2"} in c_ids
    assert {"B1", "B2"} in c_ids


def test_recluster_monster_component():
    passages = [
        # Episode 1873
        {"id": "1873_1", "date": "1873-03-27", "text": "Emprunt cantonal obligations et souscriptions publiques garanties par le gouvernement."},
        {"id": "1873_2", "date": "1873-03-28", "text": "Emprunt cantonal obligations et souscriptions publiques garanties par le gouvernement cantonal."},
        # Episode 1929
        {"id": "1929_1", "date": "1929-03-11", "text": "Convocation générale ordinaire des actionnaires de la société horlogère."},
        {"id": "1929_2", "date": "1929-03-12", "text": "Convocation générale ordinaire des actionnaires de la société horlogère réunie."},
    ]

    reclustered = recluster_monster_component(passages, max_gap_days=90, min_tokens=5)
    assert len(reclustered) == 2
    rec_ids = [set(p["id"] for p in c) for c in reclustered]
    assert {"1873_1", "1873_2"} in rec_ids
    assert {"1929_1", "1929_2"} in rec_ids
