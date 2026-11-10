import pytest
from scripts.dask_orchestration_refinement import process_cluster_record

def test_process_small_cluster():
    small_cluster = {
        "id": "tr-test-c1",
        "cluster_size": 3,
        "time_delta": 5,
        "passages": ["doc1@0:10", "doc2@0:10", "doc3@0:10"]
    }
    results = process_cluster_record(small_cluster)
    assert len(results) == 1
    assert results[0]["id"] == "tr-test-c1"
    assert results[0]["is_monster_drift"] is False

def test_process_monster_cluster_decomposition():
    monster_cluster = {
        "id": "tr-monster-c1",
        "cluster_size": 250,
        "time_delta": 4000,
        "passages": [
            # 1870 event
            {"id": "p1", "date": "1870-01-01", "text": "discours de monsieur le president a la seance"},
            {"id": "p2", "date": "1870-01-02", "text": "discours de monsieur le president a la seance d ouverture"},
            # 1914 event
            {"id": "p3", "date": "1914-08-01", "text": "mobilisation generale de l armee suisse"},
            {"id": "p4", "date": "1914-08-02", "text": "mobilisation generale de l armee suisse decret"}
        ]
    }
    results = process_cluster_record(monster_cluster, max_gap_days=60)
    assert len(results) >= 2
    for r in results:
        assert r["is_monster_drift"] is True
        assert r["parent_cluster_id"] == "tr-monster-c1"
