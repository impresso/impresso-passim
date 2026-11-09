"""Post-hoc clustering algorithms for decomposing monster text-reuse clusters.

Passim clusters texts using transitive connected components, which collapses
centuries of administrative notices, financial tables, and legal announcements
into sprawling monster components (e.g. 42,000+ passages spanning 140+ years).

This module provides two complementary post-hoc clustering strategies:
1. Temporal Episode Bisection: Splits a cluster along natural multi-month publication gaps.
2. Contiguous Token LCS Graph + Louvain Community Detection: Filters out spurious eager
   edges and partitions the remaining weighted graph into dense, coherent sub-clusters.
"""

import datetime
from typing import Dict, List, Optional
import networkx as nx
from networkx.algorithms.community import louvain_communities
from scripts.alignment_refinement import longest_contiguous_matching_span


def split_by_temporal_episodes(
    passages: List[Dict],
    max_gap_days: int = 90,
) -> List[List[Dict]]:
    """Partition a list of passages into temporal episodes based on publication date gaps.

    If the elapsed time between consecutive passages in the chronologically sorted
    series exceeds `max_gap_days`, a new episode sub-cluster is started.

    Args:
        passages: List of passage dicts, each with a 'date' string (YYYY-MM-DD).
        max_gap_days: Maximum allowable gap in days before splitting into a new episode.

    Returns:
        List of passage sub-lists, each representing a coherent temporal episode.
    """
    if not passages:
        return []

    sorted_passages = sorted(
        passages,
        key=lambda p: p.get("date", "1970-01-01"),
    )

    episodes: List[List[Dict]] = []
    current_episode: List[Dict] = [sorted_passages[0]]

    for p in sorted_passages[1:]:
        try:
            d_prev = datetime.datetime.strptime(current_episode[-1]["date"], "%Y-%m-%d")
            d_curr = datetime.datetime.strptime(p["date"], "%Y-%m-%d")
            gap = (d_curr - d_prev).days
        except (ValueError, KeyError):
            gap = 0

        if gap > max_gap_days:
            episodes.append(current_episode)
            current_episode = [p]
        else:
            current_episode.append(p)

    if current_episode:
        episodes.append(current_episode)

    return episodes


def partition_by_lcs_communities(
    passages: List[Dict],
    min_tokens: int = 6,
    resolution: float = 1.0,
    seed: int = 42,
) -> List[List[Dict]]:
    """Build a contiguous LCS matching graph and partition into Louvain communities.

    Constructs a graph where nodes are passages, and an edge is placed between two
    passages if and only if they share a contiguous run of at least `min_tokens` words.
    The edge weight is the length of the longest contiguous match.

    Then applies Louvain community detection to separate transitively chained components
    into internally cohesive sub-clusters.

    Args:
        passages: List of passage dicts with 'id' and 'text'.
        min_tokens: Minimum contiguous tokens required to establish a valid graph edge.
        resolution: Louvain resolution parameter (>1.0 produces smaller, tighter clusters).
        seed: Random seed for deterministic Louvain partitioning.

    Returns:
        List of passage sub-lists, each corresponding to a detected community.
    """
    n = len(passages)
    if n <= 1:
        return [passages] if passages else []

    G = nx.Graph()
    passage_by_id = {}
    for p in passages:
        pid = p["id"]
        passage_by_id[pid] = p
        G.add_node(pid)

    # Add edges where contiguous match >= min_tokens
    for i in range(n):
        p_i = passages[i]
        for j in range(i + 1, n):
            p_j = passages[j]
            match = longest_contiguous_matching_span(
                p_i.get("text", ""),
                p_j.get("text", ""),
                min_tokens=min_tokens,
            )
            count = match["token_count"]
            if count >= min_tokens:
                G.add_edge(p_i["id"], p_j["id"], weight=count)

    result_clusters: List[List[Dict]] = []

    # First, handle disconnected components (spurious bridges were removed)
    for comp in nx.connected_components(G):
        if len(comp) == 1:
            # Singleton (spurious match that shared < min_tokens with everything else)
            result_clusters.append([passage_by_id[pid] for pid in comp])
            continue

        subG = G.subgraph(comp)
        communities = louvain_communities(
            subG,
            weight="weight",
            resolution=resolution,
            seed=seed,
        )
        for comm in communities:
            sub_passages = [passage_by_id[pid] for pid in comm]
            result_clusters.append(sub_passages)

    return result_clusters


def recluster_monster_component(
    passages: List[Dict],
    max_gap_days: int = 90,
    min_tokens: int = 6,
) -> List[List[Dict]]:
    """Two-tier post-hoc clustering: Temporal Bisection followed by Louvain LCS Community Detection.

    1. First bisects sprawling multi-decade clusters along temporal publication gaps (> max_gap_days).
    2. Then partitions each temporal episode into dense communities using the LCS matching graph.

    Args:
        passages: List of passage records belonging to a monster cluster.
        max_gap_days: Maximum gap in days between consecutive articles in an episode.
        min_tokens: Minimum contiguous words for an edge in community detection.

    Returns:
        List of refined sub-clusters (each a list of passage dicts).
    """
    # Tier 1: Split into temporal episodes
    episodes = split_by_temporal_episodes(passages, max_gap_days=max_gap_days)

    final_subclusters: List[List[Dict]] = []
    for ep in episodes:
        if len(ep) <= 2:
            final_subclusters.append(ep)
        else:
            # Tier 2: Community detection within the episode
            communities = partition_by_lcs_communities(ep, min_tokens=min_tokens)
            final_subclusters.extend(communities)

    return final_subclusters
