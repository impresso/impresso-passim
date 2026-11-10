"""Dask pipeline script to re-cluster monster drift clusters and refine passage alignments at scale.

Usage:
    dask_orchestration_refinement.py --log-file=<lf> [--s3-bucket=<sb>] [--s3-cluster-input=<sci>] [--s3-passage-input=<spi>] [--s3-output-dir=<sod>] [--max-gap-days=<mgd>] [--similarity-threshold=<st>] [--n-workers=<nw>] [--batch-size=<bs>] [--verbose]

Options:
    --log-file=<lf>             Path to log file.
    --s3-bucket=<sb>            S3 bucket name. Defaults to `41-processed-data-staging`.
    --s3-cluster-input=<sci>    Path prefix to raw clusters. Defaults to `textreuse/textreuse_passim_v1-0-0/results/tr_clusters/`.
    --s3-passage-input=<spi>    Path prefix to raw passages. Defaults to `textreuse/textreuse_passim_v1-0-0/results/tr_passages/`.
    --s3-output-dir=<sod>       Output directory prefix on S3. Defaults to `textreuse/textreuse_passim_v1-0-0/results/refined/`.
    --max-gap-days=<mgd>        Maximum inactive temporal gap before splitting clusters into episodes [default: 60].
    --similarity-threshold=<st> Minimum Jaccard similarity edge threshold for Louvain community graph [default: 0.15].
    --n-workers=<nw>            Number of Dask local cluster workers [default: 8].
    --batch-size=<bs>           Number of clusters per Dask partition block [default: 1000].
    --verbose                   Enable debug logging.
"""

import os
import sys
import json
import logging
from typing import Dict, List, Any, Iterator
from docopt import docopt

import dask
import dask.bag as db
from dask.distributed import Client, LocalCluster

from scripts.post_hoc_clustering import (
    recluster_monster_component,
    split_by_temporal_episodes,
    partition_by_lcs_communities,
)
from scripts.alignment_refinement import (
    align_two_phase_with_ocr,
    refine_cluster_passages_with_medoid,
)

logger = logging.getLogger(__name__)

# Fallback S3 storage options if impresso_essentials is unavailable
try:
    from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT
except ImportError:
    IMPRESSO_STORAGEOPT = {
        "anon": False,
        "key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "client_kwargs": {
            "endpoint_url": os.environ.get("S3_ENDPOINT", "https://os.zhdk.cloud.switch.ch/")
        }
    }


def process_cluster_record(
    cluster_record: Dict[str, Any],
    max_gap_days: int = 60,
    similarity_threshold: float = 0.15
) -> List[Dict[str, Any]]:
    """Transform and refine a single cluster record using Dask map.

    - Slices temporal gaps (> max_gap_days) into distinct historical episodes.
    - Runs Louvain community detection on drift sub-networks.
    - Applies medoid LCS boundary refinement to each resulting sub-cluster.

    Returns:
        List of refined sub-cluster dictionaries ready for serialization.
    """
    try:
        passages = cluster_record.get("passages", [])
        cluster_size = cluster_record.get("cluster_size", len(passages))
        time_delta = cluster_record.get("time_delta", 0)

        # Fast path: tight, cohesive clusters need no re-clustering
        is_monster = (cluster_size > 100 or time_delta > 365)
        if not is_monster:
            # Still perform passage boundary refinement if passage text is embedded
            if passages and isinstance(passages[0], dict) and "text" in passages[0]:
                cluster_record["passages"] = refine_cluster_passages_with_medoid(passages)
            cluster_record["is_monster_drift"] = False
            cluster_record["subcluster_count"] = 1
            return [cluster_record]

        # Monster cluster path: re-cluster into episodes and Louvain communities
        subclusters_passages = recluster_monster_component(
            passages,
            max_gap_days=max_gap_days,
            min_tokens=int(similarity_threshold) if similarity_threshold >= 1 else 6
        )

        output_records = []
        for idx, sc_passages in enumerate(subclusters_passages):
            if sc_passages and isinstance(sc_passages[0], dict) and "text" in sc_passages[0]:
                sc_passages = refine_cluster_passages_with_medoid(sc_passages)
            
            sc = dict(cluster_record)
            sc["id"] = f"{cluster_record.get('id')}-sub{idx+1}"
            sc["parent_cluster_id"] = cluster_record.get("id")
            sc["passages"] = sc_passages
            sc["cluster_size"] = len(sc_passages)
            sc["is_monster_drift"] = True
            output_records.append(sc)

        return output_records

    except Exception as e:
        logger.error("Error processing cluster %s: %s", cluster_record.get("id"), e)
        cluster_record["processing_error"] = str(e)
        return [cluster_record]


def run_pipeline(
    s3_bucket: str,
    s3_cluster_input: str,
    s3_output_dir: str,
    max_gap_days: int = 60,
    similarity_threshold: float = 0.15,
    n_workers: int = 8
):
    """Execute the distributed Dask orchestration pipeline."""
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=2)
    client = Client(cluster)
    logger.info("Connected to Dask cluster dashboard at: %s", client.dashboard_link)

    input_pattern = f"s3://{s3_bucket}/{s3_cluster_input.rstrip('/')}/*.jsonl.bz2"
    output_pattern = f"s3://{s3_bucket}/{s3_output_dir.rstrip('/')}/refined_clusters-*.jsonl.bz2"

    logger.info("Reading input clusters from: %s", input_pattern)
    logger.info("Writing refined clusters to: %s", output_pattern)

    # Stream JSON Lines via Dask Bag
    raw_clusters_bag = (
        db.read_text(input_pattern, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
    )

    # Apply decomposition and refinement across workers
    refined_clusters_bag = (
        raw_clusters_bag
        .map(
            lambda c: process_cluster_record(
                c,
                max_gap_days=max_gap_days,
                similarity_threshold=similarity_threshold
            )
        )
        .flatten()  # Flatten the List[SubCluster] emitted by each cluster
    )

    # Serialize to bz2 compressed files
    future = (
        refined_clusters_bag
        .map(json.dumps)
        .to_textfiles(output_pattern, storage_options=IMPRESSO_STORAGEOPT)
    )

    logger.info("Pipeline completed successfully.")
    client.close()
    cluster.close()


def main():
    args = docopt(__doc__)
    log_file = args["--log-file"]
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG if args["--verbose"] else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    s3_bucket = args["--s3-bucket"] or "41-processed-data-staging"
    s3_cluster_input = args["--s3-cluster-input"] or "textreuse/textreuse_passim_v1-0-0/results/tr_clusters/"
    s3_output_dir = args["--s3-output-dir"] or "textreuse/textreuse_passim_v1-0-0/results/refined/"
    max_gap_days = int(args["--max-gap-days"])
    similarity_threshold = float(args["--similarity-threshold"])
    n_workers = int(args["--n-workers"])

    run_pipeline(
        s3_bucket=s3_bucket,
        s3_cluster_input=s3_cluster_input,
        s3_output_dir=s3_output_dir,
        max_gap_days=max_gap_days,
        similarity_threshold=similarity_threshold,
        n_workers=n_workers
    )


if __name__ == "__main__":
    main()
