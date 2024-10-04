"""Command-line script to create the `pb.pkl` dataframe from the boilerplate's output.

Usage:
    postprocess_textreuse_passages.py --log-file=<lf>  [--s3-bucket=<sb> --s3-partition=<sp> --s3-run-partition=<srp> --n-workers=<nw> --verbose]
    
Options:

--log-file=<lf>  Path to log file.
--s3-bucket=<sb>  S3 bucket (excluding the partition) where to fetch the passim output. Defaults to `41-processed-data-staging`.
--s3-partition=<sp>  Partition within the bucket where the passim output to postprocess is. Defaults to `text-reuse/text-reuse_v1-0-0/`.
--s3-run-partition=<srp>  Partition within the bucket where to upload it. Defaults to `passim_output_run_2`
--n-workers=<nw>  Number of workers for Dask cluster. Defaults to 24.
--verbose  Set logging level to DEBUG (by default is INFO)
"""

import os
import json
import signal
import logging
from typing import Callable, Iterable, Any
import pandas as pd
import re
import numpy as np
from dask import dataframe as dd
from dask import bag as db
from dask.distributed import Client, LocalCluster
# from dask_k8 import DaskCluster
from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT
from datetime import datetime, timedelta
from ast import literal_eval
import dask.config
from docopt import docopt
from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

#dask.config.set(temporary_directory='/scratch/piconti/impresso/dask_tmp')


def format_passage_data(record: dict) -> dict:
    """Format the passage data from the passim output to fit our goals.

    Args:
        record (dict): record to unify 

    Returns:
        dict: Resulting first part of passage record in the desired format.
    """
    # ensure the record's columns correspond to the goal: with the title as the last one.
    if 'title' in record:
        record_keys = list(record.keys())
        if record_keys[-1] == 'title':
            # if 'title' is the last key, return the record as is.
            return record
        else:
            # otherwise modify the dict to place it at the end
            title = record['title']
            del record['title']
    else:
        title = ''

    if 'id' in record and 'begin' in record and 'end' in record:
        passages = "{}@{}:{}".format(record['id'], record['begin'], record['end'])
        #p_id = f"c{record['cluster']}-{passages}"
    else:
        passages = ''

    return {
        "id": f"c{record['cluster']}-{passages}",
        "begin": record['begin'],
        "ci_id": record['id'],
        "cluster_id": f"tr-all-v1-24-c{record['cluster']}",
        "date": record['date'],
        "end": record['end'],
        "pages": record['pages'],
        "cluster_size": record['size'],
        "text": record['text'],
        "title": title
    }
    

def remove_extra_cluster_cols(c_record: dict) -> dict:
    return {
        'id': c_record['id'],
        'cluster_time_delta': c_record['time_delta'],
        'cluster_lexial_overlap': c_record['lexical_overlap']
    }

def get_connected_clusters(passages_df):
    try:
        grouped_clusters = passages_df.groupby('ci_id')['cluster_id']
        conn_clusters = grouped_clusters.apply(list, meta=('connected_clusters', object)).persist()
        n_conn_clusters = grouped_clusters.apply('count', meta=('n_connected_clusters', int)).compute()
        conn_clusters_df = conn_clusters.to_frame().compute()
        conn_clusters_df[n_conn_clusters.name] = n_conn_clusters
        conn_clusters_df = conn_clusters_df.reset_index()
        conn_clusters_df.columns = ['ci_id', 'connected_clusters', 'n_connected_clusters']
        return conn_clusters_df.astype({'ci_id': "string[pyarrow]"})
    except Exception as e:
        print("Exception took place in get_connected_clusters: ", e)
        logger.error("Exception took place in get_connected_clusters: %s", e)
        raise e

def eval_str_lists(passage):
    passage['pages'] = literal_eval(passage['pages'])
    passage['connected_clusters'] = literal_eval(passage['connected_clusters'])
    #passage['doc_ids'] = literal_eval(passage['doc_ids'])
    
    return passage

def main() -> None:

    def signal_handler(*args):
        # Handle any cleanup here
        print(
            "SIGINT or CTRL-C detected. Exiting gracefully"
            " and shutting down the dask kubernetes cluster"
        )
        if client:
            client.shutdown()
            client.close()
        cluster.close()
        exit(0)

    arguments = docopt(__doc__)
    s3_bucket = arguments["--s3-bucket"] if arguments['--s3-bucket'] else "41-processed-data-staging"
    s3_patition = arguments["--s3-partition"] if arguments['--s3-partition'] else "text-reuse/text-reuse_v1-0-0"
    s3_run_partition = arguments["--s3-run-partition"] if arguments['--s3-run-partition'] else "passim_output_run_2"
    n_workers = int(arguments["--n-workers"]) if arguments['--n-workers'] else 10
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(logger, log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    logger.info("Provided parameters: %s", arguments)
    print(f"Provided parameters: {arguments}")

    memory_per_worker_gb = 32
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_per_worker_gb}GB")
    client = cluster.get_client()

    logger.info("Dask Client: %s and cluster: %s", client, cluster)
    print(f"Dask Client: {client} and cluster: {cluster}")

    s3_path = os.path.join(f"s3://{s3_bucket}", s3_patition, s3_run_partition)
    s3_passages_path = f"{s3_path}/out.json/"
    s3_clusters_path = f"{s3_path}/tr_clusters/"
    s3_output_path = f"{s3_path}/tr_passages/"
    
    logger.info("Starting to load the data from S3.")
    print("Starting to load the data from S3.")

    try:
        ### Reading passim data in memory
        passim_data = db.read_text(
            f"{s3_passages_path}*.json", storage_options=IMPRESSO_STORAGEOPT
        ).map(json.loads).persist() 

        passages_data_df = passim_data.map(format_passage_data).to_dataframe().persist()
        passages_data_df = client.gather(passages_data_df)

        ### Reading clusters data in memory
        clusters_data = db.read_text(
            f"{s3_clusters_path}*.jsonl.bz2", storage_options=IMPRESSO_STORAGEOPT
        ).map(json.loads).persist()

        clusters_df = clusters_data.map(remove_extra_cluster_cols).to_dataframe().set_index('id').persist()

        ### Creating 
        logger.info("Finished the loading data, getting the connected clusters..")
        print("Finished the loading data, getting the connected clusters.")

        # fetch the information on the connected clusters and reformat them]
        conn_clusters_df = get_connected_clusters(passages_data_df)
        print(f"conn_clusters_df.columns={conn_clusters_df.columns}")

        logger.info("Merging the fetched data with the passages..")
        print("Merging the fetched data with the passages..")
        try:
            joined_df = passages_data_df.join(clusters_df, on='cluster_id')
            print(f"joined_df.columns={joined_df.columns}")
        except Exception as e:
            print("Exception took place when joining passages_data_df and clusters_df: ", e)
            logger.error("Exception took place when joining passages_data_df and clusters_df: %s", e)
            raise e
        try:
            full_joined_df = joined_df.merge(conn_clusters_df, on='ci_id', how='left').persist()
        except Exception as e:
            print("Exception took place when merging joined_df and full_joined_df: ", e)
            logger.error("Exception took place when merging joined_df and full_joined_df: %s", e)
            raise e

        logger.info("Loading the dataframe back into bags and dumping it to files on S3.")
        print("Loading the dataframe back into bags and dumping it to files on S3.")
        print("just before putting passages to bag")
        full_passages_bag = full_joined_df.to_bag(format='dict').map(eval_str_lists).persist()

        passages_output_files = [
            f"{s3_output_path}{str(n).zfill(6)}.jsonl.bz2"
            for n in range(full_passages_bag.npartitions)
        ]
        print(f"passages_output_files[:10]: {passages_output_files[:10]}")
        logger.info("Writing the passages to %s S3 files.", len(passages_output_files))
        print(f"Writing the passages to {len(passages_output_files)} S3 files.")

        future = (
            full_passages_bag.map(json.dumps)
            .repartition(len(passages_output_files))
            .to_textfiles(passages_output_files, storage_options=IMPRESSO_STORAGEOPT)
        )

        logger.info("Finished writing all to files, closing the client and cluster.")
        print("Finished writing all to files, closing the client and cluster.")

        try: 
            client.shutdown()
            cluster.close()
            client.close()
        except Exception as e: 
            logger.warning("Exception while closing the client: %s", e)
            print(f"Exception while closing the client: {e}")

    except Exception as e: 
        logger.warning("Exception occurred, closing the client: %s", e)
        print(f"Exception occurred, closing the client: {e}")
        client.shutdown()
        cluster.close()
        client.close()

if __name__ == "__main__":
    main()