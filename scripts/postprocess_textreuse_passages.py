"""Command-line script to create the `pb.pkl` dataframe from the boilerplate's output.

Usage:
    postprocess_textreuse.py --log-file=<lf>  [--s3-bucket=<sb> --s3-partition=<sp> --s3-run-partition=<srp> --n-workers=<nw> --verbose]
    
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

dask.config.set(temporary_directory='/scratch/piconti/impresso/dask_tmp')


def unify_data(record: dict) -> dict:
    """Unify the records in the data to all have the same column order.

    Args:
        record (dict): record to unify 

    Returns:
        dict: _description_
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

    record['title'] = title

    if 'id' in record and 'begin' in record and 'end' in record:
        record['passages'] = "{}@{}:{}".format(record['id'], record['begin'], record['end'])
    else:
        record['passages'] = ''

    return record

def mint_document_ids(row) -> str:
    
    ids = row['id']
    begins = row['begin']
    ends = row['end']
    
    return ",".join([
        "{}@{}:{}".format(doc_id, begin, end)
        for doc_id, begin, end in zip(ids, begins, ends)
    ])
    

def lexicaloverlap(texts):
    #texts = row['text']
    first = True
    intersection = list()
    
    longest_text_length = max([len(text) for text in texts])
    
    token_sets = [
        set(re.sub('[().,;:!0-9"{}\][»«]','',text).lower().split())
        for text in texts
    ]
    
    longest_text_length = max([len(ts) for ts in token_sets])
    intersection = set.intersection(*token_sets)
    if longest_text_length == 0:
        return 0
    
    overlap_pct = (len(intersection) * 100) / longest_text_length
    return overlap_pct

def get_timedelta(min_date: str, max_date: str) -> timedelta:
    min = datetime.strptime(min_date, '%Y-%m-%d').date()
    max = datetime.strptime(max_date, '%Y-%m-%d').date()
    return max - min

def eval_str_lists(cluster):
    cluster['newspapers'] = literal_eval(cluster['newspapers'])
    cluster['passages'] = literal_eval(cluster['passages'])
    cluster['doc_ids'] = literal_eval(cluster['doc_ids'])
    
    return cluster

def groupby_cluster_apply(data_df: dd.DataFrame, col_name: str, apply_func: Callable[..., Any]) -> pd.DataFrame:
    logger.info("Grouping by cluster, for %s column.", col_name)

    return data_df.groupby('cluster')[col_name].apply(apply_func, meta=(col_name, object)).compute()
    c_df.loc[:, new_col] = series
    #np_sub = passim_data_df.groupby('cluster').apply(lambda r: sorted(r['series'].unique()), meta=('np', object)).compute()
    #clusters_df.loc[:, 'newspapers'] = np_sub
    return c_df


def main() -> None:

    def signal_handler(*args):
        # Handle any cleanup here
        print(
            "SIGINT or CTRL-C detected. Exiting gracefully"
            " and shutting down the dask kubernetes cluster"
        )
        if client:
            client.shutdown()
        cluster.close()
        exit(0)

    arguments = docopt(__doc__)
    s3_bucket = arguments["--s3-bucket"] if arguments['--s3-bucket'] else "41-processed-data-staging"
    s3_patition = arguments["--s3-partition"] if arguments['--s3-partition'] else "text-reuse/text-reuse_v1-0-0"
    s3_run_partition = arguments["--s3-run-partition"] if arguments['--s3-run-partition'] else "passim_output_run_2"
    n_workers = int(arguments["--n-workers"]) if arguments['--n-workers'] else 24
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    logger.info("Provided parameters: %s", arguments)
    print(f"Provided parameters: {arguments}")

    memory_per_worker_gb = 16
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_per_worker_gb}GB")
    client = cluster.get_client()

    logger.info("Dask Client: %s and cluster: %s", client, cluster)
    print(f"Dask Client: {client} and cluster: {cluster}")

    s3_path = os.path.join(f"s3://{s3_bucket}", s3_patition, s3_run_partition)
    s3_input_path = f"{s3_path}/out.json/"
    s3_output_path = f"{s3_path}/tr_clusters/"
    
    logger.info("Starting to load the data from S3.")
    print("Starting to load the data from S3.")
    ### Reading data in memory
    passim_data = db.read_text(
        f"{s3_input_path}/*.json", storage_options=IMPRESSO_STORAGEOPT
    ).map(json.loads).persist() 

    passim_data_df = passim_data.map(unify_data).to_dataframe().persist()
    passim_data_df = client.gather(passim_data_df)
    passim_data_df = passim_data_df.set_index('uid').persist()

    ### Creating 
    logger.info("Finished the loading data, performing the first groupby.")
    print("Finished the loading data, performing the first groupby.")
    #clusters_df = passim_data_df.head(100000, compute=False, npartitions=10).sort_values('cluster')
    clusters_df = passim_data_df.groupby('cluster').agg({'date': ['min', 'max'], 'size': 'count'}).compute()

    ### Adding the new columns also needing groupbys
    logger.info("Finished the first groupby clusters, performing the next ones.")
    print("Finished the first groupby clusters, performing the next ones.")
    # get the list of newspapers for which a cluster contains TR instances
    #np_sub = passim_data_df.groupby('cluster').apply(lambda r: sorted(r['series'].unique()), meta=('np', object)).compute()
    clusters_df.loc[:, 'newspapers'] = groupby_cluster_apply(passim_data_df, 'series', lambda r: sorted(r.unique()))
    logger.info("Finished grouping by `series` and added the column `newspapers` to cluster DF.")
    print("Finished grouping by `series` and added the column `newspapers` to cluster DF.")

    # get the list of passages for each cluster
    #passages_sub = passim_data_df.groupby('cluster')['passages'].apply(list, meta=('passages', object)).compute()
    clusters_df.loc[:, 'passages'] = groupby_cluster_apply(passim_data_df, 'passages', list)
    logger.info("Finished grouping by `passages` and added the column `passages` to cluster DF.")
    print("Finished grouping by `passages` and added the column `passages` to cluster DF.")

    # compute the lexical overlap of passages within clusters
    #overlap_sub = passim_data_df.groupby('cluster')['text'].apply(lexicaloverlap, meta=('lo', object)).compute()
    clusters_df.loc[:, 'lexical_overlap'] = groupby_cluster_apply(passim_data_df, 'text', lexicaloverlap)
    logger.info("Finished grouping by `text` and added the column `lexical_overlap` to cluster DF.")
    print("Finished grouping by `text` and added the column `lexical_overlap` to cluster DF.")

    # compute the list of document IDs
    #doc_ids_sub = passim_data_df.groupby('cluster')['id'].apply(lambda r: sorted(r.unique()), meta=('doc_ids', object)).compute()
    clusters_df.loc[:, 'doc_ids'] = groupby_cluster_apply(passim_data_df, 'id', lambda r: sorted(r.unique()))
    logger.info("Finished grouping by `id` and added the column `doc_ids` to cluster DF.")
    print("Finished grouping by `id` and added the column `doc_ids` to cluster DF.")


    logger.info("Reformatting and retyping the columns.")
    print("Reformatting and retyping the columns.")
    # compute the time delta in days between the first and last occurence of TR within clusters.
    clusters_df.loc[:,('date', 'min')] = pd.to_datetime(clusters_df.date['min'], format='%Y-%m-%d')
    clusters_df.loc[:,('date', 'max')] = pd.to_datetime(clusters_df.date['max'], format='%Y-%m-%d')
    clusters_df.loc[:,'time_delta'] = clusters_df.date['max'] - clusters_df.date['min']

    clusters_df.loc[:, 'cluster_size'] = clusters_df['size']['count']
    clusters_df.loc[:, 'min_date'] = clusters_df['date']['min'].astype('string')
    clusters_df.loc[:, 'max_date'] = clusters_df['date']['max'].astype('string')
    clusters_df['time_delta'] = clusters_df['time_delta'].apply(lambda x: x.days).astype('int64')
    final_clusters_df =  clusters_df.drop([('date', 'min'),('date', 'max'), ('size', 'count')], axis=1)

    final_clusters_df.loc[:, 'id'] = [f"tr-all-v1-24-c{x}" for x in final_clusters_df.index]

    final_columns = ["id", "min_date", "max_date", "cluster_size", "time_delta", "newspapers", "passages", "doc_ids", "lexical_overlap"]
    #final_clusters_df = final_clusters_df[final_columns]
    #final_clusters_df = final_clusters_df.reset_index(drop = True)
    final_clusters_df = final_clusters_df[final_columns].reset_index(drop = True)
    final_clusters_df.columns = final_clusters_df.columns.droplevel(1)

    logger.info("Loading the dataframe back into bags and dumping it to files on S3.")
    print("Loading the dataframe back into bags and dumping it to files on S3.")

    clusters_ddf = dd.from_pandas(final_clusters_df, chunksize=100000)
    clusters_bag = clusters_ddf.to_bag(format='dict').map(eval_str_lists).persist()

    cluster_output_files = [
        f"{s3_output_path}{str(n).zfill(4)}.jsonl.bz2"
        for n in range(clusters_bag.npartitions)
    ]

    logger.info("Writing the clusters to S3 files.")
    print("Writing the clusters to S3 files.")

    future = (
        clusters_bag.map(json.dumps)
        .repartition(len(cluster_output_files))
        .to_textfiles(cluster_output_files, storage_options=IMPRESSO_STORAGEOPT)
    )

    logger.info("Finished writing all to files, closing the client and cluster.")
    print("Finished writing all to files, closing the client and cluster.")

    try: 
        client.shutdown()
        cluster.close()
        client.close()
    except Exception as e: 
        logger.warn("Exception while closing the client: %s", e)
        print(f"Exception while closing the client: {e}")


if __name__ == "__main__":
    main()