"""Command-line script to create the `pb.pkl` dataframe from the boilerplate's output.

Usage:
    postprocess_textreuse.py --s3_bucket=<sb> --text-reuse-dir=<trd> --log-file=<lf> --s3_patition=<sp> --bp_filename=<bf>
    
Options:

--s3_bucket=<sb>  S3 bucket (excluding the partition) where to fetch the passim output.
--s3_patition=<sp>  Partition within the bucket where the passim output to postprocess is. Defaults to `text-reuse/text-reuse_v1-0-0/boilerplate/`
--text-reuse-dir=<trd>  Local directory containing the outputs of the boilerplate in subdir `passim_bp_output`
--log-file=<lf>  Path to log file.
--s3_patition=<sp>  Partition within the bucket where to upload it. Defaults to `text-reuse/text-reuse_v1-0-0/boilerplate/`
--bp_filename=<bf>  Filename to use for the boilerplate pickle output. Defaults to `bp.pkl`
"""

import os
import json
import signal
import logging
import pandas as pd
import re
import numpy as np
from dask import dataframe as dd
from dask import bag as db
from dask.distributed import Client, LocalCluster
# from dask_k8 import DaskCluster
from impresso_commons.path.path_s3 import IMPRESSO_STORAGEOPT
from datetime import datetime, timedelta
from ast import literal_eval
import dask.config
from docopt import docopt
from impresso_commons.utils.utils import init_logger

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
    s3_bucket = arguments["--s3_bucket"] if arguments['--s3_bucket'] else "41-processed-data-staging"
    s3_patition = arguments["--s3_patition"] if arguments['--s3_patition'] else "text-reuse/text-reuse_v1-0-0"
    s3_run_partition = arguments["--s3_run_partition"] if arguments['--s3_run_partition'] else "passim_output_run_2"
    n_workers = int(arguments["--n-workers"]) if arguments['--n-workers'] else 24
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    logger.info("Provided parameters: %s", arguments)

    memory_per_worker_gb = 16
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_per_worker_gb}GB")
    client = cluster.get_client()

    logger.info("Dask Client: %s", client)

    s3_path = os.path.join(f"s3://{s3_bucket}", s3_patition, s3_run_partition)
    s3_input_path = f"{s3_path}/out.json/"
    s3_output_path = f"{s3_path}/tr_clusters/"

    passim_data = db.read_text(
        f"{s3_input_path}/*.json", storage_options=IMPRESSO_STORAGEOPT
    ).map(json.loads).persist() 

    passim_data_df = passim_data.map(unify_data).to_dataframe().persist()
    passim_data_df = client.gather(passim_data_df)
    passim_data_df = passim_data_df.set_index('uid').persist()

      #clusters_df = passim_data_df.head(100000, compute=False, npartitions=10).sort_values('cluster')
    clusters_df = passim_data_df.groupby('cluster').agg({'date': ['min', 'max'], 'size': 'count'}).compute()

    # add the new columns also needing groupbys

    # get the list of newspapers for which a cluster contains TR instances
    np_sub = passim_data_df.groupby('cluster').apply(lambda r: sorted(r['series'].unique()), meta=('np', object)).compute()
    clusters_df.loc[:, 'newspapers'] = np_sub

    # get the list of passages for each cluster
    passages_sub = passim_data_df.groupby('cluster')['passages'].apply(list, meta=('passages', object)).compute()
    clusters_df.loc[:, 'passages'] = passages_sub

    # compute the lexical overlap of passages within clusters
    overlap_sub = passim_data_df.groupby('cluster')['text'].apply(lexicaloverlap, meta=('lo', object)).compute()
    clusters_df.loc[:, 'lexical_overlap'] = overlap_sub

    doc_ids_sub = passim_data_df.groupby('cluster')['id'].apply(lambda r: sorted(r.unique()), meta=('doc_ids', object)).compute()
    clusters_df.loc[:, 'doc_ids'] = doc_ids_sub

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

    clusters_ddf = dd.from_pandas(final_clusters_df, chunksize=100000)
    clusters_bag = clusters_ddf.to_bag(format='dict').map(eval_str_lists).persist()

    cluster_output_files = [
        f"{s3_output_path}{str(n).zfill(4)}.jsonl.bz2"
        for n in range(clusters_bag.npartitions)
    ]

    future = (
        clusters_bag.map(json.dumps)
        .repartition(len(cluster_output_files))
        .to_textfiles(cluster_output_files, storage_options=IMPRESSO_STORAGEOPT)
    )

if __name__ == "__main__":
    main()