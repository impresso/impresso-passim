"""Command-line script to separate all the text-reuse passages by title-year.

Usage:
    passages_by_title_year.py --log-file=<lf>  [--s3-bucket=<sb> --input-partition=<ip> --output-partition=<op> --n-workers=<nw> --resume-title=<rt> --verbose]
    
Options:

--log-file=<lf>  Path to log file.
--s3-bucket=<sb>  S3 bucket (excluding the partition) where to fetch the passim output. Defaults to `41-processed-data-staging`.
--input-partition=<ip>  Partition within the bucket where the passim output to postprocess is. Defaults to `text-reuse/text-reuse_v1-0-0/`.
--output-partition=<op>  Partition within the bucket where to upload it. Defaults to `passim_output_run_2`
--n-workers=<nw>  Number of workers for Dask cluster. Defaults to 24.
--resume-title=<rt> Title alias to resume if any.
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
from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT, fixed_s3fs_glob
from datetime import datetime, timedelta
from ast import literal_eval
import dask.config
from docopt import docopt
from impresso_essentials.utils import init_logger, KNOWN_JOURNALS_DICT

logger = logging.getLogger(__name__)


def get_present_title_years(s3_glob_path):

    def year_from_path(s3_path):
        return s3_path.split('-')[-1].split('.')[0]
    
    split_passages_filenames = fixed_s3fs_glob(s3_glob_path)

    present_title_years = {p.split('/')[-2]: [] for p in split_passages_filenames}

    for p in split_passages_filenames:
        present_title_years[p.split('/')[-2]].append(year_from_path(p))

    return present_title_years


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
    input_partition = arguments["--input-partition"] if arguments['--input-partition'] else "text-reuse/text-reuse_v1-0-0/passim_output_run_2/tr_passages"
    output_partition = arguments["--output-partition"] if arguments['--output-partition'] else "textreuse/textreuse_passim_v1-0-0/results/tr_passages"
    n_workers = int(arguments["--n-workers"]) if arguments['--n-workers'] else 8 #10
    resume_title = arguments["--resume-title"] if arguments["--resume-title"] else ''
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

    s3_input_path = os.path.join(f"s3://{s3_bucket}", input_partition, '*.jsonl.bz2')
    s3_output_part = os.path.join(f"s3://{s3_bucket}", output_partition)

    msg = f"s3_input_path: {s3_input_path}, s3_output_partition: {s3_output_part}"
    print(msg)
    logger.info(msg)
    
    logger.info("Starting to load the data from S3.")
    print("Starting to load the data from S3.")

    try:

        present_title_years = get_present_title_years(os.path.join(s3_output_part, "*.jsonl.bz2"))

        ### Reading passim data in memory
        all_passages = db.read_text(s3_input_path, storage_options=IMPRESSO_STORAGEOPT
        ).map(json.loads).persist() 


        for provider, title_list in KNOWN_JOURNALS_DICT.items():
            msg = f"\n------------ {provider} ------------\n"
            print(msg)
            logger.info(msg)
            for title_idx, title in enumerate(title_list):
                # skip some already processed titles, unless the one to resume
                if title not in present_title_years.keys() or title == resume_title:
                    msg = f"\n---- Filtering passages for {title} ({title_idx+1}/{len(title_list)}) ----"
                    print(msg)
                    logger.info(msg)
                    passages_for_title = all_passages.filter(lambda x: title in x['ci_id'])
                    # group by year and remove years without passages
                    passages_for_title_year = passages_for_title.groupby(lambda x: x['date'].split('-')[0]).filter(lambda x: len(x[1])!=0).persist()

                    years_to_filenames = passages_for_title_year.map(lambda x: (x[0], 
                        [f"{s3_output_part}/{title}/tr_passages-{title}-{x[0]}.jsonl.bz2"])
                    ).compute()

                    for year, filename in years_to_filenames:
                        # only skip years of title to resume
                        if title != resume_title or year not in present_title_years[resume_title]:
                            msg = f"{title}-{year}, writing all passages to s3_path: {filename}"
                            print(msg)
                            logger.info(msg)
                            out_files = (
                                passages_for_title_year.filter(lambda x: x[0]== year)
                                .map(lambda x: x[1])
                                .flatten()
                                .repartition(1)
                                .map(json.dumps)
                                .to_textfiles(filename,storage_options=IMPRESSO_STORAGEOPT)
                            )
                        else:
                            msg = f"{title}-{year} - Skipping, as it's already done."
                            logger.info(msg)
                            print(msg)

                    msg = f"{title} - Finished writing all passages to files, going to next title."
                    logger.info(msg)
                    print(msg)
                else:
                    msg = f"{title} - Skipping, as it's already done."
                    logger.info(msg)
                    print(msg)
                        #break
                    #break

        logger.info("Finished writing all the passages to files, closing the client and cluster.")
        print("Finished writing all the passages to files, closing the client and cluster.")
        
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