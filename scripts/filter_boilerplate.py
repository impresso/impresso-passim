"""Command-line script to generated configuration files for ingestion/rebuild scripts.

Usage:
    filter_boilerplate.py --input-bucket=<ib> --output-bucket=<ob> --bp-s3-path=<bp> --log-file=<f> [--nworkers=<w> --scheduler=<s> --verbose]

Options:

--input-bucket=<ib>  S3 bucket where passim rebuilt data will be read from
--output-bucket=<ob>   S3 bucket where passim filtered data will written to
--bp-s3-path=<bp> S3 path of the bp.pkl dataframe
--log-file=<f>  Path to log file
--nworkers=<nw>  number of workers for (local) Dask client.
--scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
--verbose  Set logging level to DEBUG (by default is INFO)

Example:

    python filter_boilerplate.py --input-bucket="s3://30-passim-rebuilt-sandbox/passim" 
        --output-bucket="s3://30-passim-rebuilt-sandbox/passim-no-bp"  
        --bp-s3-path="s3://40-processed-data-sandbox/text-reuse/text-reuse_v1-0-0/boilerplate/pb.pkl" 
        --log-file=/dhlab-data/data/piconti-data/impresso-passim/logs/debug_filter_bp.log --verbose

"""  # noqa: E501

import os
import json
import signal
import logging
import pandas as pd
from dask import bag as db
from dask.dataframe import from_pandas
from dask.distributed import Client, progress
from docopt import docopt
from impresso_commons.path.path_s3 import list_newspapers
from impresso_commons.utils import Timer
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT, fixed_s3fs_glob
from impresso_commons.utils.utils import init_logger

logger = logging.getLogger(__name__)

# titles which do not have article-level segmentation
TITLES_NO_BP = [
    "FedGazDe", "FedGazFr", "NZZ", "handelsztg", "arbeitgeber", "ACI", "AV", "Bombe", 
    "Cancoire", "Castigat", "Charivari", "CharivariCH", "CL", "Croquis", "EM", "esta", "FAM", 
    "FAMDE", "FAN", "FAV1", "Fronde", "GAVi", "Grelot", "Griffe", "Guepe1851", "Guepe1887", 
    "JH", "JV", "JVE", "JY2", "MB", "ME", "MESSAGER", "Moniteur", "NS", "NV", "NV1", "NV2", 
    "OBS", "ouistiti", "pages", "PAT", "PDL", "PJ", "PS", "RLA", "TouSuIl", "VVS", "VVS1"
]

def filter_boilerplate(input_bucket, output_bucket, bp_s3_path):

    t = Timer()
    bp_df = (
        from_pandas(pd.read_pickle(bp_s3_path, storage_options=IMPRESSO_STORAGEOPT), chunksize=10000)
        .reset_index().persist()
    )

    nps = list_newspapers(input_bucket)

    for np in nps:
        passim_rebuilt_files = fixed_s3fs_glob(f'{os.path.join(input_bucket, np)}/*.bz2')

        # we want to keep the number of resulting files as the one of input files
        n_partitions = len(passim_rebuilt_files)
        print(f'Crunching {np}: {len(passim_rebuilt_files)} files')
        logger.info(f'Crunching {np}: {len(passim_rebuilt_files)} files')

        # detect whether the current item has already been processed
        existing_files = fixed_s3fs_glob(f'{os.path.join(output_bucket, np)}*.bz2')

        # skip newspapers that don't need to be processed
        if np in TITLES_NO_BP:
            logger.info('%s, no article segmentation, skipping', np)
            print('%s, no article segmentation, skipping', np)
            continue
        elif len(existing_files) > 0:
            logger.info('%s already done, move on', np)
            print('%s already done, move on', np)
            continue

        passim_data_df = (
            db.read_text(passim_rebuilt_files, storage_options=IMPRESSO_STORAGEOPT)
            .map(json.loads)
            .map(lambda d: {'id': d['id'], 'document': d})
            .to_dataframe()
            .set_index('id')
            .persist()
        )

        np_bp_df = bp_df[bp_df.id.str.contains(np)].set_index('id').compute()

        tmp_df = passim_data_df.join(np_bp_df, how='outer')

        filtered_df = tmp_df[tmp_df.is_boilerplate.isnull()]

        output_files = [
            f'{os.path.join(output_bucket, np)}-{str(n+1).zfill(4)}.jsonl.bz2' for n, f in enumerate(passim_rebuilt_files)
        ]

        future = (
            filtered_df.reset_index()
            .to_bag()
            .map(lambda i: i[1])
            .map(json.dumps)
            .repartition(n_partitions)
            .to_textfiles(output_files, storage_options=IMPRESSO_STORAGEOPT)
        )

        logger.info(f'Written {len(output_files)} output files; first five: {output_files[:5]}')
        print(f'Written {len(output_files)} output files; first five: {output_files[:5]}')

        print(f'Done with {np}. It took: {t.tick()}')
        print('------------------------------------')
        logger.info(f'Done with {np}. It took: {t.tick()}')
        logger.info('------------------------------------')


def main():

    def signal_handler(*args):
        # Handle any cleanup here
        print('SIGINT or CTRL-C detected. Exiting gracefully' ' and shutting down the dask kubernetes cluster')
        if client:
            client.close()
        exit(0)

    arguments = docopt(__doc__)
    input_bucket = arguments['--input-bucket']
    output_bucket = arguments['--output-bucket']
    bp_s3_path = arguments["--bp-s3-path"]
    log_file = arguments["--log-file"]
    workers = int(arguments['--nworkers']) if arguments['--nworkers'] else 50
    scheduler = arguments["--scheduler"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    # start the dask local cluster
    if scheduler is None:
        client = Client(n_workers=workers, threads_per_worker=2)
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client}"
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    try:
        filter_boilerplate(input_bucket, output_bucket, bp_s3_path)

    except Exception as e:
        raise e
    finally:
        if client:
            client.close()


if __name__ == '__main__':
    main()
