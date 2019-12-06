"""Command-line script to generated configuration files for ingestion/rebuild scripts.

Usage:
    filter_boilerplate.py --input-bucket=<ib> --output-bucket=<ob> [--k8-memory=<mem> --k8-workers=<wkrs>]

Options:

--input-bucket=<ib>  S3 bucket where passim rebuilt data will be read from
--output-bucket=<ob>   S3 bucket where passim filtered data will written to


Example:

    python filter_boilerplate.py --input-bucket="s3://passim-rebuilt/" --output-bucket="s3://passim-rebuilt-nobp/" \
     --k8-memory="1G" --k8-workers=25
"""  # noqa: E501

import os
import json
import signal
import pandas as pd
from dask import bag as db
from dask_k8 import DaskCluster
from dask.dataframe import from_pandas
from docopt import docopt
from sanity_check.contents.s3_data import list_newspapers
from impresso_commons.utils import Timer
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT, fixed_s3fs_glob

from impresso_commons.utils.kube import (
    make_scheduler_configuration,
    make_worker_configuration,
)


def signal_handler(*args):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully' ' and shutting down the dask kubernetes cluster')
    if cluster:
        cluster.close()
    exit(0)


def filter_boilerplate(input_bucket, output_bucket):

    boilerplate_dataframe_path = '/home/romanell/Downloads/bp.pkl'
    t = Timer()
    bp_df = from_pandas(pd.read_pickle(boilerplate_dataframe_path), chunksize=10000).reset_index().persist()

    nps = list_newspapers(input_bucket)

    for np in nps:
        passim_rebuilt_files = fixed_s3fs_glob(f'{os.path.join(input_bucket, np)}/*.bz2')

        # we want to keep the number of resulting files as the one of input files
        n_partitions = len(passim_rebuilt_files)
        print(f'Crunching {np}: {len(passim_rebuilt_files)} files')

        # detect whether the current item has already been processed
        existing_files = fixed_s3fs_glob(f'{output_bucket}{np}*.bz2')

        # skip newspapers that don't need to be processed
        if np == 'NZZ':
            print('NZZ, skipping')
            continue
        elif len(existing_files) > 0:
            print(f'{np} already done, move on')
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
            f'{output_bucket}{np}-{str(n+1).zfill(4)}.jsonl.bz2' for n, f in enumerate(passim_rebuilt_files)
        ]

        future = (
            filtered_df.reset_index()
            .to_bag()
            .map(lambda i: i[1])
            .map(json.dumps)
            .repartition(n_partitions)
            .to_textfiles(output_files, storage_options=IMPRESSO_STORAGEOPT)
        )

        print(f'Written {len(output_files)} output files; first five: {output_files[:5]}')

        print(f'Done with {np}. It took: {t.tick()}')
        print('------------------------------------')


def main():
    arguments = docopt(__doc__)
    memory = arguments['--k8-memory'] if arguments['--k8-memory'] else "50G"
    workers = int(arguments['--k8-workers']) if arguments['--k8-workers'] else 100
    input_bucket = arguments['--input-bucket']
    output_bucket = arguments['--output-bucket']

    signal.signal(signal.SIGINT, signal_handler)
    image_uri = "ic-registry.epfl.ch/dhlab/impresso_data-sanity-check:v1"

    try:
        # first thing to do is to create the dask kubernetes cluster
        cluster = DaskCluster(
            namespace="dhlab",
            cluster_id="impresso-filter-passim-boilerplate",
            scheduler_pod_spec=make_scheduler_configuration(),
            worker_pod_spec=make_worker_configuration(docker_image=image_uri, memory=memory),
        )
        cluster.create()
        cluster.scale(workers, blocking=True)
        dask_client = cluster.make_dask_client()
        dask_client.get_versions(check=True)
        print(dask_client)

        filter_boilerplate(input_bucket, output_bucket)

    except Exception as e:
        raise e
    finally:
        if cluster:
            cluster.close()


if __name__ == '__main__':
    main()
