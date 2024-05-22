"""Command-line script to generated configuration files for ingestion/rebuild scripts.

Usage:
    filter_boilerplate.py --input-bucket=<ib> --output-bucket=<ob> --bp-s3-path=<bp> --log-file=<f> [--nworkers=<w> --scheduler=<s> --verbose]

Options:

--input-bucket=<ib>  S3 bucket where passim rebuilt data will be read from
--output-bucket=<ob>   S3 bucket where passim filtered data will written to
--bp-s3-path=<bp> S3 path of the bp.pkl dataframe
--log-file=<f>  Path to log file
--nworkers=<w>  number of workers for (local) Dask client.
--scheduler=<s>  Tell dask to use an existing scheduler (otherwise it'll create one)
--verbose  Set logging level to DEBUG (by default is INFO)

Example:

    python filter_boilerplate.py --input-bucket="s3://30-passim-rebuilt-sandbox/passim" --output-bucket="s3://30-passim-rebuilt-sandbox/passim-no-bp"  --bp-s3-path="s3://40-processed-data-sandbox/text-reuse/text-reuse_v1-0-0/boilerplate/pb.pkl"  --log-file=/dhlab-data/data/piconti-data/impresso-passim/logs/debug_filter_bp.log --verbose

"""  # noqa: E501

import os
import json
import signal
import logging
import pandas as pd
from dask import bag as db
from dask import dataframe as dd
from dask.distributed import Client, progress, LocalCluster
from dask import config
#config.set({"dataframe.convert-string": False})
from docopt import docopt
#from impresso_commons.path.path_s3 import list_newspapers
from impresso_commons.utils import Timer
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT, fixed_s3fs_glob, get_s3_client
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

def chunk(list: list, chunksize: int, as_list: bool = True):
    """Yield successive n-sized chunks from list."""
    return [list[i : i + chunksize] for i in range(0, len(list), chunksize)]


def list_newspapers(
    bucket_name: str,
    s3_client=get_s3_client(),
    page_size: int = 10000, 
    partition: str | None = None
) -> list[str]:
    """List newspapers contained in an s3 bucket with impresso data.

    Note:
        25,000 seems to be the maximum `PageSize` value supported by
        SwitchEngines' S3 implementation (ceph).
    Note:
        Copied from https://github.com/impresso/impresso-data-sanitycheck/tree/master/sanity_check/contents/s3_data.py

    Args:
        bucket_name (str): Name of the S3 bucket to consider
        s3_client (optional): S3 client to use. Defaults to get_s3_client().
        page_size (int, optional): Pagination configuration. Defaults to 10000.

    Returns:
        list[str]: List of newspaper (aliases) present in the given S3 bucket.
    """
    print(f"Fetching list of newspapers from {bucket_name}")

    if "s3://" in bucket_name:
        s = bucket_name.replace("s3://", "").split("/")
        bucket_name = s[0]
        if len(s) > 1 and s[1]!='':
            partition = '/'.join(s[1:])
            print(f"Setting partition to be {partition}")
            logger.info("Setting partition to be %s", partition)

    paginator = s3_client.get_paginator("list_objects")

    newspapers = set()
    for n, resp in enumerate(
        paginator.paginate(Bucket=bucket_name, PaginationConfig={"PageSize": page_size})
    ):
        # means the bucket is empty
        if "Contents" not in resp:
            continue

        for f in resp["Contents"]:
            if partition is not None:
                if partition in f["Key"]:
                    journal = f["Key"].replace(partition, '').split("/")[0]
                else:
                    # if the partition is defined it should be in the key
                    continue
            else:
                journal = f["Key"].split("/")[0]
            # exclude pontential manifests
            if not journal.endswith('.json') or '.' not in journal:
                newspapers.add(journal)
            else:
                print(f"Ignoring {journal} because it contains a file extension.")
                logger.info("Ignoring %s because it contains a file extension.", journal)
        msg = (
            f"Paginated listing of keys in {bucket_name}: page {n + 1}, listed "
            f"{len(resp['Contents'])}"
        )
        logger.info(msg)

    print(f"{bucket_name} contains {len(newspapers)} newspapers")

    return newspapers

def filter_boilerplate(input_bucket, output_bucket, bp_s3_path, client):

    t = Timer()

    print(f"Loading {bp_s3_path} to dataframe")
    #bp_df = pd.read_pickle(bp_s3_path, storage_options=IMPRESSO_STORAGEOPT).reset_index().repartition(npartitions=2082)
    #bp_df = pd.read_pickle(bp_s3_path, storage_options=IMPRESSO_STORAGEOPT).repartition(npartitions=2082).persist()
    bp_df = pd.read_pickle(bp_s3_path, storage_options=IMPRESSO_STORAGEOPT).repartition(npartitions=2082).drop(columns=["is_boilerplate"])
    bp_df = client.persist(bp_df)

    nps = list_newspapers(input_bucket)
    print(f"newspapers found in {input_bucket}: {nps}")

    for np in nps:

        # detect whether the current item has already been processed
        existing_files = fixed_s3fs_glob(f'{os.path.join(output_bucket, np)}*.bz2')

        # skip newspapers that don't need to be processed
        if np in TITLES_NO_BP:
            logger.info('%s, no article segmentation, skipping', np)
            print(f'{np}, no article segmentation, skipping')
            print('------------------------------------')
            logger.info('------------------------------------')
            continue
        elif len(existing_files) > 0:
            logger.info('%s already done, move on', np)
            print(f'{np} already done, move on')
            print('------------------------------------')
            logger.info('------------------------------------')
            continue

        passim_rebuilt_files = fixed_s3fs_glob(f'{os.path.join(input_bucket, np)}/*.bz2')

        # we want to keep the number of resulting files as the one of input files
        n_partitions = len(passim_rebuilt_files)
        output_files = [
                f'{os.path.join(output_bucket, np)}-{str(n+1).zfill(4)}.jsonl.bz2' for n, f in enumerate(passim_rebuilt_files)
            ]

        if n_partitions > 50:
            rebuilt_chunks = chunk(passim_rebuilt_files, 40)
            out_files_chunks = chunk(output_files, 40)
            n_chunks=len(rebuilt_chunks)
            print(f"{np} contains {len(passim_rebuilt_files)} files. Since it's >50, it will be handled in {n_chunks} steps.")
            logger.info(f"{np} contains {len(passim_rebuilt_files)} files. Since it's >50, it will be handled in {n_chunks} steps.")
            print(f"rebuilt_chunks = {rebuilt_chunks}")
        else:
            rebuilt_chunks = [passim_rebuilt_files]
            out_files_chunks = [output_files]
            n_chunks=1

        print(f"Filtering bp to keep the wanted ids for {np}.")
        np_bp_set = bp_df[bp_df.id.str.contains(np)].persist()
        np_bp_set = set(np_bp_set.to_bag().map(lambda x: x[0]).compute())
        #np_bp_set = np_bp_set.map(lambda x: x[0]).compute()
        #bp_df['title'] = bp_df['id'].apply(lambda x: x.split('-')[0], axis=0)
        #np_bp_set = set(bp_df[bp_df.title == np].to_bag().compute())

        c=1
        # does not work when n_chunks>1
        for rebuilt_f_chunk, out_f_chunk in zip(rebuilt_chunks, out_files_chunks):
            
            print(f'Crunching {np} ({c}/{n_chunks}): {len(rebuilt_f_chunk)} files')
            logger.info(f'Crunching {np} ({c}/{n_chunks}): {len(rebuilt_f_chunk)} files')
            print(f"rebuilt_f_chunk: {rebuilt_f_chunk} –––––– out_f_chunk: {out_f_chunk}")

            #np_bp_df = client.compute(np_bp_df).result()

            print("Reading rebuilt data into a DF.")
            passim_data = (
                db.read_text(rebuilt_f_chunk, storage_options=IMPRESSO_STORAGEOPT)
                .map(json.loads)
                .persist()
            )

            print("Filtering the passim data to keep only non-bp CIs.")
            passim_filtered = passim_data.filter(lambda d: d['id'] not in np_bp_set)

            print("Writing the created files to S3.")
            future = (
                passim_filtered.map(json.dumps)
                .repartition(len(rebuilt_f_chunk))
                .to_textfiles(out_f_chunk, storage_options=IMPRESSO_STORAGEOPT)
            )

            logger.info(f'Written {len(out_f_chunk)} output files; first five: {out_f_chunk[:5]}')
            print(f'Written {len(out_f_chunk)} output files; first five: {out_f_chunk[:5]}')
            c+=1
            del passim_data
            del passim_filtered
            del future
            client.cancel(passim_data)
            try:
                client.cancel(future)
                print("cancelled 'future'")
                client.cancel(passim_filtered)
                print("cancelled 'filtered_df'")
            except:
                print("could not cancel 'future'")

        logger.info(f'Written {len(output_files)} output files; first five: {output_files[:5]}')
        print(f'Written {len(output_files)} output files; first five: {output_files[:5]}')

        print(f"using del np_bp_df here to reduce the memory usage")
        del np_bp_set

        print(f'Done with {np}. It took: {t.tick()}')
        print('------------------------------------')
        logger.info(f'Done with {np}. It took: {t.tick()}')
        logger.info('------------------------------------')

        
def main():

    def signal_handler(*args):
        # Handle any cleanup here
        print('SIGINT or CTRL-C detected. Exiting gracefully' ' and shutting down the dask kubernetes cluster')
        if client:
            client.shutdown()
        exit(0)

    arguments = docopt(__doc__)
    input_bucket = arguments['--input-bucket']
    output_bucket = arguments['--output-bucket']
    bp_s3_path = arguments["--bp-s3-path"]
    log_file = arguments["--log-file"]
    workers = int(arguments['--nworkers']) if arguments['--nworkers'] else 10
    scheduler = arguments["--scheduler"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    # start the dask local cluster
    if scheduler is None:
        cluster = LocalCluster(n_workers=workers, threads_per_worker=3, scheduler_port="8786", memory_limit='40GB')
        client = cluster.get_client()
        #client = Client(n_workers=workers, threads_per_worker=2)
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client}"
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    try:
        filter_boilerplate(input_bucket, output_bucket, bp_s3_path, client)

    except Exception as e:
        raise e
    finally:
        if client:
            print("Closing client")
            client.shutdown()


if __name__ == '__main__':
    main()
