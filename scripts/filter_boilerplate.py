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
from dask.diagnostics import ProgressBar
from dask import config
import psutil

# config.set({"dataframe.convert-string": False})
from docopt import docopt

# from impresso_commons.path.path_s3 import list_newspapers
from impresso_commons.utils import Timer
from impresso_commons.utils.s3 import (
    IMPRESSO_STORAGEOPT,
    fixed_s3fs_glob,
    get_s3_client,
)
from impresso_commons.utils.utils import init_logger
import gc

logger = logging.getLogger(__name__)

# titles which do not have article-level segmentation
TITLES_NO_BP = [
    "FedGazDe",
    "FedGazFr",
    "NZZ",
    "handelsztg",
    "arbeitgeber",
    "ACI",
    "AV",
    "Bombe",
    "Cancoire",
    "Castigat",
    "Charivari",
    "CharivariCH",
    "CL",
    "Croquis",
    "EM",
    "esta",
    "FAM",
    "FAMDE",
    "FAN",
    "FAV1",
    "Fronde",
    "GAVi",
    "Grelot",
    "Griffe",
    "Guepe1851",
    "Guepe1887",
    "JH",
    "JV",
    "JVE",
    "JY2",
    "MB",
    "ME",
    "MESSAGER",
    "Moniteur",
    "NS",
    "NV",
    "NV1",
    "NV2",
    "OBS",
    "ouistiti",
    "pages",
    "PAT",
    "PDL",
    "PJ",
    "PS",
    "RLA",
    "TouSuIl",
    "VVS",
    "VVS1",
]


def chunk(list: list, chunksize: int, as_list: bool = True):
    """Yield successive n-sized chunks from list."""
    return [list[i : i + chunksize] for i in range(0, len(list), chunksize)]


def list_newspapers(
    bucket_name: str,
    s3_client=get_s3_client(),
    page_size: int = 10000,
    partition: str | None = None,
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
        if len(s) > 1 and s[1] != "":
            partition = "/".join(s[1:])
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
                    journal = f["Key"].replace(partition, "").split("/")[0]
                else:
                    # if the partition is defined it should be in the key
                    continue
            else:
                journal = f["Key"].split("/")[0]
            # exclude pontential manifests
            if not journal.endswith(".json") or "." not in journal:
                newspapers.add(journal)
            else:
                print(f"Ignoring {journal} because it contains a file extension.")
                logger.info(
                    "Ignoring %s because it contains a file extension.", journal
                )
        msg = (
            f"Paginated listing of keys in {bucket_name}: page {n + 1}, listed "
            f"{len(resp['Contents'])}"
        )
        logger.info(msg)

    print(f"{bucket_name} contains {len(newspapers)} newspapers")

    return newspapers


def process_newspaper(np, input_bucket, output_bucket, bp_df_future, client):
    existing_files = fixed_s3fs_glob(f"{os.path.join(output_bucket, np)}*.bz2")

    t = Timer()

    if np in TITLES_NO_BP:
        logger.info("%s, no article segmentation, skipping", np)
        print(f"{np}, no article segmentation, skipping")
        print("------------------------------------")
        return
    elif len(existing_files) > 0:
        logger.info("%s already done, move on", np)
        print(f"{np} already done, move on")
        print("------------------------------------")
        return

    passim_rebuilt_files = fixed_s3fs_glob(f"{os.path.join(input_bucket, np)}/*.bz2")

    # always chunk!
    n_partitions = len(passim_rebuilt_files)
    output_files = [
        f"{os.path.join(output_bucket, np)}-{str(n + 1).zfill(4)}.jsonl.bz2"
        for n in range(n_partitions)
    ]

    chunk_size = 50  # move it to args?
    rebuilt_chunks = list(chunk(passim_rebuilt_files, chunk_size))
    out_files_chunks = list(chunk(output_files, chunk_size))
    n_chunks = len(rebuilt_chunks)

    if n_chunks > 1:
        print(
            f"{np} contains {n_partitions} files. It will be handled in {n_chunks} steps."
        )
        logger.info(
            f"{np} contains {n_partitions} files. It will be handled in {n_chunks} steps."
        )
    else:
        print(
            f"{np} contains {n_partitions} files. It will be handled in a single step."
        )
        logger.info(
            f"{np} contains {n_partitions} files. It will be handled in a single step."
        )

    print(f"Filtering bp to keep the wanted ids for {np}.")

    bp_df = client.gather(bp_df_future)
    filtered_df = bp_df[bp_df.id.str.contains(np)]

    np_bp_set = set(filtered_df["id"].compute())

    del filtered_df
    gc.collect()  # let's collect garbage

    for c, rebuilt_f_chunk in enumerate(rebuilt_chunks):

        out_f_chunk = out_files_chunks[c]
        print(f"Crunching {np} ({c + 1}/{n_chunks}): {len(rebuilt_f_chunk)} files")
        logger.info(
            f"Crunching {np} ({c + 1}/{n_chunks}): {len(rebuilt_f_chunk)} files"
        )

        print("Reading rebuilt data into a DF.")
        passim_data = db.read_text(
            rebuilt_f_chunk, storage_options=IMPRESSO_STORAGEOPT
        ).map(json.loads)

        print("Filtering the passim data to keep only non-bp CIs.")
        passim_filtered = passim_data.filter(lambda d: d["id"] not in np_bp_set)

        print("Writing the created files to S3.")
        # WARNING! the files are not always written back with the columns in the same order
        # + sometimes the `title` is missing. Should add a step to ensure this.
        future = (
            passim_filtered.map(json.dumps)
            .repartition(len(rebuilt_f_chunk))
            .to_textfiles(out_f_chunk, storage_options=IMPRESSO_STORAGEOPT)
        )

        logger.info(f"Written {len(out_f_chunk)} output files; first five: {out_f_chunk[:5]}")
        print(f"Written {len(out_f_chunk)} output files; first five: {out_f_chunk[:5]}")

        del passim_data
        del passim_filtered
        del future
        gc.collect()  # some more garbage

        try:
            client.cancel(passim_data)
            client.cancel(passim_filtered)
            client.cancel(future)
            print("Cancelled 'future'")
        except:
            print("Could not cancel 'future'")

    logger.info(
        f"Written {len(output_files)} output files; first five: {output_files[:5]}"
    )
    print(f"Written {len(output_files)} output files; first five: {output_files[:5]}")

    print(f"Using del np_bp_set here to reduce the memory usage")
    del np_bp_set
    gc.collect()  # Force garbage collection

    time = t.tick()
    print(f"Done with {np}. It took: {time}")
    print("------------------------------------")
    logger.info(f"Done with {np}. It took: {time}")
    logger.info("------------------------------------")


def filter_boilerplate(input_bucket, output_bucket, bp_s3_path, client):

    print(f"Loading {bp_s3_path} to dataframe")
    bp_df = (
        pd.read_pickle(bp_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .drop(columns=["is_boilerplate"])
        .repartition(npartitions=2082)
    )
    bp_df = client.persist(bp_df)

    nps = list_newspapers(input_bucket)
    print(f"{len(nps)} Newspapers found in {input_bucket}.")

    # Let's remove some randomness in the order of the newspapers
    nps_list = sorted(list(nps))
    print(f"Ordered list of newspapers: {nps_list}")

    for np in nps_list:
        print(f"Processing newspaper: {np}")
        try:
            process_newspaper(np, input_bucket, output_bucket, bp_df, client)
        except Exception as e:
            logger.error(f"Error processing {np}: {e}")
            print(f"Error processing {np}: {e}")
            print("------------------------------------")
            logger.info("------------------------------------")


def main():

    def signal_handler(*args):
        # Handle any cleanup here
        print(
            "SIGINT or CTRL-C detected. Exiting gracefully"
            " and shutting down the dask kubernetes cluster"
        )
        if client:
            client.shutdown()
        exit(0)

    arguments = docopt(__doc__)
    input_bucket = arguments["--input-bucket"]
    output_bucket = arguments["--output-bucket"]
    bp_s3_path = arguments["--bp-s3-path"]
    log_file = arguments["--log-file"]
    workers = int(arguments["--nworkers"]) if arguments["--nworkers"] else 10
    scheduler = arguments["--scheduler"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    signal.signal(signal.SIGINT, signal_handler)
    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    if scheduler is None:

        # Get system resources
        total_cores = psutil.cpu_count(logical=False)  # Physical cores
        total_memory = psutil.virtual_memory().total  # Total memory in bytes
        print(f"Total cores: {total_cores}")
        print(f"Total memory: {total_memory / (1024**3):.2f} GB")

        # Determine memory limit per worker and calculate number of workers
        memory_per_worker_gb = 5  # Memory limit per worker
        memory_per_worker_bytes = memory_per_worker_gb * (1024**3)  # Convert to bytes
        workers = total_memory // memory_per_worker_bytes  # Calculate number of workers
        threads_per_worker = 3  # One thread per worker to start

        print(f"Memory limit per worker: {memory_per_worker_gb} GB")
        print(f"Number of workers: {workers}")

        cluster = LocalCluster(
            n_workers=workers,
            threads_per_worker=threads_per_worker,
            memory_limit=f"{memory_per_worker_gb}GB",
            # processes=True,  # Use separate processes for each worker
            # dashboard_address=":8787",  # Enable Dask dashboard
            # local_directory="/home/piconti/.dask-spill",  # Enable spill to disk
        )
        client = cluster.get_client()
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client} with {workers} workers."
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    filter_boilerplate(input_bucket, output_bucket, bp_s3_path, client)

    if client:
        print("Closing client")
        client.shutdown()


if __name__ == "__main__":
    main()