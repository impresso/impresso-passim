"""Command-line script to create the `pb.pkl` dataframe from the boilerplate's output.

Usage:
    postprocess_boilerplate.py --text-reuse-dir=<trd> --log-file=<lf> --s3_bucket=<sb>  --s3_patition=<sp> --bp_filename=<bf>
    
Options:

--text-reuse-dir=<trd>  Local directory containing the outputs of the boilerplate in subdir `passim_bp_output`
--log-file=<lf>  Path to log file.
--s3_bucket=<sb>  S3 bucket where to upload the boilerplate pickle after creation. Defaults to `41-processed-data-staging`
--s3_patition=<sp>  Partition within the bucket where to upload it. Defaults to `text-reuse/text-reuse_v1-0-0/boilerplate/`
--bp_filename=<bf>  Filename to use for the boilerplate pickle output. Defaults to `bp.pkl`
"""

import os
import logging
from tqdm import tqdm
import json
from typing import Any, Iterator
import pickle

from docopt import docopt

import dask.bag as db
import dask.dataframe as dd

from impresso_commons.utils.utils import init_logger
from impresso_commons.utils.s3 import get_s3_resource

logger = logging.getLogger()

def chunks(lst: list[Any], n: int) -> Iterator[list[Any]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def format(entries: list[dict[str,Any]]) -> list[dict[str, str | bool]]:
    """Reformat the given partition of entries to match needs.

    In particular, all entries provided should contain fields `'id'` and `'src'`
    where:
    - `'id'` is the canonical ID (with offsets appended to it) of a boilerplate CI.
    - `'scr'` contains another field `'id'` corresponding to the match idenfitified.

    Both IDs correspond to boilerplate and should be included in the resulting list.

    Args:
        entries (list[dict[str,Any]]): Entries from passim boilerplate detection output.

    Returns:
        list[dict[str, str | bool]]: IDs corresponding to identified boilerplate CIs.
    """
    out = []
    for e in entries:
        e_id = e['id'].split('_')[0]
        # append first id
        out.append({
            'id': e_id, 
            "is_boilerpate": True,
        })
        # add second id
        out.append({
            "id": e['src']['id'],
            "is_boilerplate": True
        })

    return out


def main():
    # TODO: write 1 bp.pkl file PER TITLE or PER Provider (too large in the long run)
    # Optional TODO: switch approach to bags & JSON files: may allow to prevent massive join
    arguments = docopt(__doc__)
    text_reuse_dir = arguments["--text-reuse-dir"]
    s3_bucket = arguments["--s3_bucket"] if arguments['--s3_bucket'] else "41-processed-data-staging"
    s3_bp_patition = arguments["--s3_patition"] if arguments['--s3_patition'] else "text-reuse/text-reuse_v1-0-0/boilerplate/"
    bp_filename = arguments["--bp_filename"] if arguments['--bp_filename'] else "bp.pkl"
    log_file = arguments["--log-file"]

    init_logger(logging.INFO, log_file)
    logger.info(
        "Creating the pb.pkl file from the outputs in %s", text_reuse_dir
    )

    out_jsons_dir = os.path.join(text_reuse_dir, "passim_bp_output/out.json")
    bp_pkl_out_filepath = os.path.join(text_reuse_dir, bp_filename)

    json_parts = list(
        map(lambda f: os.path.join(out_jsons_dir, f), 
        filter(lambda f: f.endswith('json'), os.listdir(out_jsons_dir)))
    )

    json_file_chunks = chunks(json_parts, 5)

    # The output df starts empty
    bp_df = None

    for chunk in tqdm(json_file_chunks):
        # read the chunk of 5 parts into a dask bag
        bp_bag = db.read_text(chunk).map(json.loads)
        # filter out all non-boilerplate ids: only keept entries where the field "src" is defined
        filtered = bp_bag.filter(lambda x: 'src' in x)

        formatted = filtered.map_partitions(format)

        chunck_df = formatted.to_dataframe(meta={"id": str, "is_boilerplate": bool}).persist()

        logger.info(f"Appending new data to pb.pkl: \n{chunck_df.tail()}")

        if bp_df is not None:
            bp_df = dd.concat([bp_df, chunck_df])
        else:
            bp_df = chunck_df

        logger.info(f"New length of the dataframe: {len(bp_df)}")

    logger.info(f"Dropping the duplicated ids before wirting it to disk. Length before: {len(bp_df)}")
    filtered_dup_bp = bp_df.drop_duplicates(subset=['id']).persist()
    logger.info(f"Filtering of duplicated done. Length before: {len(bp_df)}, length now: {len(filtered_dup_bp)}")

    # writing df to pickle file
    with open(bp_pkl_out_filepath, 'wb') as handle:
        pickle.dump(filtered_dup_bp, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(bp_pkl_out_filepath, 'rb') as handle:
        b = pickle.load(handle)

    logger.info("pb.pkl successfully created and saved to disk at %s: %s", bp_pkl_out_filepath, all(filtered_dup_bp == b))

    bp_key_name = os.path.join(s3_bp_patition, bp_filename)
    logger.info(f"Uploading newly created file to S3 at path: {os.path.join("s3://", s3_bucket, bp_key_name)}")

    s3 = get_s3_resource()
    bucket = s3.Bucket(s3_bucket)
    bucket.upload_file(bp_pkl_out_filepath, bp_key_name)

    logger.info("-------- Done! --------")


if __name__ == "__main__":
    main()
