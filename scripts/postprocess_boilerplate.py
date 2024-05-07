"""Command-line script to create the `pb.pkl` dataframe from the boilerplate's output.

Usage:
    postprocess_boilerplate.py --text-reuse-dir=<trd> --log-file=<lf>
    
Options:

--text-reuse-dir=<trd>  Local directory containing the outputs of the boilerplate in subdir `passim_bp_output`
--log-file=<lf>  Path to log file.
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
    arguments = docopt(__doc__)
    text_reuse_dir = arguments["--text-reuse-dir"]
    log_file = arguments["--log-file"]

    init_logger(logging.INFO, log_file)
    logger.info(
        "Creating the pb.pkl file from the outputs in %s", text_reuse_dir
    )

    out_jsons_dir = os.path.join(text_reuse_dir, "passim_bp_output/out.json")
    pb_pkl_out_filepath = os.path.join(text_reuse_dir, "pb.pkl")

    json_parts = list(
        map(lambda f: os.path.join(out_jsons_dir, f), 
        filter(lambda f: f.endswith('json'), os.listdir(out_jsons_dir)))
    )

    json_file_chunks = chunks(json_parts, 5)

    # The output df starts empty
    pb_df = None

    for chunk in tqdm(json_file_chunks):
        # read the chunk of 5 parts into a dask bag
        bp_bag = db.read_text(chunk).map(json.loads)
        # filter out all non-boilerplate ids: only keept entries where the field "src" is defined
        filtered = bp_bag.filter(lambda x: 'src' in x)

        formatted = filtered.map_partitions(format)

        chunck_df = formatted.to_dataframe(meta={"id": str, "is_boilerplate": bool}).persist()

        logger.info(f"Appending new data to pb.pkl: \n{chunck_df.tail()}")

        if pb_df is not None:
            pb_df = dd.concat([pb_df, chunck_df])
        else:
            pb_df = chunck_df

        logger.info(f"New length of the dataframe: {len(pb_df)}")

    # writing df to pickle file
    with open(pb_pkl_out_filepath, 'wb') as handle:
        pickle.dump(pb_df, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(pb_pkl_out_filepath, 'rb') as handle:
        b = pickle.load(handle)

    logger.info("pb.pkl successfully created and saved to disk at %s: %s", pb_pkl_out_filepath, all(pb_df == b))
    logger.info("-------- Done! --------")


if __name__ == "__main__":
    main()
