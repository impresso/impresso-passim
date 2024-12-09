"""Command-line script to separate all the text-reuse passages by title-year.

Usage:
    passages_by_title_year.py --log-file=<lf>  [--s3-bucket=<sb> --s3-partition=<sp> --s3-run-partition=<srp> --n-workers=<nw> --verbose]
    
Options:

--log-file=<lf>  Path to log file.
--s3-bucket=<sb>  S3 bucket (excluding the partition) where to fetch the passim output. Defaults to `41-processed-data-staging`.
--s3-partition=<sp>  Partition within the bucket where the passim output to postprocess is. Defaults to `text-reuse/text-reuse_v1-0-0/`.
--s3-run-partition=<srp>  Partition within the bucket where to upload it. Defaults to `passim_output_run_2`
--n-workers=<nw>  Number of workers for Dask cluster. Defaults to 24.
--verbose  Set logging level to DEBUG (by default is INFO)
"""