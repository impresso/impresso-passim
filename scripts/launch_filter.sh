#!/bin/bash
# script to setup env. variables and arguments and launch the filter_boilerplate.py script on runai
# /!\ This script should be modified and adapted to each run.

#export SE_ACCESS_KEY='' # add your access key here
#export SE_SECRET_KEY='' # add your secret key here

export output_bucket='s3://30-passim-rebuilt-sandbox/passim-no-bp' # TODO fill in
export input_bucket='s3://30-passim-rebuilt-sandbox/passim' # TODO fill in

export bp_s3_path='s3://40-processed-data-sandbox/text-reuse/text-reuse_v1-0-0/boilerplate/bp.pkl'

export log_file='/home/piconti/impresso-passim/logs/debug.log'


echo "Launch the process"

# ensure you are in the right dir
cd /home/piconti/impresso-passim/scripts

python filter_boilerplate.py --input-bucket=$input_bucket --output-bucket=$output_bucket --bp-s3-path=$bp_s3_path --log-file=$log_file --verbose