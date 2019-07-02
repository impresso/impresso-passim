
s3cmd get s3://passim-rebuilt/IMP/IMP-1900.jsonl.bz2 ../impresso-passim/sample_data/
s3cmd get s3://passim-rebuilt/GDL/GDL-1900.jsonl.bz2 ../impresso-passim/sample_data/

json-df-schema ../impresso-passim/sample_data/GDL-1900-12-12-a-i0029.json > ../impresso-passim/sample_data/passim.schema.orig

SPARK_SUBMIT_ARGS='--master local[36] --driver-memory 50G --executor-memory 50G --conf spark.local.dir=/scratch/matteo/spark-tmp/' passim --schema-path=/home/romanell/impresso_code/impresso-passim/sample_data/passim.schema  "/home/romanell/impresso_code/impresso-passim/sample_data/*.bz2" "/scratch/matteo/passim/impresso/"

cat /scratch/matteo/passim/impresso/out.json/part-00104-d7ac9716-69bb-442c-9c04-4e425433e21f-c000.json|jq --slurp ".[0:10]"
