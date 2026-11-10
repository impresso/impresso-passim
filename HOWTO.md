# Impresso Text-Reuse & Cluster Refinement Operations Guide (DevOps / DataOps Runbook)

This runbook guides DevOps and Data Engineers on deploying, configuring, monitoring, and scaling the **Impresso Text-Reuse & Post-Hoc Cluster Refinement Pipeline**.

---

## 1. Architecture & System Overview

The text-reuse pipeline ingests historical newspaper corpus data from S3, generates seed reuse clusters via Passim, and refines them using post-hoc temporal/community clustering and two-phase collinear OCR passage alignment.

```
                           S3 BUCKET (Switch Cloud)
                 ┌─────────────────────────────────────────┐
                 │ s3://41-processed-data-staging/         │
                 │   ├── textreuse/results/tr_clusters/    │
                 │   └── textreuse/results/tr_passages/    │
                 └───────────────────┬─────────────────────┘
                                     │ Streaming JSONL (.bz2)
                                     ▼
                      DASK ORCHESTRATION PIPELINE
             (scripts/dask_orchestration_refinement.py)
 ┌────────────────────────────────────────────────────────────────────────┐
 │ 1. Streaming JSONL Ingestion (Dask Bag via fsspec/s3fs)               │
 │ 2. Selective Monster Detection (cluster_size > 100 or time_delta > 365)│
 │ 3. Temporal Episodic Bisection (max_gap_days threshold)                │
 │ 4. Louvain Community Detection (k-shingle Jaccard graph)               │
 │ 5. Medoid Alignment Refinement (Phase 1: Collinear DP, Phase 2: OCR)   │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ Streamed output (.jsonl.bz2)
                                     ▼
                 ┌─────────────────────────────────────────┐
                 │ s3://41-processed-data-staging/         │
                 │   └── textreuse/results/refined/        │
                 └───────────────────┬─────────────────────┘
                                     │
                                     ▼
                         DOWNSTREAM SOLR INDEXER
```

---

## 2. Infrastructure Requirements & Prerequisites

### 2.1 Compute Sizing Recommendations

| Environment | Workload Size | Nodes / Instances | CPU / Worker | RAM / Worker |
| :--- | :--- | :--- | :--- | :--- |
| **Dev / Staging** | Sample / Test batches | 1 Node (LocalCluster) | 4–8 cores | 16–32 GB |
| **Production** | Full Corpus (~10M+ passages) | Slurm / Kubernetes cluster | 32–64 cores total | 4 GB RAM per worker process |

- **Storage**: Minimum 50 GB local scratch space for temporary Dask worker spill (`/tmp` or `/scratch`).
- **Network**: Low latency to Switch Cloud S3 endpoint (`https://os.zhdk.cloud.switch.ch/`).

### 2.2 Python Runtime & Dependencies

- Python: `>= 3.10` (Tested on `3.11.16`)
- Required packages (see `requirements.txt`):
  - `dask[distributed]`, `dask[bag]`
  - `s3fs`, `fsspec`
  - `networkx`, `python-louvain`
  - `pytest` (for test validation)
  - `impresso-essentials` (optional, fallback credentials in place)

---

## 3. Environment Configuration & Secrets Management

The pipeline communicates with Switch Cloud S3 via standard AWS S3-compatible credentials.

### 3.1 Required Environment Variables

Create or provision an environment file (e.g. `.env` or Kubernetes Secret):

```bash
# S3 Switch Cloud Credentials
export AWS_ACCESS_KEY_ID="<YOUR_S3_ACCESS_KEY>"
export AWS_SECRET_ACCESS_KEY="<YOUR_S3_SECRET_KEY>"
export S3_ENDPOINT="https://os.zhdk.cloud.switch.ch/"
export S3_BUCKET="41-processed-data-staging"

# Dask Optimization & Scratch Directory
export DASK_TEMPORARY_DIRECTORY="/tmp/dask-scratch"
export PYTHONPATH="."
```

> [!CAUTION]
> Never commit `.env` or credentials into version control. In CI/CD or production environments, mount these as Kubernetes Secrets, Slurm environment variables, or HashiCorp Vault secrets.

---

## 4. Quickstart & Smoke Testing

Before running a full corpus job, execute the test suite and verify S3 read/write capabilities:

```bash
# 1. Clone repository and checkout release branch
git clone https://github.com/impresso/impresso-passim.git
cd impresso-passim
git checkout fix/code-quality-and-runtime-bugs

# 2. Activate virtual environment
source .venv/bin/activate

# 3. Execute test suite
PYTHONPATH=. pytest tests/ -v
```

Expected output:
```text
tests/test_alignment_refinement.py .....       [ 25%]
tests/test_dask_orchestration.py ..            [ 35%]
tests/test_lexicaloverlap.py .....             [ 60%]
tests/test_post_hoc_clustering.py ...          [ 75%]
tests/test_s3_integration.py ..                [ 85%]
tests/test_two_phase_alignment.py ...          [100%]
============================== 20 passed in 1.51s ==============================
```

---

## 5. Deployment Options

### Option A: Standalone VM / Single Node (LocalCluster)

Best for staging, testing, or modest workloads:

```bash
PYTHONPATH=. python scripts/dask_orchestration_refinement.py \
    --log-file=/var/log/impresso/refinement.log \
    --s3-bucket=41-processed-data-staging \
    --s3-cluster-input=textreuse/textreuse_passim_v1-0-0/results/tr_clusters/ \
    --s3-output-dir=textreuse/textreuse_passim_v1-0-0/results/refined/ \
    --n-workers=8 \
    --batch-size=1000 \
    --max-gap-days=60 \
    --similarity-threshold=0.15 \
    --verbose
```

### Option B: Distributed Dask on Slurm Cluster

For large-scale production HPC runs:

1. **Start Dask Scheduler and Workers across Slurm nodes**:
   Create a Slurm job script (`run_dask_refinement.slurm`):
   ```bash
   #!/bin/bash
   #SBATCH --job-name=impresso-refine
   #SBATCH --nodes=4
   #SBATCH --ntasks-per-node=8
   #SBATCH --cpus-per-task=2
   #SBATCH --mem=32G
   #SBATCH --time=12:00:00
   #SBATCH --output=/scratch/impresso/logs/%j.out

   module load python/3.11
   source /scratch/impresso/venv/bin/activate

   # Launch Dask Scheduler
   dask scheduler --scheduler-file=$SLURM_TMPDIR/scheduler.json &
   sleep 5

   # Launch Workers on each allocated node
   srun dask worker --scheduler-file=$SLURM_TMPDIR/scheduler.json \
       --nthreads 2 --memory-limit 4GB &
   sleep 10

   # Execute orchestration client connecting to scheduler
   PYTHONPATH=. python scripts/dask_orchestration_refinement.py \
       --log-file=/scratch/impresso/logs/refinement_${SLURM_JOB_ID}.log \
       --s3-bucket=41-processed-data-staging \
       --s3-cluster-input=textreuse/textreuse_passim_v1-0-0/results/tr_clusters/ \
       --s3-output-dir=textreuse/textreuse_passim_v1-0-0/results/refined/ \
       --n-workers=32 \
       --batch-size=2000
   ```

2. **Submit Job**:
   ```bash
   sbatch run_dask_refinement.slurm
   ```

### Option C: Containerized / Docker Deployment

Dockerfile recipe for containerized environments:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libbz2-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH="/app"

ENTRYPOINT ["python", "scripts/dask_orchestration_refinement.py"]
```

Build and run:
```bash
docker build -t impresso-passim-refinement:latest .
docker run --rm \
    --env-file .env \
    -v /tmp/logs:/var/log \
    impresso-passim-refinement:latest \
    --log-file=/var/log/refine.log \
    --n-workers=4
```

---

## 6. Pipeline CLI Flags & Tuning Guide

| Flag | Type | Default | Description & Tuning Advice |
| :--- | :--- | :--- | :--- |
| `--s3-bucket` | `str` | `41-processed-data-staging` | Target S3 bucket name. |
| `--s3-cluster-input` | `str` | `textreuse/.../tr_clusters/` | Prefix path where input cluster `.jsonl.bz2` files reside. |
| `--s3-output-dir` | `str` | `textreuse/.../refined/` | Destination prefix for refined `.jsonl.bz2` partitions. |
| `--max-gap-days` | `int` | `60` | Max days of quiet between passages before creating a new temporal episode. Lower (e.g. 30) for fast-moving wire news; higher (e.g. 180) for slow serial literature. |
| `--similarity-threshold` | `float` | `0.15` | Minimum token Jaccard similarity edge cutoff for Louvain community splitting. Recommended 0.10–0.20. |
| `--n-workers` | `int` | `8` | Number of concurrent Dask worker processes. Set according to CPU cores. |
| `--batch-size` | `int` | `1000` | Number of clusters per partition task. Adjust up for higher throughput, down to limit memory pressure. |
| `--log-file` | `str` | *Required* | Absolute destination for execution logs. |
| `--verbose` | `flag` | `False` | Enables detailed DEBUG log traces. |

---

## 7. Monitoring, Metrics & Observability

### 7.1 Dask Web Dashboard

When launching `LocalCluster` or a remote Dask scheduler, a live diagnostic dashboard is available:
- **Default URL**: `http://localhost:8787/status` (or dynamically allocated port printed in logs).
- **Key Tabs to Monitor**:
  - **Tasks**: Check for task progress, retries, and task duration distribution.
  - **Memory (Workers)**: Verify memory per worker stays below thresholds (avoid red spikes indicating unmanaged memory or spills).
  - **System / CPU**: Ensure all allocated cores are fully saturated.

### 7.2 Log Analysis & Progress Checking

Monitor the designated log file with real-time counters:
```bash
tail -f /var/log/impresso/refinement.log | grep -E "INFO|WARNING|ERROR"
```

Sample output:
```text
2026-11-10 10:15:20 INFO [dask_orchestration_refinement] Initialized Dask cluster: 8 workers, 16 threads
2026-11-10 10:15:22 INFO [dask_orchestration_refinement] Found 42 input partitions in s3://41-processed-data-staging/...
2026-11-10 10:15:45 INFO [dask_orchestration_refinement] Completed partition 1/42: 1000 clusters processed, 14 monster clusters bisected into 48 cohesive sub-clusters.
```

---

## 8. Failure Modes, Troubleshooting & Disaster Recovery

| Symptom | Root Cause | Remediation Step |
| :--- | :--- | :--- |
| **Worker OOM / `KilledWorker` Error** | Monster cluster partition exceeds worker RAM limit during Louvain graph matrix allocation. | 1. Increase `--memory-limit` per worker.<br>2. Decrease `--batch-size` (e.g., `--batch-size=500`).<br>3. Ensure `DASK_TEMPORARY_DIRECTORY` has fast NVMe/SSD scratch space. |
| **S3 403 Forbidden / SignatureDoesNotMatch** | S3 credentials invalid or system clock drift. | 1. Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.<br>2. Test with `aws --endpoint-url https://os.zhdk.cloud.switch.ch/ s3 ls s3://41-processed-data-staging/`.<br>3. Sync host clock with NTP. |
| **S3 503 SlowDown / Rate Limit** | Too many Dask workers requesting S3 partitions simultaneously. | 1. Increase fsspec retry count: set `boto3` max attempts.<br>2. Group partitions into larger chunks or decrease `--n-workers`. |
| **Solr Ingestion Mismatch** | Downstream Solr schema missing new refined fields. | Ensure Solr schema incorporates `refined_begin`, `refined_end`, `subcluster_id`, and `is_monster_drift` (see Section 9). |

### Idempotency & Resuming Interrupted Runs

The Dask refinement pipeline is **fully idempotent**. Each S3 output file corresponds to a deterministic partition hash. If a job fails midway:
1. Output files already written to `--s3-output-dir` do not corrupt state.
2. You can resume by either specifying an uncompleted S3 prefix or letting the job overwrite existing keys safely.

---

## 9. Downstream Solr Schema Changes

For engineers managing the Solr search index, the refinement pipeline outputs extra metadata attributes per passage:

```xml
<!-- solr/schema.xml additions for passage core -->
<field name="refined_begin" type="pint" indexed="true" stored="true" docValues="true" />
<field name="refined_end" type="pint" indexed="true" stored="true" docValues="true" />
<field name="subcluster_id" type="string" indexed="true" stored="true" />
<field name="parent_cluster_id" type="string" indexed="true" stored="true" />
<field name="is_monster_drift" type="boolean" indexed="true" stored="true" />
```

---

## 10. DevOps Handover Checklist

- [ ] Repository cloned and switched to `fix/code-quality-and-runtime-bugs` branch.
- [ ] Pytest verification passes (`PYTHONPATH=. pytest tests/`).
- [ ] `.env` credentials verified against Switch Cloud S3 endpoint.
- [ ] Dask worker scratch directory configured with sufficient storage.
- [ ] Batch size and worker count tuned for target compute environment.
- [ ] Downstream Solr schema updated to index refined passage boundaries.
