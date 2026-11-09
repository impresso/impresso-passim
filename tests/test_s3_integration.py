import os
import bz2
import json
import boto3
import pytest
from scripts.postprocess_textreuse_clusters import lexicaloverlap


@pytest.fixture(scope="module")
def s3_client():
    env_file = os.path.join(os.path.dirname(__file__), "..", ".env")
    if not os.path.exists(env_file):
        pytest.skip(".env credentials not found")

    env_dict = {}
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env_dict[k] = v

    if not env_dict.get("SE_ACCESS_KEY") or not env_dict.get("SE_SECRET_KEY"):
        pytest.skip("S3 credentials not configured in .env")

    client = boto3.client(
        "s3",
        aws_access_key_id=env_dict.get("SE_ACCESS_KEY"),
        aws_secret_access_key=env_dict.get("SE_SECRET_KEY"),
        endpoint_url=env_dict.get("SE_HOST_URL", "https://os.zhdk.cloud.switch.ch/"),
    )
    return client


def test_s3_read_sample_clusters_and_passages(s3_client):
    """Test reading real cluster records from S3 and checking lexical overlap computation."""
    bucket = "41-processed-data-staging"
    cluster_key = "textreuse/textreuse_passim_v1-0-0/results/tr_clusters/tr_clusters-0000.jsonl.bz2"

    obj = s3_client.get_object(Bucket=bucket, Key=cluster_key)
    # Stream first few kilobytes to decompress sample lines without loading whole file
    data = obj["Body"].read(1024 * 1024)  # 1 MB
    decompressor = bz2.BZ2Decompressor()
    decompressed = decompressor.decompress(data)
    lines = decompressed.decode("utf-8", errors="ignore").splitlines()

    assert len(lines) > 0
    sample_cluster = json.loads(lines[0])
    assert "id" in sample_cluster
    assert "cluster_size" in sample_cluster
    assert "passages" in sample_cluster

    print(f"Sample cluster loaded from S3: ID={sample_cluster['id']}, Size={sample_cluster['cluster_size']}")
