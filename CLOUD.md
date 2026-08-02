# Running benchmarks on AWS

Runbook for benchmarking on ephemeral AWS VMs with per-second billing. The
instance is created for a single campaign, uploads its results, and
terminates itself — there is nothing to keep or pay for between runs. Container
images automatically follow the Docker engine architecture; x86 and ARM runs
must still use distinct system labels because their results are not directly
comparable.

## Instance choice

Use the storage-optimized `i4i` family: dedicated x86 (Intel Ice Lake) with
large local NVMe included in the hourly price. Local NVMe matters — network
volumes (EBS) would bottleneck populate and distort disk-heavy queries. This
runbook uses x86 so all machine sizes in a campaign have the same CPU
architecture.

| Instance     | vCPU | RAM     | Local NVMe | On-demand (us-east-1, 2026) |
| ------------ | ---- | ------- | ---------- | --------------------------- |
| i4i.xlarge   | 4    | 32 GiB  | 937 GB     | ~$0.34/h                    |
| i4i.2xlarge  | 8    | 64 GiB  | 1 875 GB   | ~$0.69/h                    |
| i4i.4xlarge  | 16   | 128 GiB | 3 750 GB   | ~$1.37/h                    |

Spot is roughly half price but an interruption late in a long run wastes more
than it saves; at these totals (a ~10 h campaign per size costs $3–14) use
on-demand.

Results are labeled per machine via `OLAP_BENCHMARKS_SYSTEM` (e.g.
`aws-i4i-2xlarge`) and written to a per-machine revision
(`results/aws-i4i-2xlarge.db`), so runs from different sizes never conflict.

## One-time AWS setup

- Request an on-demand vCPU quota bump if the account is fresh (the default
  standard-family quota is often below 16 vCPUs).
- Key pair and SSH-only security group:

  ```bash
  aws ec2 create-key-pair --key-name olap-bench --query KeyMaterial --output text > ~/.ssh/olap-bench.pem
  chmod 600 ~/.ssh/olap-bench.pem
  aws ec2 create-security-group --group-name olap-bench --description "olap-benchmarks ssh"
  aws ec2 authorize-security-group-ingress --group-name olap-bench --protocol tcp --port 22 --cidr "$(curl -s https://checkip.amazonaws.com)/32"
  ```

- Create a fine-grained GitHub PAT scoped to this repository only (contents:
  read/write, short expiry). The VM uses it to clone and to upload the results
  database as a release asset; revoke it after the campaign.

## Launch

```bash
AMI=$(aws ssm get-parameter \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)

aws ec2 run-instances \
  --image-id "$AMI" \
  --instance-type i4i.2xlarge \
  --key-name olap-bench \
  --security-groups olap-bench \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=olap-bench-i4i-2xlarge}]'
```

`--instance-initiated-shutdown-behavior terminate` makes `sudo shutdown -h now`
from inside the VM terminate (not just stop) the instance, so the run script
can end the billing by itself.

## Provision

SSH in (`ssh -i ~/.ssh/olap-bench.pem ubuntu@<public-ip>`) and run, with
`SYSTEM` and `GITHUB_TOKEN` adjusted:

```bash
export SYSTEM=aws-i4i-2xlarge
export GITHUB_TOKEN=github_pat_...

sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir -p /mnt/data
sudo mount /dev/nvme1n1 /mnt/data
sudo chown ubuntu /mnt/data

# Docker, with images and container storage on the NVMe
curl -fsSL https://get.docker.com | sudo sh
echo '{"data-root": "/mnt/data/docker"}' | sudo tee /etc/docker/daemon.json
sudo systemctl restart docker
sudo usermod -aG docker ubuntu

# gh, used to upload the results database as a release asset. It picks up
# GITHUB_TOKEN from the environment, so no interactive login is needed.
sudo apt-get update && sudo apt-get install -y gh
gh auth status

# uv (fetches Python automatically) and the TPC data generators
curl -LsSf https://astral.sh/uv/install.sh | sh
curl -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env && source ~/.local/bin/env
cargo install --git https://github.com/clflushopt/tpchgen-rs --rev e53dea45345d3c934c724147e393983a53a40986 tpchgen-cli tpcgen-cli

git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/wlaur/olap-benchmarks.git"
cd olap-benchmarks
mkdir -p /mnt/data/input /mnt/data/dbs /mnt/data/temp

cat > .env <<EOF
OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY=/mnt/data/input
OLAP_BENCHMARKS_TEMPORARY_DIRECTORY=/mnt/data/temp
OLAP_BENCHMARKS_DATABASE_DIRECTORY=/mnt/data/dbs
OLAP_BENCHMARKS_RESULTS_DIRECTORY=$HOME/olap-benchmarks/results
OLAP_BENCHMARKS_SYSTEM=$SYSTEM
EOF

uv sync
```

Log out and back in once so the `docker` group membership applies.

## Run

Everything below runs inside `tmux` (`tmux new -s bench`) so a dropped SSH
connection does not kill the campaign.

Input data: `prepare` generates or downloads most suites; clickbench needs its
parquet fetched manually, and kaggle_airbnb requires a Kaggle download — `scp`
those CSVs from the laptop or skip the suite.

```bash
mkdir -p /mnt/data/input/clickbench
curl -o /mnt/data/input/clickbench/hits.parquet https://datasets.clickhouse.com/hits_compatible/hits.parquet

uv run olap prepare all
uv run olap prepare tpc_h --scale-factor 50
```

Then the campaign, with results uploaded and the instance terminated at the end.
Results databases are not committed: they travel as assets on the `runs` release
(`olap results upload`), which is why the instance needs `gh`. The compact
step rewrites the database into a fresh file first — DuckDB keeps space freed by
checkpoints and re-runs, so this typically shrinks it 2–3x and makes the upload
correspondingly faster:

```bash
REV=$SYSTEM
uv run olap benchmark all all --revision "$REV" --cleanup
uv run olap benchmark all tpc_h --scale-factor 50 --revision "$REV" --cleanup

uv run olap results compact --revision "$REV"
uv run olap results upload --revision "$REV"
sudo shutdown -h now
```

Use a revision name unique to the host and campaign: uploading the same name
twice replaces the existing asset.

Chain the commands with `&&` (or put them in a script) when leaving the run
unattended overnight, so a finished run terminates itself instead of idling.
If a benchmark step fails, the upload and shutdown still matter — run them
manually rather than leaving the instance up.

## Back on the laptop

```bash
uv run olap results fetch --revision aws-i4i-2xlarge
uv run olap results migrate --revision aws-i4i-2xlarge
uv run olap results runs --revision aws-i4i-2xlarge
```

Sanity-check the runs, then fold them into the site data. `--merge` adds them to
what is already published and needs the current published database locally, so
fetch it first:

```bash
cd site && bun run fetch-data && cd ..
uv run olap publish --revision aws-i4i-2xlarge --merge --upload
```

Verify in the AWS console that the instance is terminated, and revoke the PAT
once the campaign is done.

## Cost expectations

Provisioning takes ~15 min (dominated by `cargo install`), a full-matrix
campaign several hours per machine size. A three-size campaign at ~10 h each
lands around $25 on-demand, plus a few dollars of EBS root and data transfer.
The only recurring cost after termination is nothing — data on the local NVMe
is destroyed with the instance, which is why the results are uploaded first.
