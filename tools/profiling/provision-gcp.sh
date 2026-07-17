#!/usr/bin/env bash
#
# provision-gcp.sh — create a DEDICATED-vCPU GCE VM for Valkey profiling,
# copy valkey-profile.sh onto it, and run the setup step.
#
# Uses a c3 (Intel Sapphire Rapids) instance: fixed-clock, non-burstable vCPUs
# that mirror Memorystore's x86 hardware — the right choice for benchmarking.
# Swap MACHINE=c2-standard-4 for an older/cheaper compute-optimized tier, or
# c4a-standard-4 for ARM (Axion) if you want to match an Ampere-style box.
#
# Prereqs: gcloud CLI authenticated (`gcloud auth login`) and a project set
# (`gcloud config set project <id>`). This costs money — it is NOT free tier.
# Tear it down with the printed delete command when done.
#
set -euo pipefail

NAME="${NAME:-valkey-profiler}"
ZONE="${ZONE:-us-central1-a}"
MACHINE="${MACHINE:-c3-standard-4}"       # 4 dedicated vCPU, 16 GB, fixed clock
IMAGE_FAMILY="${IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"
IMAGE_PROJECT="${IMAGE_PROJECT:-ubuntu-os-cloud}"
DISK_GB="${DISK_GB:-30}"
SCRIPT="$(dirname "$0")/valkey-profile.sh"

[ -f "$SCRIPT" ] || { echo "valkey-profile.sh not found next to this script" >&2; exit 1; }

echo "==> Creating $MACHINE VM '$NAME' in $ZONE (dedicated vCPU)"
gcloud compute instances create "$NAME" \
  --zone="$ZONE" \
  --machine-type="$MACHINE" \
  --image-family="$IMAGE_FAMILY" \
  --image-project="$IMAGE_PROJECT" \
  --boot-disk-size="${DISK_GB}GB" \
  --boot-disk-type=pd-ssd \
  --threads-per-core=1                     # 1 thread/core => 1 vCPU == 1 physical core

echo "==> Waiting for SSH to come up"
until gcloud compute ssh "$NAME" --zone="$ZONE" --command="true" 2>/dev/null; do
  sleep 5
done

echo "==> Copying valkey-profile.sh and running setup"
gcloud compute scp "$SCRIPT" "$NAME":~/valkey-profile.sh --zone="$ZONE"
gcloud compute ssh "$NAME" --zone="$ZONE" --command="chmod +x ~/valkey-profile.sh && sudo ~/valkey-profile.sh setup"

cat <<EOF

==> Ready. SSH in and profile:
    gcloud compute ssh $NAME --zone=$ZONE

    ./valkey-profile.sh start
    ./valkey-profile.sh load 120 &
    sudo ./valkey-profile.sh flame 30
    # copy the svg back:
    gcloud compute scp $NAME:~/valkey-profiles/flame-*.svg . --zone=$ZONE

==> Tear down when done (stops billing):
    gcloud compute instances delete $NAME --zone=$ZONE --quiet
EOF
