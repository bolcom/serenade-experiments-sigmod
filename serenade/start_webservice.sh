#!/bin/bash -x

echo Determine qty CPU workers
if [[ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
  # This machine is running in kubernetes. We determine the qty workers by the configured CPU quota.
  cfs_quota_us=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
  if [[ $cfs_quota_us > 0 ]]; then
    cfs_period_us=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
    qty_workers=$((cfs_quota_us / cfs_period_us))
  fi
fi
qty_workers="${qty_workers:-$(nproc)}"
echo qty_workers ${qty_workers}

NUM_WORKERS=${qty_workers} ./serving  # Optional: config.toml
