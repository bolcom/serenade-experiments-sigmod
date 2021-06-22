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

gs_trainingindex_basedir="gs://my-google-project-data/serenade_indexes"
echo using gs_trainingindex_basedir: ${gs_trainingindex_basedir}

if [[ ${gs_trainingindex_basedir} == "gs://"* ]] ;
then
  # the argument is a google storage dir. Thus it will contain multiple files that need to be concatenated
  most_recent_successfile=$(gsutil ls -b ${gs_trainingindex_basedir}/*/avro/sessionindex/_SUCCESS | sort | tail -n 1)
  # determine the base directory that contains the two directories with the index files
  most_recent_dir=$(dirname $(dirname ${most_recent_successfile}))
  echo Most recent match: ${most_recent_dir}
  echo Downloading ...
  training_filelocation=indices
  mkdir ${training_filelocation}
  gsutil -m cp -R ${most_recent_dir}/* ${training_filelocation}
else
  training_filelocation=$1
fi

./serving ${training_filelocation} ${qty_workers}

