#!/bin/bash -x

export PROJECT="my-google-project-name"
export REGION="my-google-region"
export USER=${USER:-SA}

function submit_train_eval_job() {
    echo start submit_train_eval_job
    if [[ $# -ne 7 ]]; then
        echo "Illegal number of function parameters"
        echo "Usage: submit " serenade_java_202010023 eu.gcr.io/${PROJECT}/serenade-java:master n1-highmem-4 gs://somepath/retailrocket9_train.txt gs://somepath/retailrocket9_test.txt gs://somepath/java_vsknn_predictions.txt gs://somepath/java_position_latency.csv
        exit 2
    fi
    jobname=$1
    jobname=${jobname//-/_}
    dockerimageuri=$2
    machinetype=$3
    training=$4
    test=$5
    output=$6
    latency=$7

    gcloud beta ai-platform jobs submit training ${jobname} \
      --region $REGION \
      --project $PROJECT \
      --master-image-uri ${dockerimageuri} \
      --scale-tier CUSTOM \
      --master-machine-type ${machinetype} \
      -- \
      ${training} ${test} ${output} ${latency}
    echo end submit_train_eval_job
}

for comp_model in python sql java dd rust-optimized; do
  for size in 100k 1m; do
    machinetype="n1-highmem-8"
    submit_train_eval_job \
      ${USER}_serenade_${size}_${comp_model}_$(date +%Y%m%d_%H%M%S) \
      eu.gcr.io/${PROJECT}/serenade-${comp_model}:master \
      ${machinetype} \
      gs://${PROJECT}-shared/serenade/datasets/private-clicks-${size}_train.txt \
      gs://${PROJECT}-shared/serenade/datasets/private-clicks-${size}_test.txt \
      gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-private-${size}_predictions.txt \
      gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-private-${size}_latencies.txt
  done
  for size in 50m 100m 200m; do
    machinetype="n1-highmem-8"
    submit_train_eval_job \
      ${USER}_serenade_${size}_${comp_model}_$(date +%Y%m%d_%H%M%S) \
      eu.gcr.io/${PROJECT}/serenade-${comp_model}:master \
      ${machinetype} \
      gs://${PROJECT}-shared/serenade/datasets/private-clicks-${size}_train.txt \
      gs://${PROJECT}-shared/serenade/datasets/private-clicks-${size}_test.txt \
      gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-private-${size}_predictions.txt \
      gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-private-${size}_latencies.txt
  done
  machinetype="n1-highmem-8"
  submit_train_eval_job \
    ${USER}_serenade_rsc15_${comp_model}_$(date +%Y%m%d_%H%M%S) \
    eu.gcr.io/${PROJECT}/serenade-${comp_model}:master \
    ${machinetype} \
    gs://${PROJECT}-shared/serenade/datasets/rsc15-clicks_train_full.txt \
    gs://${PROJECT}-shared/serenade/datasets/rsc15-clicks_test.txt \
    gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-rsc15_predictions.txt \
    gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-rsc15_latencies.txt
  machinetype="n1-highmem-8"
  submit_train_eval_job \
    ${USER}_serenade_retailrocket_${comp_model}_$(date +%Y%m%d_%H%M%S) \
    eu.gcr.io/${PROJECT}/serenade-${comp_model}:master \
    ${machinetype} \
    gs://${PROJECT}-shared/serenade/datasets/retailrocket9_train.txt \
    gs://${PROJECT}-shared/serenade/datasets/retailrocket9_test.txt \
    gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-retailrocket_predictions.txt \
    gs://${PROJECT}-shared/serenade/results/${comp_model}-vsknn-retailrocket_latencies.txt

done
