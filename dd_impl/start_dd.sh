#!/bin/bash
if [[ $# -ne 4 ]]; then
    echo "Illegal number of parameters. Received $# we need 4"
    echo "Usage: " $0 ../datasets/private-clicks-1m_train.txt ../datasets/private-clicks-1m_test.txt dd_predictions.txt dd_latencies.csv
    exit 2
fi
training=$1
test=$2
output=$3
position_latency=$4

if [[ $training == "gs://"* ]] ;
then
    f=$(basename -- $training)
    gsutil cp $training ${f}
    training=${f}
fi
if [[ $test == "gs://"* ]] ;
then
    f=$(basename -- $test)
    gsutil cp $test ${f}
    test=${f}
fi

function do_run() {
  if [[ $# -ne 4 ]]; then
    echo "Illegal number of function parameters"
    echo "Usage: " $0 ../datasets/private-clicks-1m_train.txt ../datasets/private-clicks-1m_test.txt dd_predictions.txt dd_latencies.csv
    exit 2
  fi
  train_in=$1
  test_in=$2
  pred_out=$3
  latency_out=$4
  RUSTFLAGS="-C target-cpu=native"

  cargo run --release --bin dd_replay_from_file -- -w1 ${train_in} ${test_in} ${pred_out} ${latency_out} 100 5000
}

do_run $training $test tmp_output.csv tmp_position_latency.csv

if [[ $output == "gs://"* ]] ;
then
  gsutil cp tmp_output.csv $output
  gsutil cp tmp_position_latency.csv $position_latency
else
  cp tmp_output.csv $output
  cp tmp_position_latency.csv $position_latency
fi

