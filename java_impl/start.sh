#!/bin/bash -x
if [[ $# -ne 4 ]]; then
    echo "Illegal number of parameters"
    echo "Usage: " $0 ../datasets/retailrocket9_train.txt ../datasets/retailrocket9_test.txt java_vsknn_predictions.txt java_position_latency.csv
    exit 2
fi

training=$1
test=$2
output=$3
position_latency=$4


if [[ $training == "gs://"* ]] ;
then
    gsutil cp $training tmp_training.csv
    training=tmp_training.csv
fi
if [[ $test == "gs://"* ]] ;
then
    gsutil cp $test tmp_test.csv
    test=tmp_test.csv
fi


function do_run() {
  if [[ $# -ne 4 ]]; then
    echo "Illegal number of parameters"
    echo "Usage: " $0 ../datasets/retailrocket9_train.txt ../datasets/retailrocket9_test.txt java_vsknn_predictions.txt java_position_latency.csv
    exit 2
  fi
  train_in=$1
  test_in=$2
  pred_out=$3
  latency_out=$4

  JAVA_OPTS="-XX:InitialRAMPercentage=70.0 \
        -XX:MaxRAMPercentage=85.0 \
        -XX:+UnlockExperimentalVMOptions \
        -XX:MaxGCPauseMillis=100 \
        -XshowSettings:vm"
  java $JAVA_OPTS -jar target/serenade-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${train_in} ${test_in} ${pred_out} ${latency_out}

}

mvn clean package
do_run $training $test tmp_output.csv tmp_position_latency.csv

if [[ $output == "gs://"* ]] ;
then
  gsutil cp tmp_output.csv $output
  gsutil cp tmp_position_latency.csv $position_latency
else
  cp tmp_output.csv $output
  cp tmp_position_latency.csv $position_latency
fi




