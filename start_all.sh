#!/bin/bash

TRAINING_CSV_PATH=../datasets/retailrocket9_train.txt
TEST_CSV_PATH=../datasets/retailrocket9_test.txt
OUTPUT_FILE=vsknn_predictions.txt
LATENCY_FILE=vsknn_latencies.csv

BASEDIR=$(pwd)

cd java_impl
source start.sh ${TRAINING_CSV_PATH} ${TEST_CSV_PATH} java_${OUTPUT_FILE}
cd ${BASEDIR}

cd python_impl
source start_python.sh ${TRAINING_CSV_PATH} ${TEST_CSV_PATH} python_${OUTPUT_FILE}
cd ${BASEDIR}

cd rust_optimized_impl
source start_rust.sh ${TRAINING_CSV_PATH} ${TEST_CSV_PATH} rust_${OUTPUT_FILE}
cd ${BASEDIR}

cd dd_impl
source start_dd.sh ${TRAINING_CSV_PATH} ${TEST_CSV_PATH} dd_${OUTPUT_FILE}
cd ${BASEDIR}


