# VMIS-Diff

This repository contains an implementation of the VMIS-kNN in Differental Dataflow, which computes the recommendations incrementally via joins and aggregations; this variant allows us to evaluate the benefits of an incremental similatiry computation for growing sessions.

### getting started
The vmis_diff requires the following arguments:
1) the location of the training data csv file
2) the location of the test data csv file
3) output file that will contain the predictions
4) output file that will contain the latencies for the predictions
5) hyperparameter k
6) hyperparameter m

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release --bin vmis_diff -- -w1 path/to/training_data_1m.txt path/to/test_data_1m.txt predictions.txt latencies.txt 100 5000
```