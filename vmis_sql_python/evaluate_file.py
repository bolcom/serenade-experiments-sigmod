from dataio.predictions import PredictionsReader
from dataio.sessionloader.loader import Loader
from evaluation.metrics.accuracy_multiple import *
from evaluation.metrics.accuracy import *
from evaluation.metrics.coverage import Coverage
from evaluation.metrics.popularity import Popularity

if __name__ == '__main__':
    # the location of the exact training data that was used to train the model.
    training_csv_path = '../datasets/bolcom-clicks-100k_train.txt'

    training_df = Loader.read_csv(training_csv_path)

    reader = PredictionsReader(inputfilename='vsknn_rust_predictions.txt',
                               training_df=training_df)

    metrics = [NDCG(), MAP(), Precision(), Recall(), HitRate(), MRR(), Coverage(training_df=training_df),
               Popularity(training_df=training_df)]

    for (recommendations, next_items) in reader.get_next_line():
        for metric in metrics:
            metric.add(recommendations, next_items)

    print(training_csv_path)
    for metric in metrics:
        metric_name, score = metric.result()
        print(metric_name, "%.4f" % score)


