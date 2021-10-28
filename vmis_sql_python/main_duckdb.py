import argparse

from dataio.predictions import PredictionsWriter
from dataio.sessionloader.latency_writter import LatencyWriter
from dataio.sessionloader.loader import Loader
from evaluation.replayer import Replayer
from evaluation.utils import DataframeStatistics
from models.sql.vmisknnduck import VMISSQL


def run(training_csv_path, test_csv_path, outputfilename, latency_outputfilename):
    print('reading training data')
    training_df = Loader.read_csv(training_csv_path)
    DataframeStatistics.print_df_statistics('training', training_df)

    print('reading test data')
    test_df = Loader.read_csv(test_csv_path)
    DataframeStatistics.print_df_statistics('test', test_df)

    test_limit = 1000000
    if len(test_df) > test_limit:
        # python is slow at scale thus we test on a small subset of testdata
        print('reducing testdata to ' + str(test_limit))
        test_df = test_df.head(test_limit)
        DataframeStatistics.print_df_statistics('reduced test data', test_df)

    print('model.fit(training_df) start')
    model = VMISSQL()
    model.fit(training_df)
    print('model.fit(training_df) done')
    replayer = Replayer(test_df)

    predictions_writer = PredictionsWriter(outputfilename=outputfilename, evaluation_n=20)
    latency_writer = LatencyWriter(latency_outputfilename)
    for (current_session_id, current_item_id, ts, rest) in replayer.next_sequence():

        recommendations = model.predict_next(session_id=current_session_id, input_item_id=current_item_id,
                                                timestamp=ts)

        predictions_writer.appendline(recommendations, rest)

        if len(model.get_latencies()) < 10000:
            if len(model.get_latencies()) % 1000 == 0:
                print('qty predictions:' + str(len(model.get_latencies())))
        else:
            if len(model.get_latencies()) % 10000 == 0:
                print('qty predictions:' + str(len(model.get_latencies())))

    import numpy as np
    print(np.percentile(model.get_latencies(), 90))
    predictions_writer.close()
    for (position, latency) in model.get_latencies():
        latency_writer.append_line(position, latency)
    latency_writer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('training_csv_path', help='Path to training data. Like ../datasets/bolcom-clicks-1m_train.txt')
    parser.add_argument('test_csv_path', help='Path to test data. Like ../datasets/bolcom-clicks-1m_test.txt')
    parser.add_argument('outputfilename', help='Output filename where the predictions will be writen to. Like vsknn_sql_predictions.txt')
    parser.add_argument('latency_outputfilename', help='Output latency_outputfilename where the predictions will be writen to. Like vsknn_sql_latencies.txt')
    args = parser.parse_args()
    print(args)
    run(**vars(args))