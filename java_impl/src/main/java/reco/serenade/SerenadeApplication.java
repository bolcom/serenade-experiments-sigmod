package reco.serenade;

import java.io.IOException;
import java.util.*;

import reco.serenade.dataio.LatencyWriter;
import reco.serenade.dataio.PredictionsWriter;
import reco.serenade.dataio.sessionloader.Loader;
import reco.serenade.dataio.sessionloader.Row;
import reco.serenade.models.Recommendation;
import reco.serenade.models.VSKNN;
import reco.serenade.evaluation.DataframeStatistics;
import reco.serenade.evaluation.PositionLatencyTuple;
import reco.serenade.evaluation.ReplayResult;
import reco.serenade.evaluation.Replayer;

public class SerenadeApplication {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Invalid arguments. Usage: " + SerenadeApplication.class.getSimpleName() + " ../datasets/retailrocket9_train.txt ../datasets/retailrocket9_test.txt java_vsknn_predictions.txt java_position_latency.csv");
            System.exit(-1);
        }

        String training_csv_path = args[0];  //
        String test_csv_path = args[1];  //
        String predictionOutputfilename = args[2]; // "../python_impl/java_vsknn_predictions.txt";
        String latencyOutputfilename = args[3]; // "../python_impl/java_position_latency.csv";

        List<Row> training_df = Loader.readCsv(training_csv_path);
        DataframeStatistics.printDataframeStatistics("training data", training_df);

        List<Row> test_df = Loader.readCsv(test_csv_path);
        DataframeStatistics.printDataframeStatistics("test data", test_df);

        int testLimit = 1000000;
        if (test_df.size() > testLimit) {
            test_df = test_df.subList(0, testLimit);
            DataframeStatistics.printDataframeStatistics("reduced test data", test_df);
        }

        VSKNN model = new VSKNN(100, 5000);
        System.out.println("training model: start");
        model.fit(training_df);
        System.out.println("training model: finished");

        Replayer replayer = new Replayer(test_df);
        PredictionsWriter predictionsWriter = new PredictionsWriter(predictionOutputfilename);

        ReplayResult replayResult = null;
        System.out.println("predicting");
        while ((replayResult = replayer.getReplayResultOrNull()) != null) {
            List<Recommendation> predictions = model.predict(replayResult.getSessionId(), replayResult.getCurrentItemId());
            predictionsWriter.appendLine(predictions, replayResult.getNextItems());

            if (model.getLatencies().size() < 10000) {
                if (model.getLatencies().size() % 1000 == 0) {
                    System.out.println("qty predictions:" + model.getLatencies().size());
                }
            } else {
                if (model.getLatencies().size() % 10000 == 0) {
                    System.out.println("qty predictions:" + model.getLatencies().size());
                }
            }
        }

        LatencyWriter latencyWriter = new LatencyWriter(latencyOutputfilename);
        for (PositionLatencyTuple positionLatencyTuple : model.getLatencies()) {
            latencyWriter.appendLine(positionLatencyTuple);
        }
        latencyWriter.close();
        predictionsWriter.close();
    }
}
