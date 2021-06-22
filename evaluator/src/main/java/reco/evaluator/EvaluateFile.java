package reco.evaluator;

import reco.evaluator.dataio.PredictionsReader;
import reco.evaluator.dataio.sessionloader.Loader;
import reco.evaluator.dataio.sessionloader.Row;
import reco.evaluator.evaluation.DataframeStatistics;
import reco.evaluator.models.Recommendation;
import com.google.cloud.Tuple;
import reco.evaluator.evaluation.metrics.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class EvaluateFile {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Invalid arguments. Usage: " + EvaluateFile.class.getSimpleName() + " ../datasets/retailrocket9_train.txt java_vsknn_predictions.txt");
            System.exit(-1);
        }
        String training_csv_path = args[0];  //  "../datasets/retailrocket9_train.txt";
        String predictionsFile = args[1]; // "java_vsknn_predictions.txt";

        List<Row> training_df = Loader.readCsv(training_csv_path);
        DataframeStatistics.printDataframeStatistics("training data", training_df);

        List<SessionMetric> metrics = Arrays.asList(new NDCG(), new MAP(), new Precision(), new Recall(), new HitRate(),
                new MRR(), new Coverage(training_df), new Popularity(training_df));

        PredictionsReader reader = new PredictionsReader(predictionsFile);
        Tuple<List<Recommendation>, List<Long>> replayResult = null;
        while ((replayResult = reader.nextLineOrNull()) != null) {
            for (SessionMetric metric : metrics) {
                metric.add(replayResult.x(), replayResult.y());
            }
        }
        for (SessionMetric metric : metrics) {
            System.out.println(metric.getName() + String.format(": %.4f", metric.result()));
        }
    }
}
