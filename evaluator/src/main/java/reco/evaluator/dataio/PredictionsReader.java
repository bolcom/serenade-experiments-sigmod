package reco.evaluator.dataio;

import reco.evaluator.dataio.sessionloader.Loader;
import reco.evaluator.models.Recommendation;
import com.google.cloud.Tuple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class PredictionsReader {
    private BufferedReader reader;
    public PredictionsReader(String inputFilename) throws FileNotFoundException {
        System.out.println(PredictionsReader.class.getSimpleName() + " reading: " + inputFilename);
        this.reader = Loader.createReaderFor(inputFilename);
    }

    public Tuple<List<Recommendation>, List<Long>> nextLineOrNull() throws IOException {
        String line = this.reader.readLine();
        if (line == null) {
            return null;
        }
        String[] parts = line.split(";");
        List<Recommendation> recommendations = PredictionsReader.parseRecommendations(parts[0]);

        List<Long> nextItems = PredictionsReader.parseNextItems(parts[1]);
        return Tuple.of(recommendations, nextItems);
    }

    private static List<Recommendation> parseRecommendations(String text) {
        if (text == null || text.isEmpty()) {
            System.out.println("WARN: empty list of recommendations.");
            return new ArrayList<>();
        }
        List<Long> itemIds = Arrays.stream(text.split(",")).map(t -> Long.valueOf(t))
                .collect(Collectors.toList());
        double score = 1.0;
        List<Recommendation> result = new ArrayList<>();
        for (long itemId : itemIds) {
            result.add(new Recommendation(itemId, score));
            score -= 0.001;
        }
        return result;
    }

    private static List<Long> parseNextItems(String text) {
        return Arrays.stream(text.split(",")).map(t -> Long.valueOf(t))
                .collect(Collectors.toList());
    }
}
