package reco.evaluator.evaluation.metrics;

import reco.evaluator.dataio.sessionloader.Row;
import reco.evaluator.models.Recommendation;

import java.util.*;
import java.util.stream.Collectors;

public class Popularity implements SessionMetric {

    private double sumOfScores = 0;
    private double qty = 0;

    private Map<Long, Double> popScores;
    private final int length;

    public Popularity(List<Row> training_df) {
        this(20, training_df);
    }

    public Popularity(int length, List<Row> training_df) {
        this.length = length;
        Map<Long, Long> popScores = training_df.stream()
                .collect(Collectors.groupingBy(Row::getItemId, Collectors.counting()));
        double maxFrequency = popScores.entrySet().stream().map(kv -> kv.getValue().longValue()).reduce(Long::max).get();
        this.popScores = popScores.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() / maxFrequency));
    }


    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        Set<Long> items = recommendations.subList(0, Math.min(this.length, recommendations.size())).stream().map(r -> r.getItemId()).collect(Collectors.toSet());
        this.qty += 1;
        if (items.size() > 0) {
            double sum = 0;
            for (long itemId : items) {
                sum += this.popScores.get(itemId);
            }
            sumOfScores += sum / (double) items.size();
        }
    }

    @Override
    public double result() {
        return this.sumOfScores / this.qty;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName() + "@" + this.length;
    }
}
