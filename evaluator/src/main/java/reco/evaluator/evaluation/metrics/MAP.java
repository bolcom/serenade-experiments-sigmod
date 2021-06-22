package reco.evaluator.evaluation.metrics;

import reco.evaluator.models.Recommendation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MAP implements SessionMetric {
    private double sumOfScores = 0;
    private double qty = 0;

    private final int length;

    public MAP() {
        this(20);
    }

    public MAP(int length) {
        this.length = length;
    }


    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        double last_recall = 0;
        double res = 0;
        List<Long> recommendationsAsList = recommendations.stream().map(r -> r.getItemId()).collect(Collectors.toList());
        for (int i = 0; i < Math.min(this.length, recommendationsAsList.size() + 1) ; i++) {
            double recall = recall(recommendationsAsList.subList(0, Math.min(i, recommendationsAsList.size())), nextItems);
            double precision = precision(recommendationsAsList.subList(0, Math.min(i, recommendationsAsList.size())), nextItems);
            res += precision * (recall - last_recall);
            last_recall = recall;
        }
        this.sumOfScores += res;
        this.qty += 1;
    }

    private double recall(List<Long> recommendations, List<Long> nextItems) {
        Set<Long> intersection = new HashSet<>(nextItems);
        Set<Long> distinctRecommendations = new HashSet<>(recommendations);
        intersection.retainAll(distinctRecommendations);
        return intersection.size() / (double) nextItems.size();
    }

    private double precision(List<Long> recommendations, List<Long> nextItems) {
        Set<Long> intersection = new HashSet<>(nextItems);
        Set<Long> distinctRecommendations = new HashSet<>(recommendations);
        intersection.retainAll(distinctRecommendations);
        return intersection.size() / (double) this.length;
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
