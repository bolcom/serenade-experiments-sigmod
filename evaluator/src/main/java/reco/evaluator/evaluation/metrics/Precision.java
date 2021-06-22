package reco.evaluator.evaluation.metrics;

import reco.evaluator.models.Recommendation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Precision implements SessionMetric {

    private double sumOfScores = 0;
    private double qty = 0;

    private final int length;

    public Precision() {
        this(20);
    }

    public Precision(int length) {
        this.length = length;
    }

    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        Set<Long> intersection = new HashSet<>(nextItems);
        Set<Long> reco = recommendations.subList(0, Math.min(this.length, recommendations.size())).stream().map(r -> r.getItemId()).collect(Collectors.toSet());
        intersection.retainAll(reco);

        this.sumOfScores += intersection.size() / (double) this.length;
        this.qty += 1;
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
