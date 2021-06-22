package reco.evaluator.evaluation.metrics;

import reco.evaluator.models.Recommendation;

import java.util.List;
import java.util.stream.Collectors;

public class HitRate implements SessionMetric {

    private double sumOfScores = 0;
    private double qty = 0;

    private final int length;

    public HitRate() {
        this(20);
    }

    public HitRate(int length) {
        this.length = length;
    }

    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        this.qty += 1;

        List<Long> recommendationsAsList = recommendations.subList(0, Math.min(this.length, recommendations.size())).
                stream().map(r -> r.getItemId()).collect(Collectors.toList());

        if (recommendationsAsList.contains(nextItems.get(0))) {
            this.sumOfScores += 1;
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
