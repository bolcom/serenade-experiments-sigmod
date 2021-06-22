package reco.evaluator.evaluation.metrics;

import reco.evaluator.dataio.sessionloader.Row;
import reco.evaluator.models.Recommendation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Coverage implements SessionMetric {
    private final long numItems;
    private Set<Long> coverageSet = new HashSet<>();

    private final int length;

    public Coverage(List<Row> training_df) {
        this(20, training_df);
    }

    public Coverage(int length, List<Row> training_df) {
        this.length = length;
        long unique_item_ids = training_df.stream().map(row -> row.getItemId()).distinct().count();
        this.numItems = unique_item_ids;
    }

    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        Set<Long> reco = recommendations.subList(0, Math.min(this.length, recommendations.size())).stream().map(r -> r.getItemId()).collect(Collectors.toSet());
        coverageSet.addAll(reco);
    }

    @Override
    public double result() {
        return coverageSet.size() / (double) numItems;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName() + "@" + this.length;
    }
}
