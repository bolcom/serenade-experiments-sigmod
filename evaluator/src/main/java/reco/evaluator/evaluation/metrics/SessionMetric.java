package reco.evaluator.evaluation.metrics;

import reco.evaluator.models.Recommendation;

import java.util.List;

public interface SessionMetric {
    void add(List<Recommendation> recommendations, List<Long> nextItems);
    double result();
    String getName();
}
