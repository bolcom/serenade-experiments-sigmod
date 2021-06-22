package reco.evaluator.evaluation.metrics;

import reco.evaluator.models.Recommendation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NDCG implements SessionMetric {

    private double sumOfScores = 0;
    private double qty = 0;

    private final int length;

    private final static double LOG2 = Math.log(2);

    public NDCG() {
        this(20);
    }

    public NDCG(int length) {
        this.length = length;
    }

    @Override
    public void add(List<Recommendation> recommendations, List<Long> nextItems) {
        List<Long> topRecos = new ArrayList<>();
        for (int i = 0; i < Math.min(recommendations.size(), this.length); i++) {
            topRecos.add(recommendations.get(i).getItemId());
        }
        List<Long> topNextItems = nextItems.subList(0, Math.min(nextItems.size(), this.length));
        double dcg = dcg(topRecos, nextItems);
        double dcgMax = dcg(topNextItems, nextItems);
        this.sumOfScores += dcg / dcgMax;
        this.qty += 1;
    }

    private double dcg(List<Long> topRecos, List<Long> nextItems) {
        double result = 0;
        Set<Long> nextItemsSet = new HashSet<>(nextItems);
        for (int i = 0; i < Math.min(topRecos.size(), this.length); i++) {
            if (nextItemsSet.contains(topRecos.get(i))) {
                if (i == 0) {
                    result += 1;
                } else {
                    result += 1 / log2(i + 1);
                }
            }
        }
        return result;
    }

    @Override
    public double result() {
        return this.sumOfScores / this.qty;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName() + "@" + this.length;
    }

    private static double log2(double n) {
        return Math.log(n) / LOG2;
    }
}
