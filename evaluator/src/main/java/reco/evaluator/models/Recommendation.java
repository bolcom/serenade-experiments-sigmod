package reco.evaluator.models;

public class Recommendation {
    private long itemId;
    private double score;
    public Recommendation(long itemId, double score) {
        this.itemId = itemId;
        this.score = score;
    }

    public long getItemId() {
        return itemId;
    }

    public double getScore() {
        return score;
    }
}
