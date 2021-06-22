package reco.evaluator.models;

import java.util.Objects;

public class SessionSimilarity {
    private int sessionId;
    private double score;

    public SessionSimilarity(int sessionId, double score) {
        this.sessionId = sessionId;
        this.score = score;
    }

    public int getSessionId() {
        return sessionId;
    }

    public double getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionSimilarity that = (SessionSimilarity) o;
        return sessionId == that.sessionId &&
                Double.compare(that.score, score) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, score);
    }
}
