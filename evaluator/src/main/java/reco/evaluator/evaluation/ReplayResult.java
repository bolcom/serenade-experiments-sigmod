package reco.evaluator.evaluation;

import java.util.ArrayList;
import java.util.List;

public class ReplayResult {
    private int sessionId;
    private long currentItemId;
    private int time;
    private List<Long> nextItems;

    public ReplayResult(int sessionId, long currentItemId, int time, List<Long> nextItems) {
        this.sessionId = sessionId;
        this.currentItemId = currentItemId;
        this.time = time;
        this.nextItems = new ArrayList<>(nextItems);
    }

    public int getSessionId() {
        return sessionId;
    }

    public long getCurrentItemId() {
        return currentItemId;
    }

    public int getTime() {
        return time;
    }

    public List<Long> getNextItems() {
        return nextItems;
    }

    @Override
    public String toString() {
        return "ReplayResult{" +
                "sessionId=" + sessionId +
                ", currentItemId=" + currentItemId +
                ", time=" + time +
                ", nextItems=" + nextItems +
                '}';
    }

}
