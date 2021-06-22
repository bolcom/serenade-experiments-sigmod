package reco.evaluator.evaluation;

import reco.evaluator.dataio.sessionloader.Row;

import java.util.*;
import java.util.stream.Collectors;

public class Replayer {
    private Map<Integer, List<Long>> sessionIdToItems;

    private Integer currentSessionId = null;
    private Random random = new Random();

    public Replayer(List<Row> testDf) {
        sessionIdToItems = testDf.stream().collect(Collectors.groupingBy(Row::getSessionId,
                Collectors.mapping(Row::getItemId, Collectors.toList())
        ));
        currentSessionId = new ArrayList<>(sessionIdToItems.keySet()).get(random.nextInt(sessionIdToItems.size())); // random sessionId
    }

    public ReplayResult getReplayResultOrNull() {
        List<Long> nextItems = sessionIdToItems.get(currentSessionId);
        if (nextItems.size() == 1) {
            // if there is only one remaining click in the session there is nothing to evaluate and thus we remove it.
            sessionIdToItems.remove(currentSessionId);
            if (sessionIdToItems.size() == 0) {
                // if there are no more sessions we abort
                return null;
            }
            // pick new random session_id without replacement
            currentSessionId = new ArrayList<>(sessionIdToItems.keySet()).get(random.nextInt(sessionIdToItems.size())); // random sessionId
            nextItems = sessionIdToItems.get(currentSessionId);
        }
        long input_item_id = nextItems.remove(0);
        return new ReplayResult(currentSessionId, input_item_id, 0, nextItems);
    }
}

