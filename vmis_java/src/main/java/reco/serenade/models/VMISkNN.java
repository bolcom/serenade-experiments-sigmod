package reco.serenade.models;

import reco.serenade.dataio.sessionloader.Row;
import reco.serenade.evaluation.PositionLatencyTuple;
import reco.serenade.evaluation.StopWatch;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VMISkNN {
    private final int k;
    private final int sampleSize;
    private Map<Long, Double> idf = null;
    private Map<Integer, Set<Long>> sessionToItems;  // h_sessionid with its items
    private Map<Integer, Integer> sessionMaxTime;  // h_session_id with it max timestamp
    private Map<Long, Set<Integer>> itemToSessions; // for every item in h, the h_sessionids
    private Map<Integer, List<Long>> evolvingSessions; // e_sessionid with the visited items
    private Map<Integer, Set<Integer>> relevantSessions; // e_sessionid with its precalculated matches
    private int amount = 20;  // amount of returned recommendations
    private int maxClicksPerSession = 99;
    private StopWatch stopWatch = new StopWatch();

    public VMISkNN(int k, int sampleSize) {
        this.k = k;
        this.sampleSize = sampleSize;
    }

    public void fit(List<Row> training_df) {
        System.out.println("start idf");
        idf = VMISkNN.computeItemIdfWeights(training_df);
        System.out.println("start sessionMaxTime");
        this.sessionMaxTime = training_df.stream().collect(Collectors.toMap(Row::getSessionId, Row::getTime, Math::max));

        System.out.println("start sessionToItems");
        this.sessionToItems = training_df.stream()
                .collect(Collectors.groupingBy(Row::getSessionId,
                        Collectors.mapping(Row::getItemId, Collectors.toSet())));

        System.out.println("start itemToSessions");
        this.itemToSessions = training_df.stream()
                .collect(Collectors.groupingBy(Row::getItemId,
                        Collectors.mapping(Row::getSessionId, Collectors.toSet())));

        this.evolvingSessions = new HashMap<>();
        this.relevantSessions = new HashMap<>();
    }

    public List<Recommendation> predict(Integer sessionId, Long inputItemId) {
        stopWatch.start();
        List<Long> sessionItems = this.evolvingSessions.getOrDefault(sessionId, new ArrayList<Long>());
        sessionItems.add(inputItemId);
        if (sessionItems.size() > maxClicksPerSession) {
            sessionItems = sessionItems.subList(Math.max(sessionItems.size() - maxClicksPerSession, 0), sessionItems.size());
        }
        this.evolvingSessions.put(sessionId, sessionItems);

        Set<Integer> possibleSessions = possibleNearestSessions(sessionId, inputItemId);
        PriorityQueue<SessionSimilarity> scoredSessions = calcSimilarity(sessionItems, possibleSessions);
        List<Recommendation> recommendations = scoreItems(sessionItems, scoredSessions);
        stopWatch.stop(sessionItems.size());
        return recommendations;
    }

    public List<PositionLatencyTuple> getLatencies() {
        return stopWatch.getLatencies();
    }

    private List<Recommendation> scoreItems(List<Long> sessionItems, PriorityQueue<SessionSimilarity> scoredSessions) {
        Map<Long, Double> scores = new HashMap<>();
        for (SessionSimilarity sessionSimilarity : scoredSessions){
            Set<Long> items = this.sessionToItems.get(sessionSimilarity.getSessionId());
            int step = 0;
            double decay = 0;
            for (int index = sessionItems.size(); index-- > 0; ) {
                Long sessionItem = sessionItems.get(index);
                step += 1;
                if (items.contains(sessionItem)) {
                    decay = 1 - (0.1 * step);
                    break;
                }
            }
            if (step <= 100) {
                double decayed_session_score = sessionSimilarity.getScore() * decay;
                for (Long item : items) {
                    double newScore = decayed_session_score * this.idf.get(item);
                    double sumOfScores = scores.getOrDefault(item, 0.0);
                    scores.put(item, sumOfScores + newScore);
                }
            }
        }
        Long inputItemId = sessionItems.get(sessionItems.size() - 1);
        scores.remove(inputItemId);  // remove the current_item_id as a recommendation

        PriorityQueue<Recommendation> topRecos = new PriorityQueue<>(Comparator.comparing((Recommendation r) -> r.getScore()).thenComparing(r -> r.getItemId()));
        for (Map.Entry<Long, Double> entry : scores.entrySet()) {
            topRecos.add(new Recommendation(entry.getKey(), entry.getValue()));
            if (topRecos.size() > this.amount) {
                topRecos.poll();  // remove head of the queue, containing the lowest score
            }
        }
        List<Recommendation> result = new ArrayList<>();
        while ( !topRecos.isEmpty() ) {
            result.add(topRecos.poll());
        }
        Collections.reverse(result);
        return result;
    }

    private PriorityQueue<SessionSimilarity> calcSimilarity(List<Long> sessionItems, Set<Integer> possibleSessions) {
        Map<Long, Double> posMap = new HashMap<>();
        Set<Long> items = new HashSet<>(sessionItems);
        double qty_distinct_items = items.size();
        int count = 1;
        for (Long item : sessionItems) {
            posMap.put(item, count / qty_distinct_items);
            count +=1;
        }
        PriorityQueue<SessionSimilarity> sessionSimilarities = new PriorityQueue<>(Comparator.comparing((SessionSimilarity ss) -> ss.getScore()).thenComparing(ss -> ss.getSessionId()));
        for (Integer hSessionId : possibleSessions) {
            double hScore = 0;
            Set<Long> hItems = this.sessionToItems.get(hSessionId);
            for (Long hItemId : hItems) {
                if (items.contains(hItemId)) {
                    hScore += posMap.get(hItemId);
                }
            }
            sessionSimilarities.add(new SessionSimilarity(hSessionId, hScore));
            if (sessionSimilarities.size() > this.k) {
                sessionSimilarities.poll(); // remove head of the queue, containing the lowest score
            }
        }
        return sessionSimilarities;
    }

    private Set<Integer> possibleNearestSessions(Integer sessionId, Long inputItemId) {
        Set<Integer> previousCandidateSessions = relevantSessions.getOrDefault(sessionId, new HashSet<>());
        Set<Integer> itemCandidateSessions = itemToSessions.getOrDefault(inputItemId, new HashSet<>());
        Set<Integer> candidateSessions = Stream.of(previousCandidateSessions, itemCandidateSessions).flatMap(Set::stream).collect(Collectors.toSet());

        if (candidateSessions.size() > this.sampleSize) {
            PriorityQueue<SessionSimilarity> topRecos = new PriorityQueue<>(Comparator.comparing((SessionSimilarity r) -> r.getScore()).thenComparing(r -> r.getSessionId()));
            for (Integer hSessionId : candidateSessions) {
                Integer time = this.sessionMaxTime.get(hSessionId);
                topRecos.add(new SessionSimilarity(hSessionId, time));
                if (topRecos.size() > this.sampleSize) {
                    topRecos.poll();  // remove head of the queue, containing the lowest score
                }
            }
            Set<Integer> result = new HashSet<>();
            while ( !topRecos.isEmpty() ) {
                result.add(topRecos.poll().getSessionId());
            }
            candidateSessions.retainAll(result);
        }

        this.relevantSessions.put(sessionId, candidateSessions);
        return candidateSessions;
    }

    private static Map<Long, Double> computeItemIdfWeights(List<Row> training_df) {
        double qtyDistinctSessions = training_df.stream().map(row -> row.sessionId).distinct().count();
        Map<Long, Long> idfTemp = training_df.stream()
                .collect(Collectors.groupingBy(Row::getItemId, Collectors.counting()));
        Map<Long, Double> idf = new HashMap<>();
        for (Map.Entry<Long, Long> element : idfTemp.entrySet()) {
            Double idfScore = Math.log(qtyDistinctSessions / element.getValue());
            idf.put(element.getKey(), idfScore);
        }
        return idf;
    }
}
