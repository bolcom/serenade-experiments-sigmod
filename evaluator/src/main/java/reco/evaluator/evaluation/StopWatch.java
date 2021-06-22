package reco.evaluator.evaluation;

import com.tdunning.math.stats.TDigest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StopWatch {
    private TDigest tdigest = TDigest.createDigest(100);
    private List<PositionLatencyTuple> latencies = new ArrayList<PositionLatencyTuple>();
    private long startNanos;

    public StopWatch() {
        System.out.println(StopWatch.class.getSimpleName() + " ##############################################################################");
        System.out.println(StopWatch.class.getSimpleName() + " # Capturing metadata for all predictions.                                    #");
        System.out.println(StopWatch.class.getSimpleName() + " # This consumes memory and must not be used in production                    #");
        System.out.println(StopWatch.class.getSimpleName() + " ##############################################################################");
    }

    public void start() {
        startNanos = System.nanoTime();
    }

    public void stop(int position) {
        long stopNanos = System.nanoTime();
        long durationMicros = Math.round((stopNanos - startNanos) / 1000.0);
        tdigest.add(durationMicros);
        PositionLatencyTuple t = new PositionLatencyTuple(position, durationMicros);
        latencies.add(t);
    }

    /**
     * @param p Percentile to compute. Must be between 0 and 100 inclusive.
     * @return the p-th percentile(s) of the measured data in milliseconds.
     */
    public double getPercentileInMSec(double p) {
        return tdigest.quantile(p / 100.0);
    }

    public long getN() {
        return tdigest.size();
    }

    public List<PositionLatencyTuple> getLatencies() {
        return latencies.stream().collect(Collectors.toList());
    }
}
