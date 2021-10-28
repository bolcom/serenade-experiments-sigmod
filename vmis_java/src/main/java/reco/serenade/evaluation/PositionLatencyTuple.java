package reco.serenade.evaluation;

public class PositionLatencyTuple {
    public int position;
    public long latencyInMicros;
    public PositionLatencyTuple(int position, long latencyInMicros) {
        this.position = position;
        this.latencyInMicros = latencyInMicros;
    }
}
