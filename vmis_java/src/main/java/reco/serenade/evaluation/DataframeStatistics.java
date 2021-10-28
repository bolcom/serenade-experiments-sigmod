package reco.serenade.evaluation;

import reco.serenade.dataio.sessionloader.Row;

import java.time.Instant;
import java.util.List;

public class DataframeStatistics {

    public static void printDataframeStatistics(String descriptiveName, List<Row> input_df) {
        long unique_session_ids = input_df.stream().map(row -> row.getSessionId()).distinct().count();
        long unique_item_ids = input_df.stream().map(row -> row.getItemId()).distinct().count();
        int minTime = input_df.stream().map(row -> row.getTime()).reduce(Integer.MAX_VALUE, Integer::min);
        int maxTime = input_df.stream().map(row -> row.getTime()).reduce(Integer.MIN_VALUE, Integer::max);
        System.out.println("Loaded " + descriptiveName);
        System.out.println("\tEvents: " + input_df.size());
        System.out.println("\tSessions: " + unique_session_ids);
        System.out.println("\tItems: " + unique_item_ids);
        System.out.println("\tSpan: " + Instant.ofEpochSecond(minTime).toString() + " / " +
                Instant.ofEpochSecond(maxTime).toString());
    }
}
