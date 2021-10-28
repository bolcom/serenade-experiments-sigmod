//package reco.serenade.dataio;
//
//import com.google.api.core.ApiFuture;
//import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
//import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
//import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
//import com.google.cloud.bigtable.admin.v2.models.GCRules;
//import com.google.cloud.bigtable.data.v2.BigtableDataClient;
//import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
//import com.google.cloud.bigtable.data.v2.models.Row;
//import com.google.cloud.bigtable.data.v2.models.RowMutation;
//import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
//import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRuleFactory;
//import com.google.protobuf.ByteString;
//import org.springframework.util.SerializationUtils;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//
//import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
//
//public class SessionHistoryDao {
//    private static final String TABLE_ID = "evolving_sessions";
//    private static final String FAMILY_ID = "payload";
//    private static final String GCP_PROJECT_ID = "someProjectId";
//    private static final String GCP_INSTANCE_ID = "someInstanceId";
//
//    // Clients that will be connected to the emulator
//    private BigtableTableAdminClient tableAdminClient;
//    private BigtableDataClient dataClient;
//
//    public SessionHistoryDao() throws Throwable {
//        BigtableEmulatorRuleFactory ruleFactory = new BigtableEmulatorRuleFactory();
//        BigtableEmulatorRule bigtableEmulator = ruleFactory.getBigtableEmulator();
//        int port = bigtableEmulator.getPort();
//        BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilderForEmulator(port);
//        tableAdminSettings.setProjectId(GCP_PROJECT_ID);
//        tableAdminSettings.setInstanceId(GCP_INSTANCE_ID);
//        tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());
//
//        BigtableDataSettings.Builder dataSettings = BigtableDataSettings.newBuilderForEmulator(port);
//        dataSettings.setProjectId(GCP_PROJECT_ID);
//        dataSettings.setInstanceId(GCP_INSTANCE_ID);
//        dataClient = BigtableDataClient.create(dataSettings.build());
//
//        GCRules.VersionRule versionRule = GCRULES.maxVersions(1);
//        GCRules.DurationRule maxAgeRule = GCRULES.maxAge(15, TimeUnit.MINUTES);
//        // Drop cells that are either older than the xxx recent versions
//        // IntersectionRule: remove all data matching any of a set of given rules
//        // Drop cells that are older than xxx TimeUnit
//        GCRules.IntersectionRule cleanupPolicy = GCRULES.intersection().rule(maxAgeRule).rule(versionRule);
//
//        tableAdminClient.createTable(
//                CreateTableRequest.of(TABLE_ID)
//                        .addFamily(FAMILY_ID, cleanupPolicy)
//        );
//    }
//
//    public List<Long> getRowFor(String sessionId) throws IOException, ClassNotFoundException {
//        Row row = dataClient.readRow(TABLE_ID, sessionId);
//        ByteString payload = row.getCells(FAMILY_ID, ByteString.copyFromUtf8("payload")).get(0).getValue();
//        List<Long> result = (List<Long>) SessionHistoryDao.deserialize(payload.toByteArray());
//        // https://cloud.google.com/bigtable/docs/writing-data
//        return result;
//    }
//
//    public void storeIntoDb(String sessionId, List<Long> evolvingSessionItemIds, Set<Integer> candidateSessionIds) throws IOException {
////        MutablePair<List<Long>, Set<Integer>> payload = new MutablePair<>(evolvingSessionItemIds, candidateSessionIds);
//
//        byte[] asBytes= SessionHistoryDao.serialize(evolvingSessionItemIds);
//        dataClient.mutateRow(RowMutation.create(TABLE_ID, sessionId)
//                .setCell(FAMILY_ID, ByteString.copyFromUtf8("payload"), ByteString.copyFrom(asBytes))
//        );
//    }
//
//    public void writeAsync(String sessionId) throws ExecutionException, InterruptedException {
//        String rowKey = "sessionAbc1233";
//        ApiFuture<Void> mutateFuture = dataClient.mutateRowAsync(
//                RowMutation.create(TABLE_ID, sessionId)
//                        .setCell(FAMILY_ID, "e_sessions", "value")
//        );
//
//        mutateFuture.get();
//
//        ApiFuture<Row> rowFuture = dataClient.readRowAsync(TABLE_ID, sessionId);
//
//        System.out.println(rowFuture.get().getCells().get(0).getValue().toStringUtf8());
//
//    }
//
//    private static byte[] serialize(Object input) throws IOException {
//        return SerializationUtils.serialize(input);
//    }
//
//    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//        return SerializationUtils.deserialize(bytes);
//    }
//
//}
