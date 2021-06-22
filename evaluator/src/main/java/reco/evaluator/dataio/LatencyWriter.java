package reco.evaluator.dataio;

import reco.evaluator.evaluation.PositionLatencyTuple;
import com.google.cloud.storage.*;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LatencyWriter {
    private PrintWriter outputPrintWriter;
    private String outputFilename;
    private String tmpFilename = null;

    public LatencyWriter(String outputFilename) throws FileNotFoundException {
        this.outputFilename = outputFilename;
        if (outputFilename.startsWith("gs://")) {
            // The GFS API does not support appending data to a file.
            // our output can be large.
            // We first write the content to a local temp file and copy output to GFS when done.
            this.tmpFilename = System.getProperty("java.io.tmpdir") + RandomStringUtils.random(12, true, true);
            this.outputPrintWriter = new PrintWriter(tmpFilename);
        } else {
            this.outputPrintWriter = new PrintWriter(outputFilename);
        }
        outputPrintWriter.write("position,latency_in_micros\n");
    }

    public void appendLine(PositionLatencyTuple tuple) {
        outputPrintWriter.write(tuple.position + "," + tuple.latencyInMicros + "\n");
    }

    /**
     * Closes the stream and releases any system resources associated with it.
     * If the outputFilename is on google storage bucket then the data will now be copied to it.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        outputPrintWriter.close();
        if (tmpFilename != null) {
            // copy tmpFilename to outputFilename
            URI outputURI = URI.create(this.outputFilename);
            Storage storageClient = StorageOptions.getDefaultInstance().getService();
            Bucket bucket = storageClient.get(outputURI.getHost());
            String path = outputURI.getPath();
            while (path.startsWith("/")) {
                // URI Path starts with a '/'. The Google Storage API expects paths NOT to start with a '/'.
                path = path.substring(1);
            }
            BlobId blobId = BlobId.of(bucket.getName(), path);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            storageClient.create(blobInfo, Files.readAllBytes(Paths.get(tmpFilename)));
        }
    }
}
