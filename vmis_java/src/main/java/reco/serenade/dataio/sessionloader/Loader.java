package reco.serenade.dataio.sessionloader;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class Loader {

    public static List<Row> readCsv(String fullPathToCsvFile) throws IOException {
        System.out.println(Loader.class.getSimpleName() + " reading: " + fullPathToCsvFile);
        URI fileLocation = URI.create(fullPathToCsvFile);

        BufferedReader br;
        if ("gs".equals(fileLocation.getScheme())){
            br = createGoogleFilesystemReader(fileLocation);
        } else {
            br = createLocalFileReader(fileLocation);
        }
        return Loader.readerToRows(br);
    }

    private static List<Row> readerToRows(BufferedReader br) throws IOException {
        br.readLine(); // ignore header line
        List<Row> result = br.lines().map(line -> {
            String[] parts = line.split("\t");
            int sessionId = Integer.valueOf(parts[0]);
            long itemId = Long.valueOf(parts[1]);
            int time = (int)Math.round(Double.valueOf(parts[2]));
            return new Row(sessionId, itemId, time);
        }).collect(Collectors.toList());
        return result;
    }

    private static BufferedReader createGoogleFilesystemReader(URI fileLocation) throws FileNotFoundException {
        Storage storageClient = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storageClient.get(fileLocation.getHost());

        String path = fileLocation.getPath();
        while (path.startsWith("/")) {
            // URI Path starts with a '/'. The Google Storage API expects paths NOT to start with a '/'.
            path = path.substring(1);
        }
        Blob blob = bucket.get(path);
        Reader reader = new StringReader(new String(blob.getContent()));
        return new BufferedReader(reader);
    }

    private static BufferedReader createLocalFileReader(URI fileLocation) throws FileNotFoundException {
        String path = fileLocation.getPath();
        return new BufferedReader(new FileReader(path));
    }
}
