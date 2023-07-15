import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static final int CHUNK_SIZE = 500000000;
    private static final String URI_STRING = "https://geo.mirror.pkgbuild.com/iso/2023.07.01/archlinux-2023.07.01-x86_64.iso";
    private static final String FILE_NAME = "C:\\Users\\andre\\Desktop\\Archlinux.2023.iso";
    private static final String TMP_FILE_NAME_PREFIX = FILE_NAME;
    private static final int MAX_RETRY = 3;

    public static void main(String[] args) {
        try {
            URL url = retrieveURL();

            HttpURLConnection urlConnection = openConnection(url);

            boolean support = checkIfAcceptsRangeHeader(urlConnection);

            if (!support) {
                System.out.println("The server does not support RANGE header. Terminating...");
                return;
            }

            long contentLength = getContentLength(urlConnection);

            urlConnection.disconnect();

            List<String> tmpFiles = downloadChunksAndRetrieveFileNames(url, contentLength);

            writeChunksOnFile(tmpFiles);

        } catch (IOException | URISyntaxException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void writeChunksOnFile(List<String> tmpFiles) throws IOException {
        System.out.println("\n\nWriting chunks on file....");
        OutputStream outStream = createOutputStream();
        tmpFiles.forEach(tmpFile -> {
            try {
                outStream.write(new FileInputStream(tmpFile).readAllBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        outStream.close();
        System.out.println("DONE!");
    }

    private static List<String> downloadChunksAndRetrieveFileNames(URL url, long contentLength) {
        int retry = 0;
        List<String> tmpFiles = new ArrayList<>();
        for (long i = 0; i < contentLength; ) {
            try {
                OutputStream tmpOutStream = createTmpOutputStream(i);
                tmpFiles.add(calculateTmpFileName(i));

                HttpURLConnection urlConnection = openConnection(url);

                long calculatedEndPosition = Math.min(i + CHUNK_SIZE, contentLength);

                urlConnection.setRequestProperty("Range", "bytes=" + i + "-" + calculatedEndPosition);

                InputStream inputStream = urlConnection.getInputStream();

                int currentChunkIdx = (int) (i / CHUNK_SIZE) + 1;
                int totalChunks = (int) (contentLength / CHUNK_SIZE) + 1;
                byte[] data = new byte[CHUNK_SIZE];
                long chunkProgress = 0;
                int x = 0;
                while ((x = inputStream.read(data, 0, CHUNK_SIZE)) >= 0) {
                    chunkProgress += x;
                    final int currentProgress = (int) chunkProgress / (CHUNK_SIZE / 100);
                    tmpOutStream.write(data, 0, x);
                    System.out.print("\rChunk " + currentChunkIdx + "/" + totalChunks + " progress: " + currentProgress + "%");
                }
                inputStream.close();
                tmpOutStream.close();
                urlConnection.disconnect();

                i = calculatedEndPosition + 1;

            } catch (IOException e) {
                System.out.println("Chunk download exception....retrying...(n. of retry: " + retry + "/" + MAX_RETRY + ")");
                retry += 1;
                if (retry > MAX_RETRY) {
                    i = contentLength;
                }
            }
        }
        return tmpFiles;
    }

    private static String calculateTmpFileName(long i) {
        return TMP_FILE_NAME_PREFIX + "-" + i + ".chunk";
    }

    private static OutputStream createOutputStream() throws FileNotFoundException {
        File targetFile = new File(FILE_NAME);
        return new FileOutputStream(targetFile);
    }


    private static OutputStream createTmpOutputStream(long pos) throws FileNotFoundException {
        File targetFile = new File(calculateTmpFileName(pos));
        return new FileOutputStream(targetFile);
    }


    private static boolean checkIfAcceptsRangeHeader(HttpURLConnection urlConnection) {
        return urlConnection.getHeaderField("Accept-Ranges").equals("bytes");
    }

    private static long getContentLength(HttpURLConnection urlConnection) {
        return urlConnection.getContentLengthLong();
    }

    private static HttpURLConnection openConnection(URL url) throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    private static URL retrieveURL() throws MalformedURLException, URISyntaxException {
        return new URI(URI_STRING).toURL();
    }
}