import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.gdata.util.ServiceException;
import org.fluttercode.datafactory.impl.DataFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class Main
{
    private static DataFactory df = new DataFactory();
    private static String[] generateRow()
    {
        // 10 columns
        return new String[] {
                    String.valueOf(System.currentTimeMillis()),
                    df.getFirstName(),
                    df.getLastName(),
                    df.getAddress(),
                    df.getAddressLine2(),
                    df.getCity(),
                    df.getEmailAddress(),
                    df.getRandomWord(10, 20),
                    df.getNumberText(5),
                    df.getRandomWord(100)
            };
    }

    public static void main(String[] args) throws IOException, ServiceException
    {
        final URL url = Resources.getResource("config.properties");
        final byte[] byteSource = Resources.toByteArray(url);
        final Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(byteSource));
        Logger log = LogFactory.getInstance();
        // configs
        int rowCount = 199999; // 199999 * 10
        int bufSize = 300; // upload each batch of 300 rows
        String title = String.valueOf(System.currentTimeMillis());

        SheetsClient client = new SheetsClient(properties.getProperty("refresh_token"), properties.getProperty("client_id"), properties.getProperty("client_secret"));
        // 10 columns
        log.info("Adding new worksheet: " + title + " ...");
        client.add(title, 1, 10);
        log.info("Deleting default worksheet ... ");
        // delete default sheet to save some cells (26 * 1000)
        client.getWorksheetEntry("Sheet1").delete();

        List<String[]> buf = Lists.newLinkedList();
        int curRow = 1;
        for (int i = 0; i < rowCount; i++) {
            buf.add(generateRow());
            // flush
            if (buf.size() == bufSize) {
                curRow += bufSize;
                try {
                    client.uploadData(title, buf, curRow);
                }
                catch (Throwable e) {
                    // token is expired
                    client.refreshToken();
                    client.uploadData(title, buf, curRow);
                }
                log.info("Uploaded: " + curRow);
                buf = Lists.newLinkedList();
            }
        }
    }
}
