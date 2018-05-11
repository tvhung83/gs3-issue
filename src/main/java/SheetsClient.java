import com.google.gdata.client.spreadsheet.CellQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.client.spreadsheet.WorksheetQuery;
import com.google.gdata.data.Link;
import com.google.gdata.data.PlainTextConstruct;
import com.google.gdata.data.batch.BatchOperationType;
import com.google.gdata.data.batch.BatchUtils;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;
import com.google.gdata.util.ServiceException;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

class SheetsClient
{
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final String APPLICATION_NAME = "google_sheets-capacity-issue-reproducer";
    private static final String ENTRY_URL_PREFIX = "https://spreadsheets.google.com/feeds/spreadsheets/private/full/";
    private String accessToken;
    private final String refreshToken;
    private final String clientId;
    private final String clientSecret;
    private final SpreadsheetService ssService;
    private final SpreadsheetEntry ssEntry;
    private final OkHttpClient httpClient = new OkHttpClient();
    private final Moshi moshi = new Moshi.Builder().build();
    private final JsonAdapter<Map> jsonAdapter = moshi.adapter(Map.class);
    private final Logger log = LogFactory.getInstance();

    SheetsClient(String refreshToken, String clientId, String clientSecret) throws IOException, ServiceException
    {
        this.refreshToken = refreshToken;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.ssService = new SpreadsheetService(APPLICATION_NAME);
        this.ssService.setConnectTimeout(3 * 60000); // 3 minutes
        this.ssService.setReadTimeout(5 * 60000);    // 5 minutes
        refreshToken();
        this.ssService.setProtocolVersion(SpreadsheetService.Versions.V3);

        HashMap<String, String> h = new HashMap<>();
        h.put("title", new Date().toString());
        h.put("uploadType", "media");
        h.put("mimeType", "application/vnd.google-apps.spreadsheet");

        String response = post("https://www.googleapis.com/drive/v2/files", jsonAdapter.toJson(h));
        Map<String, String> map = jsonAdapter.fromJson(response);
        URL entryUrl = new URL(ENTRY_URL_PREFIX + map.get("id"));
        this.ssEntry = ssService.getEntry(entryUrl, SpreadsheetEntry.class);
        log.info("Created new spreadsheet: " + this.ssEntry.getKey());
    }

    void uploadData(String worksheetName, List<String[]> values, int atRow) throws IOException, ServiceException
    {
        log.info("Expanding worksheet ...");
        WorksheetEntry worksheet = getWorksheetEntry(worksheetName);
        int current = worksheet.getRowCount();
        worksheet.setRowCount(current + values.size());
        worksheet.update();

        // request feed of empty cells
        CellQuery query = new CellQuery(worksheet.getCellFeedUrl());
        query.setMinimumRow(atRow - values.size() + 1);
        query.setMaximumRow(atRow);
        query.setMinimumCol(1);
        query.setMaximumCol(values.get(0).length);
        query.setReturnEmpty(true);
        log.info("Requesting CellFeed ...");
        CellFeed cellFeed = ssService.query(query, CellFeed.class);

        CellFeed batchRequestFeed = new CellFeed();

        // set values for each cell
        int currentCellEntry = 0;
        for (String[] row : values) {
            for (String cell : row) {
                CellEntry entry = new CellEntry(cellFeed.getEntries().get(currentCellEntry));
                entry.changeInputValueLocal(cell);
                BatchUtils.setBatchId(entry, String.valueOf(currentCellEntry));
                BatchUtils.setBatchOperationType(entry, BatchOperationType.UPDATE);
                batchRequestFeed.getEntries().add(entry);
                currentCellEntry++;
            }
        }

        // upload cells
        Link batchLink = cellFeed.getLink(Link.Rel.FEED_BATCH, Link.Type.ATOM);
        log.info("Uploading batch update request ...");
        ssService.batch(new URL(batchLink.getHref()), batchRequestFeed);
    }

    WorksheetEntry getWorksheetEntry(String worksheetName) throws IOException, ServiceException
    {
        WorksheetQuery wsQuery = new WorksheetQuery(ssEntry.getWorksheetFeedUrl());
        wsQuery.setTitleQuery(worksheetName);
        WorksheetFeed wsFeed = ssEntry.getService().query(wsQuery, WorksheetFeed.class);
        List<WorksheetEntry> list = wsFeed.getEntries();
        if (list == null || list.isEmpty()) {
            return null;
        }
        else {
            for (WorksheetEntry e : wsFeed.getEntries()) {
                if (!e.getTitle().isEmpty() && e.getTitle().getPlainText().equals(worksheetName)) {
                    return e;
                }
            }
            return null;
        }
    }

    private String post(String url, String json) throws IOException
    {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .header("Authorization", "Bearer " + accessToken)
                .post(body)
                .build();
        Response response = httpClient.newCall(request).execute();
        return response.body().string();
    }

    void add(String name, int rowCount, int colCount) throws IOException, ServiceException
    {
        WorksheetEntry wsEntry = new WorksheetEntry();
        wsEntry.setTitle(new PlainTextConstruct(name));
        wsEntry.setRowCount(rowCount);
        wsEntry.setColCount(colCount);
        URL wsFeedUrl = ssEntry.getWorksheetFeedUrl();
        ssService.insert(wsFeedUrl, wsEntry);
    }

    void refreshToken() throws IOException
    {
        RequestBody formBody = new FormBody.Builder()
                .add("grant_type", "refresh_token")
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .add("refresh_token", refreshToken)
                .build();
        Request request = new Request.Builder()
                .url("https://accounts.google.com/o/oauth2/token")
                .header("Accept", "application/json")
                .post(formBody)
                .build();
        Response response = httpClient.newCall(request).execute();
        this.accessToken = (String) jsonAdapter.fromJson(response.body().string()).get("access_token");
        this.ssService.setHeader("Authorization", "Bearer " + accessToken);
    }
}
