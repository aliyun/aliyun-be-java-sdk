package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.AgentRequestBuilder;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author silan.wpq
 * @date 2021/7/19
 */
@Slf4j
public class BeClientTest extends ClientTestBase {

    private AgentRequestBuilder requestBuilder;
    private BeClient client;

    @Before
    public void setUp() throws Exception {
        requestBuilder = new AgentRequestBuilder();
    }

    @Test
    public void testRead() throws Exception {
        String domain = "10.2.207.14";
        int port = 16832;
        client = new BeClient(domain, port, "", "", requestBuilder);
        checkRecallParams("query/x2i_recall_param.json");
        checkRecallParams("query/x2i_recall_param_with_exposure.json");
        checkRecallParams("query/vector_request.json");
        checkRecallParams("query/vector_request_with_filter.json");
        checkRecallParams("query/multi_request.json");
    }

    private void checkRecallParams(String paramPath) throws InvalidParameterException {
        TestReadParams params = buildTestReadParam(paramPath);
        BeResponse<BeResult> resp = query(params.getRequest());
        assertTrue("Failed to query, resp:" + resp.getMessage(), resp.success);
        assertTrue(params.check(resp.result));
    }


    private BeResponse<BeResult> query(BeReadRequest request) throws InvalidParameterException {
        BeResponse<BeResult> resp = client.query(request);
        log.info("Query resp:\n" + new Gson().toJson(resp));
        log.info("Items count:" + resp.getResult().getItemsCount());
        return resp;
    }

    @Test
    public void testWrite() throws Exception {
        long begin = System.currentTimeMillis() * 1000;

        String domain = "10.0.141.19";
        int port = 80;
        String tableName = "be-cn-7e22hft5l001_aime_example_expose_2";

        // add
        BeWriteRequest addRequest = fromResourceJson("write/write_exposure_request.json", BeWriteRequest.class);

        //addRequest.setTableName(tableName);
        String id = addRequest.getContents().get(0).get("id");
        client = new BeClient(domain, port, "", "", requestBuilder);
        BeResponse resp = client.write(addRequest);
        log.info("Write add resp:\n" + new Gson().toJson(resp));
        assertNotNull(resp);
        assertTrue(resp.isSuccess());

        // delete
        HashMap<String, String> contents = new HashMap<>();
        contents.put("id", id);
        List<Map<String, String>> contentList = Collections.singletonList(contents);
        BeWriteRequest deleteRequest = BeWriteRequest.builder()
                .type(BeWriteType.DELETE)
                .tableName(tableName)
                .primaryKey("id")
                .contents(contentList)
                .build();
        resp = client.write(deleteRequest);
        log.info("Write delete resp:\n" + new Gson().toJson(resp));
        assertNotNull(resp);
        assertTrue(resp.isSuccess());

        long end = (System.currentTimeMillis() + 1000) * 1000;

        // check write success
        JsonArray recordsGet = getSwiftRecord(domain, port, tableName, begin, end, 10, 0);
        assertEquals(2, recordsGet.size());
    }

    private JsonArray getSwiftRecord(String gatewayHost, int gatewayPort, String topicName,
                                     long beginTime, long endTime, int maxCount, int partition) throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.addParameter("table", topicName);
        builder.addParameter("p", String.valueOf(partition));
        builder.addParameter("b", String.valueOf(beginTime));
        builder.addParameter("e", String.valueOf(endTime));
        builder.addParameter("l", String.valueOf(maxCount));
        builder.setHost(gatewayHost);
        builder.setPort(gatewayPort);
        builder.setPath("getmsg");
        builder.setScheme("http");
        requestBuilder.rewriteAgentHost(gatewayHost, gatewayPort, builder);
        String url = URLDecoder.decode(builder.toString(), "UTF-8");

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;

        response = httpClient.execute(httpGet);
        HttpEntity httpEntity = response.getEntity();
        if (httpEntity == null) {
            throw new RuntimeException("Empty entity");
        }

        String responseStr = EntityUtils.toString(httpEntity);
        log.info("Get swift result:\n" + responseStr);
        return new Gson().fromJson(responseStr, JsonArray.class);
    }

}