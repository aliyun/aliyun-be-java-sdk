package com.aliyuncs.be.client.protocol;

import com.aliyuncs.be.client.BeReadRequest;
import com.aliyuncs.be.client.BeResult;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.ClientTestBase;
import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.clause.filter.*;
import com.aliyuncs.be.client.protocol.clause.score.ScorerClause;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.net.URLEncoder;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Slf4j
public class DefaultRequestBuilderTest extends ClientTestBase {

    private BeReadRequest readRequest;
    private DefaultRequestBuilder builder = new DefaultRequestBuilder();
    private String domain = "domain.for.test";
    private int port = 80;


    private SingleFilter single1;
    private SingleFilter single2;
    private SingleFilter single3;
    private InFilter inFilter;

    @Before
    public void setUp() throws Exception {
        single1 = new SingleFilter("score", FilterOperator.LT, "0");
        single2 = new SingleFilter("field1", FilterOperator.EQ, "'value1'");
        single3 = new SingleFilter("field2", FilterOperator.NE, "field3");
        inFilter = new InFilter("inField", Arrays.asList("v1", "v2", "v3"));
    }

    @Test
    public void testRequest() throws InvalidParameterException {
        BeReadRequest request = fromResourceJson("query/request.json", BeReadRequest.class);
        String url = builder.buildReadUri(domain, port, request);
        assertEquals("http://domain.for.test:80/be?biz_name=searcher&p=testBiz&s=testBiz&return_count=10&outfmt=json2&x2i_trigger_list=A:1,B:2,C:3&x2i_score_rule=weight*score&x2i_return_count=5&vector_trigger_list=1,1,1,1;2,2,2,2;3,3,3,3&vector_score_rule=weight*score&vector_return_count=5",
                url);
    }

    @Test
    public void testFilter() throws InvalidParameterException {
        assertEquals("http://domain.for.test:80/be?biz_name=searcher&p=test_biz&s=test_biz&return_count=0&outfmt=json2&trigger_list=1001:1.0,1002:2.0,1003:3.0&score_rule=score*double%28weight%29&filter_rule=score%3C0",
                buildReadUri(single1));
        assertEquals("http://domain.for.test:80/be?biz_name=searcher&p=test_biz&s=test_biz&return_count=0&outfmt=json2&trigger_list=1001:1.0,1002:2.0,1003:3.0&score_rule=score*double%28weight%29&filter_rule=in_filter%28%29&in_filter_field=inField&in_filter_values=v1,v2,v3",
                buildReadUri(inFilter));
        assertEquals("http://domain.for.test:80/be?biz_name=searcher&p=test_biz&s=test_biz&return_count=0&outfmt=json2&trigger_list=1001:1.0,1002:2.0,1003:3.0&score_rule=score*double%28weight%29&filter_rule=score%3C0+OR+field1%3D%27value1%27+OR+%28field2%21%3Dfield3+AND+in_filter%28%29%29&in_filter_field=inField&in_filter_values=v1,v2,v3",
                buildReadUri(new MultiFilter(FilterConnector.OR, single1, single2, new MultiFilter(FilterConnector.AND, single3, inFilter))));
    }

    private String buildReadUri(BeFilter filter) throws InvalidParameterException {
        RecallParam param = RecallParam.builder()
                .triggerItems(Arrays.asList("1001:1.0","1002:2.0","1003:3.0"))
                .scorerClause(new ScorerClause("score*double(weight)"))
                .recallType(RecallType.X2I)
                .build();
        BeReadRequest request = BeReadRequest.builder()
                .bizName("test_biz")
                .recallParams(Arrays.asList(param))
                .filterClause(new FilterClause(filter))
                .build();
        String uri = builder.buildReadUri(domain, port, request);
        log.info(uri);
        return uri;
    }

    @Test
    public void testBuildWriteRequest() throws Exception {
        HashMap<String, String> contents = new HashMap<>();
        contents.put("id", "111111");
        List<Map<String, String>> contentList = Arrays.asList(contents);
        BeWriteRequest writeRequest = BeWriteRequest.builder()
                .type(BeWriteType.ADD)
                .primaryKey("id")
                .tableName("test6")
                .instanceName("testInstance")
                .contents(contentList)
                .writeMethod("GET")
                .build();
        String uri = builder.buildWriteUri(domain, port, writeRequest, 0);
        log.info("Get uri:\n" + uri);
        assertEquals("http://domain.for.test:80/sendmsg?table=test6&h=1449589344&msg=CMD%3Dadd%1F%0Aid%3D111111%1F%0A", uri);

        writeRequest.setWriteMethod("POST");
        uri = builder.buildWriteUri(domain, port, writeRequest, 0);
        log.info("Post uri:\n" + uri);
        assertEquals("http://domain.for.test:80/sendmsg", uri);
        String postContent = writeRequest.buildContent(0);
        log.info("Content:\n" + postContent);
        assertEquals("table=testInstance_test6&h=1449589344&msg=CMD%3Dadd%1F%0Aid%3D111111%1F%0A", postContent);
    }

    @Test
    public void testAddRequest() throws Exception {
        StringBuilder contentBuilder = new StringBuilder();
        char separator = (char) 31;
        contentBuilder.append("CMD=delete").append(separator).append('\n')
                .append("id=-5656019669429076362").append(separator).append('\n');
        log.info(URLEncoder.encode(contentBuilder.toString(), "UTF-8"));
    }

    @Test
    public void testAdd() throws Exception {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        Gson gson = gsonBuilder.create();

        String content = ClientTestBase.resourceLoad("query/query_result.json");
        BeResult result = gson.fromJson(content, BeResult.class);
        BeResult.MatchItems matchItems = result.getMatchItems();
        List<String> fields = matchItems.getFieldNames();
        List<String> values = matchItems.getFieldValues().get(0);
        StringBuilder contentBuilder = new StringBuilder();
        char separator = (char) 31;
        contentBuilder.append("CMD=add").append(separator).append('\n');
        for (int i = 0; i < fields.size(); i++) {
            contentBuilder.append(fields.get(i)).append("=").append(values.get(i))
                    .append(separator).append('\n');
        }
        log.info("url:\n" + URLEncoder.encode(contentBuilder.toString(), "UTF-8"));

    }
}