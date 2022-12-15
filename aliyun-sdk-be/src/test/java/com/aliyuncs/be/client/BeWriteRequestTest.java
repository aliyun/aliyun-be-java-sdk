package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.aliyuncs.be.client.protocol.DefaultRequestBuilder;
import lombok.extern.slf4j.Slf4j;
import static org.junit.Assert.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author silan.wpq
 * @date 2021/7/19
 */
@Slf4j
public class BeWriteRequestTest extends ClientTestBase {

    @Test
    public void testValidate() throws InvalidParameterException {
        newRequest().validate();
    }

    @Test(expected = InvalidParameterException.class)
    public void testValidateType() throws InvalidParameterException {
        BeWriteRequest request = newRequest();
        request.setType(null);
        request.validate();
    }

    @Test(expected = InvalidParameterException.class)
    public void testValidateTable() throws InvalidParameterException {
        BeWriteRequest request = newRequest();
        request.setTableName(null);
        request.validate();
    }

    @Test(expected = InvalidParameterException.class)
    public void testValidateContents() throws InvalidParameterException {
        BeWriteRequest request = newRequest();
        request.setContents(null);
        request.validate();
    }

    @Test
    public void testWriteRequest() throws InvalidParameterException {
        BeWriteRequest request = fromResourceJson("write/write_vector_request.json", BeWriteRequest.class);
        DefaultRequestBuilder builder = new DefaultRequestBuilder();
        String url = builder.buildWriteUri("127.0.0.1", 80, request, 0);
        System.out.println(url);
        assertEquals("http://127.0.0.1:80/sendmsg?table=be-cn-zvp2j6v1z001_mock_vec_table&h=49&msg=CMD%3Dadd%1F%0Aitem_id%3D1%1F%0Avector%3D1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C1%1F%0A", url);
    }

    @Test
    public void testWriteBuilder() throws InvalidParameterException {
        String tableName = "exposure_table_inc";
        String instanceName = "be-cn-7mz2xhe0w001";
        Map<String, String> content = new HashMap<>();
        content.put("user_id", "silan");
        content.put("item_id", "390433");
        content.put("bhv_time", "1671075848");

        List<Map<String, String>> contents = new ArrayList<>();
        contents.add(content);

        // post
        BeWriteRequest postRequest = BeWriteRequest.builder()
                .tableName(tableName)
                .primaryKey("user_id")
                .type(BeWriteType.ADD)
                .contents(contents)
                .instanceName(instanceName)
                .postWrite(true)
                .build();
        String postUri = new DefaultRequestBuilder().buildWriteUri("127.0.0.1", 80, postRequest, 0);
        log.info("post uri:" + postUri);
        assertEquals("http://127.0.0.1:80/sendmsg", postUri);
        log.info("post data:" + postRequest.buildContent(0));
        assertEquals("table=be-cn-7mz2xhe0w001_exposure_table_inc&h=109439875&msg=CMD%3Dadd%1F%0Auser_id%3Dsilan%1F%0Abhv_time%3D1671075848%1F%0Aitem_id%3D390433%1F%0A", postRequest.buildContent(0));

        BeWriteRequest getRequest = BeWriteRequest.builder()
                .tableName(tableName)
                .primaryKey("user_id")
                .type(BeWriteType.ADD)
                .contents(contents)
                .build();
        String getUri = new DefaultRequestBuilder().buildWriteUri("127.0.0.1", 80, getRequest, 0);
        log.info("get uri:" + getUri);
        assertEquals("http://127.0.0.1:80/sendmsg?table=exposure_table_inc&h=109439875&msg=CMD%3Dadd%1F%0Auser_id%3Dsilan%1F%0Abhv_time%3D1671075848%1F%0Aitem_id%3D390433%1F%0A", getUri);



    }

    private BeWriteRequest newRequest() {
        return fromResourceJson("write/write_request.json", BeWriteRequest.class);
    }
}