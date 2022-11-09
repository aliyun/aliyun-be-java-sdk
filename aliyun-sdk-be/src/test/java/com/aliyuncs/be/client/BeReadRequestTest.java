package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.DefaultRequestBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author silan.wpq
 * @date 2021/7/19
 */
@Slf4j
public class BeReadRequestTest extends ClientTestBase {

    private BeReadRequest request;
    private DefaultRequestBuilder requestBuilder;

    @Before
    public void setUp() throws Exception {
        request = fromResourceJson("query/kv_request.json", BeReadRequest.class);
        requestBuilder = new DefaultRequestBuilder();
    }

    private String buildUrl(BeReadRequest request) throws InvalidParameterException {
        return requestBuilder.buildReadUri("domainTest", 80, request);
    }

    @Test
    public void testRawQuery() throws Exception {
        request = fromResourceJson("query/raw_request.json", BeReadRequest.class);
        request.validate();
        Assert.assertEquals("http://domainTest:80/be?biz_name=searcher&p=airec_test&s=airec_test&return_count=10&outfmt=json2&key1=value1&key2=value2&key3=value3", buildUrl(request));
    }

    @Test(expected = InvalidParameterException.class)
    public void testRequestValidate() throws InvalidParameterException {
        request.setReturnCount(0);
        request.validate();
    }
}