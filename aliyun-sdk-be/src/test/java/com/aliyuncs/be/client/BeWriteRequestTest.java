package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.DefaultRequestBuilder;
import org.junit.Test;

/**
 * @author silan.wpq
 * @date 2021/7/19
 */
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
        String url = builder.buildWriteUri("http://127.0.0.1", 80, request, 0);
        System.out.println(url);
    }

    private BeWriteRequest newRequest() {
        return fromResourceJson("write/write_request.json", BeWriteRequest.class);
    }
}