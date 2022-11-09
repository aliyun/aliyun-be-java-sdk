package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.DefaultRequestBuilder;
import com.aliyuncs.be.client.protocol.RequestBuilder;
import com.google.gson.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Slf4j
public class BeClient {

    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String SOCKET_TIMEOUT = "socketTimeout";

    private static final Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        gson = gsonBuilder.create();
    }

    private final int port;
    private final String domain;
    private final RequestBuilder requestBuilder;
    private final String authHeader;
    private final CloseableHttpClient httpClient;

    public BeClient(String domain, int port, String userName, String passWord) {
        this(domain, port, userName, passWord, new Properties(), new DefaultRequestBuilder());
    }

    public BeClient(String domain, int port, String userName, String passWord, Properties properties) {
        this(domain, port, userName, passWord, properties, new DefaultRequestBuilder());
    }

    protected BeClient(String domain, int port, String userName, String passWord, RequestBuilder requestBuilder) {
        this(domain, port, userName, passWord, new Properties(), requestBuilder);
    }

    protected BeClient(String domain, int port, String userName, String passWord,
                       Properties properties, RequestBuilder requestBuilder) {
        if (properties == null) {
            properties = new Properties();
        }

        this.domain = domain;
        this.port = port;
        this.requestBuilder = requestBuilder;

        String auth = userName + ":" + passWord;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
        authHeader = "Basic " + new String(encodedAuth);

        // TODO support user define http connection pool
        PoolingHttpClientConnectionManager connectionManager
                = new PoolingHttpClientConnectionManager(60000, TimeUnit.MILLISECONDS);
        connectionManager.setMaxTotal(50);
        connectionManager.setDefaultMaxPerRoute(50);

        int connectTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT, "1000"));
        int socketTimeout = Integer.parseInt(properties.getProperty(SOCKET_TIMEOUT, "3000"));
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        try {
                            return Math.max(1, (Long.parseLong(value) - 2)) * 1000;
                        } catch(NumberFormatException ignore) {
                        }
                    }
                }
                return 30 * 1000;
            }
        };

        httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionManager)
                .setKeepAliveStrategy(keepAliveStrategy)
                .disableAutomaticRetries()
                .build();
    }

    public BeResponse<BeResult> query(BeReadRequest request) throws InvalidParameterException {
        if (request == null) {
            throw new InvalidParameterException("Request should not be null");
        }
        request.validate();
        String url = requestBuilder.buildReadUri(domain, port, request);
        return getForResult(url, resp -> {
            BeResult result;
            try {
                result = gson.fromJson(resp, BeResult.class);
            } catch (Throwable e) {
                return BeResponse.buildFailureResponse("Failed to parse be result:" + resp);
            }
            return BeResponse.buildSuccessResponse(result);
        });
    }

    public BeResponse write(BeWriteRequest request) throws InvalidParameterException {
        if (request == null) {
            throw new InvalidParameterException("Request should not be null");
        }
        request.validate();
        // TODO modify to batch put
        BeResponse beResponse = null;
        for (int i = 0; i < request.getContents().size(); i++) {
            String url = requestBuilder.buildWriteUri(domain, port, request, i);
            beResponse = getForResult(url, resp -> {
                JsonObject respJson;
                try {
                    respJson = gson.fromJson(resp, JsonObject.class);
                    JsonElement codeElement = respJson.get("errno");
                    if (codeElement == null) {
                        return BeResponse.buildFailureResponse("Illegal write resp, no errno found, resp:" + resp);
                    }
                    int code = codeElement.getAsInt();
                    if (code == 0) {
                        return BeResponse.buildSuccessResponse(null);
                    } else if (code == 1) {
                        return BeResponse.buildFailureResponse("Failed to write be, illegal request body, error code:" + code);
                    } else {
                        return BeResponse.buildFailureResponse("Failed to write be, error code:" + code);
                    }
                } catch (Throwable e) {
                    return BeResponse.buildFailureResponse("Failed to parse be result:" + resp);
                }
            });
            if (!beResponse.isSuccess()) {
                return beResponse;
            }
        }
        return beResponse;
    }

    private <T> BeResponse<T> getForResult(String query, Function<String, BeResponse<T>> resultHandler) {
        HttpGet httpGet = new HttpGet(query);
        CloseableHttpResponse response = null;
        try {
            httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
            response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() != 200) {
                String respEntity = EntityUtils.toString(response.getEntity());
                return BeResponse.buildFailureResponse(String.format("Error code[%s], reason[%s], resp entity[%s]",
                        statusLine.getStatusCode(), statusLine.getReasonPhrase(), respEntity));
            }
            HttpEntity httpEntity = response.getEntity();
            if (httpEntity != null) {
                String responseStr = EntityUtils.toString(httpEntity);
                return resultHandler.apply(responseStr);
            } else {
                return BeResponse.buildFailureResponse("Failed to request be, empty response");
            }
        } catch (Exception e) {
            //TODO handler exception
            String message = "Failed to request be, error message:" + e.getMessage();
            log.error(message, e);
            return BeResponse.buildFailureResponse(message);
        } finally {
            if (response != null) {
                try {
                    //response.close();
                    EntityUtils.consume(response.getEntity());
                } catch (IOException ignore) {
                }
            }
        }
    }
}
