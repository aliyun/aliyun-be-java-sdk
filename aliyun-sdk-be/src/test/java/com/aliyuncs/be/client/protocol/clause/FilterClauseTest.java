package com.aliyuncs.be.client.protocol.clause;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
@Slf4j
public class FilterClauseTest {

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
    public void testSimple() throws Exception {
        assertEquals(encode("score<0"), buildFilterValue(single1));
        assertEquals(encode("score<0 AND field1='value1' AND field2!=field3"),
                buildFilterValue(new MultiFilter(FilterConnector.AND, single1, single2, single3)));
        assertEquals(encode("score<0 OR (field1='value1' AND field2!=field3)"),
                buildFilterValue(new MultiFilter(FilterConnector.OR, single1, new MultiFilter(FilterConnector.AND, single2, single3))));
        assertEquals(encode("score<0 OR (field1='value1' AND field2!=field3) OR in_filter()"),
                buildFilterValue(new MultiFilter(FilterConnector.OR, single1, new MultiFilter(FilterConnector.AND, single2, single3), inFilter)));
    }

    private String encode(String param) throws UnsupportedEncodingException {
        return URLEncoder.encode(param, "UTF-8");
    }

    private String buildFilterValue(BeFilter condition) throws InvalidParameterException {
        FilterClause clause = new FilterClause(condition);
        String value = clause.buildParams();
        log.info("Value:\n" + value);
        return value;
    }
}