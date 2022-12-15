package com.aliyuncs.be.client.protocol.clause.index;

import static org.junit.Assert.*;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.List;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
@Slf4j
public class IndexQueryParserTest {

    @Test
    public void testParseQuery() throws InvalidParameterException {
        indexQueryEqual("(a=1)", "{\"match\":{\"a\":\"1\"}}");
        indexQueryEqual("(((a=1)))", "{\"match\":{\"a\":\"1\"}}");
        indexQueryEqual("itemtype=100", "{\"match\":{\"itemtype\":\"100\"}}");
        indexQueryEqual("a=1 and b=2",
                "{\"and\":[{\"match\":{\"a\":\"1\"}},{\"match\":{\"b\":\"2\"}}]}");
        indexQueryEqual("(a=1 and b=2) or c=3 and e=2",
                "{\"or\":[{\"and\":[{\"match\":{\"a\":\"1\"}},{\"match\":{\"b\":\"2\"}}]},{\"and\":[{\"match\":{\"c\":\"3\"}},{\"match\":{\"e\":\"2\"}}]}]}");
        indexQueryEqual("(a=2 and (b=3 or (c=4 or a=1)))",
                "{\"and\":[{\"match\":{\"a\":\"2\"}},{\"or\":[{\"match\":{\"b\":\"3\"}},{\"or\":[{\"match\":{\"c\":\"4\"}},{\"match\":{\"a\":\"1\"}}]}]}]}");
        indexQueryEqual("a=1 and b=2 or c=3 and d=4 or (a=5 and d=6 or (e=7 or d=8))",
                "{\"or\":[{\"and\":[{\"match\":{\"a\":\"1\"}},{\"match\":{\"b\":\"2\"}}]},{\"and\":[{\"match\":{\"c\":\"3\"}},{\"match\":{\"d\":\"4\"}}]},{\"or\":[{\"and\":[{\"match\":{\"a\":\"5\"}},{\"match\":{\"d\":\"6\"}}]},{\"or\":[{\"match\":{\"e\":\"7\"}},{\"match\":{\"d\":\"8\"}}]}]}]}");
        indexQueryEqual("(a=an and b=2 or c=3) and d=4 or (a=5 and d=6 or (e=7 or d=8))",
                "{\"or\":[{\"and\":[{\"or\":[{\"and\":[{\"match\":{\"a\":\"an\"}},{\"match\":{\"b\":\"2\"}}]},{\"match\":{\"c\":\"3\"}}]},{\"match\":{\"d\":\"4\"}}]},{\"or\":[{\"and\":[{\"match\":{\"a\":\"5\"}},{\"match\":{\"d\":\"6\"}}]},{\"or\":[{\"match\":{\"e\":\"7\"}},{\"match\":{\"d\":\"8\"}}]}]}]}");
    }

    private void indexQueryEqual(String expression, String query) throws InvalidParameterException {
        IndexQueryParser parser = new IndexQueryParser(expression);
        assertEquals(parser.buildQueryClause(false), query);
    }

}