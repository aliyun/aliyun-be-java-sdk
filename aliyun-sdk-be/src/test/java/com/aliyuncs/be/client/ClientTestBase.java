package com.aliyuncs.be.client;

import com.google.gson.Gson;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

@Slf4j
public class ClientTestBase {

    private static String read(String filePath) throws IOException {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String result;
        try {
            br = new BufferedReader(new FileReader(filePath));

            while ((result = br.readLine()) != null) {
                sb.append(result);
                sb.append("\n");
            }
        } catch (Exception var11) {
            log.error(String.format("Failed to read file[%s].", filePath), var11);
            throw new IOException(var11);
        } finally {
            try {
                if (null != br) {
                    br.close();
                }
            } catch (IOException var10) {
                log.error(String.format("Failed to close file[%s].", filePath), var10);
            }

        }

        result = sb.toString();
        if (result.length() >= 1) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }

    public static String resourceLoad(String fileName) throws IOException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);
        if (StringUtils.isNotBlank(url.getPath())) {
            String fileContent = read(url.getPath());
            return fileContent;
        } else {
            throw new IOException(String.format("%s not exist", fileName));
        }
    }

    public static <T> T fromResourceJson(String jsonFileName, Class<T> cls) {
        try {
            Gson gson = new Gson();
            return gson.fromJson(resourceLoad(jsonFileName), cls);
        } catch (IOException var3) {
            throw new RuntimeException(String.format("Read file [%s] failed", jsonFileName), var3);
        }
    }

    protected TestReadParams buildTestReadParam(String paramPath) {
        return fromResourceJson(paramPath, TestReadParams.class);
    }

    @Data
    public static class TestReadParams {
        private BeReadRequest request;
        private List<TestChecker> checkers;

        public boolean check(BeResult result) {
            if (result == null) {
                log.error("Result is null");
                return false;
            }
            if (result.getErrorCode() != 0) {
                log.error("Error code[{}], message:[{}]", result.getErrorCode(), result.getErrorMessage());
                return false;
            }
            if (result.getItemsCount() == 0) {
                log.error("Empty result");
                return false;
            }

            if (CollectionUtils.isEmpty(checkers)) {
                return true;
            }
            for (TestChecker checker : checkers) {
                if (!checker.check(result)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Data
    public static class TestChecker {
        private String field;
        private List<Object> values;

        public boolean check(BeResult result) {
            for (int i = 0; i < result.getItemsCount(); i++) {
                Map<String, Object> item = result.getItem(i);
                Object fieldValue = item.get(field);
                if (fieldValue == null) {
                    log.error("Field[{}] not exist in item:{}", field, new Gson().toJson(item));
                    return false;
                }
                if (!values.contains(fieldValue)) {
                    log.error("Field[{}] item:{} not contain expected values:{}",
                            field, new Gson().toJson(item), new Gson().toJson(values));
                    return false;
                }
            }
            return true;
        }
    }
}
