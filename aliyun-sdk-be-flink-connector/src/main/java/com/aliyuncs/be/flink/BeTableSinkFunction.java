package com.aliyuncs.be.flink;

import com.aliyuncs.be.client.BeClient;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.aliyuncs.be.flink.BeDynamicTableFactory.*;

/**
 * @author silan.wpq
 * @date 2022/9/26
 */
@Slf4j
public class BeTableSinkFunction extends AbstractBeRichSinkFunction<RowData> implements CheckpointedFunction {
    private Set<String> fields;
    private DataStructureConverter converter;
    private DataType dataType;
    private ReadableConfig options;

    private String tableName;
    private String pkField;
    private String cmdField;
    private boolean dryRun;
    private boolean async;
    private int nextSlot;

    /**
     * Parallelism number with async mode.
     */
    private int connectionPoolSize;

    /**
     * Concurrent request executor and callback.
     */
    private ExecutorService executor;
    private Future[] resultFeatures;

    /**
     * Concurrent compute resource management.
     */
    private Queue<Integer> availableSlots;


    /**
     * Cache store for submitted but not completed requests.
     */
    private Map<String, Tuple2<Integer, BeWriteRequest>> activeRequests;


    /**
     * Queue for not submitted requests.
     */
    private Queue<BeWriteRequest> readyRequests;

    /**
     * List contains all ready and active requests for snapshot.
     */
    private transient ListState<BeWriteRequest> elementState;

    public BeTableSinkFunction(DataStructureConverter converter, DataType dataType, ReadableConfig options) {
        this.converter = converter;
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RowType logicalType = (RowType) dataType.getLogicalType();
        fields = logicalType.getFields().stream()
                .map(RowType.RowField::getName)
                .collect(Collectors.toSet());
        tableName = options.get(TABLE);
        String endpoint = options.get(ENDPOINT);
        int port = options.get(PORT);
        String userName = options.get(USERNAME);
        String passWord = options.get(PASSWORD);
        client = new BeClient(endpoint, port, userName, passWord);
        pkField = options.get(PK_FIELD);
        cmdField = options.get(CMD_FIELD);
        retryNum = Math.max(0, options.get(REQUEST_RETRY));
        dryRun = options.get(DRY_RUN);
        async = options.get(ASYNC);
        connectionPoolSize = options.get(CONNECTION_POOL_SIZE);
        nextSlot = 0;

        if (async) {
            executor = Executors.newFixedThreadPool(connectionPoolSize);
            resultFeatures = new Future[connectionPoolSize];
            activeRequests = new HashMap<>();
            availableSlots = new LinkedList<>();
        }

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (async) {
            refreshSlotResource();
            elementState.clear();
            elementState.addAll(new ArrayList<>(readyRequests));
            for (Map.Entry<String, Tuple2<Integer, BeWriteRequest>> entry : activeRequests.entrySet()) {
                elementState.add(entry.getValue().f1);
            }

            consumeUnfinishedElement();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initializeState begin");
        ListStateDescriptor<BeWriteRequest> elementStateDescriptor =
                new ListStateDescriptor<>(
                        "elementState",
                        TypeInformation.of(new TypeHint<BeWriteRequest>() {
                        }));
        elementState = context.getOperatorStateStore().getListState(elementStateDescriptor);
        readyRequests = new LinkedList<>();
        if (context.isRestored()) {
            for (BeWriteRequest element : elementState.get()) {
                readyRequests.add(element);
            }
            log.info("recover from snapshot get unfinished list {}", readyRequests);
        }
        log.info("initializeState end");

    }


    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        // precondition check
        RowKind rowKind = rowData.getRowKind();
        if (!RowKind.INSERT.equals(rowKind)) {
            log.warn("Only INSERT RowKind supported, but:" + rowKind.name());
            return;
        }

        Row data = (Row) converter.toExternal(rowData);
        if (data == null) {
            return;
        }

        if (data.getField(pkField) == null) {
            log.warn("Pk field[{}] not found", pkField);
            return;
        }

        if (data.getField(cmdField) == null) {
            log.warn("Cmd field[{}] not found", cmdField);
            return;
        }

        String cmdValue = data.getField(cmdField).toString();
        if (!StringUtils.equalsIgnoreCase(BeWriteType.ADD.name(), cmdValue) &&
                !StringUtils.equalsIgnoreCase(BeWriteType.DELETE.name(), cmdValue)) {
            log.warn("Cmd value[{}] not supported, only support ADD and DELETE", cmdValue);
            return;
        }
        BeWriteType writeType = BeWriteType.valueOf(StringUtils.upperCase(cmdValue));

        Map<String, String> contents = new HashMap<>();
        for (String field : fields) {
            if (data.getField(field) != null) {
                contents.put(field, data.getField(field).toString());
            }
        }

        List<Map<String, String>> contentsList = new ArrayList<>();
        contentsList.add(contents);
        BeWriteRequest request = BeWriteRequest.builder()
                .primaryKey(pkField)
                .type(writeType)
                .contents(contentsList)
                .tableName(tableName)
                .build();

        if (dryRun) {
            log.info("Request:" + new Gson().toJson(request));
        } else if (async) {
            refreshSlotResource();
            readyRequests.offer(request);
            consumeUnfinishedElement();
        } else {
            sendRequest(request);
        }

    }

    private void consumeUnfinishedElement() throws ExecutionException, InterruptedException {

        while (!readyRequests.isEmpty()) {
            // get enough resource
            BeWriteRequest beWriteRequest = readyRequests.poll();
            String pk = beWriteRequest.getContents().get(0).get(beWriteRequest.getPrimaryKey());
            if (activeRequests.containsKey(pk)) {
                int occupiedSlotIndex = activeRequests.get(pk).f0;
                String result = (String) resultFeatures[occupiedSlotIndex].get();
                clearSlotResourceAndCache(result);
            } else {
                refreshSlotResource();
                if (availableSlots.isEmpty()) {
                    String result = (String) resultFeatures[nextSlot].get();
                    clearSlotResourceAndCache(result);
                }
            }


            // deliver request to thread
            int slotIndex = availableSlots.poll();
            activeRequests.put(pk, Tuple2.of(slotIndex, beWriteRequest));

            Future<String> future = sendBeRequest(beWriteRequest);
            resultFeatures[slotIndex] = future;
        }
    }

    /**
     * Check concurrent requests callback result and recycle compute slot.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void refreshSlotResource() throws ExecutionException, InterruptedException {
        for (int i = 0; i < resultFeatures.length; ++i) {
            if (resultFeatures[i] != null && resultFeatures[i].isDone()) {
                String result = (String) resultFeatures[i].get();
                clearSlotResourceAndCache(result);
            }

            if (resultFeatures[i] == null && !availableSlots.contains(i)) {
                availableSlots.offer(i);
                nextSlot = (i + 1) % connectionPoolSize;
            }
        }
    }

    private void clearSlotResourceAndCache(String pk) {
        int slotIndex = activeRequests.get(pk).f0;
        activeRequests.remove(pk);
        resultFeatures[slotIndex] = null;
        availableSlots.offer(slotIndex);
    }

    private Future<String> sendBeRequest(BeWriteRequest beWriteRequest) {
        return executor.submit(() -> sendRequest(beWriteRequest));
    }
}
