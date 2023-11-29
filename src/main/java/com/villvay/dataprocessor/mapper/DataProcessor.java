package com.villvay.dataprocessor.mapper;

import com.codahale.metrics.SlidingWindowReservoir;
import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.ValidationMessage;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;


/**
 * @author Ilman Iqbal
 * 11/16/2023
 */
public class DataProcessor<T> extends ProcessAllWindowFunction<String, T, TimeWindow> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    private transient Counter counter;

    private transient Histogram histogram;

    private transient Meter meter;

    private transient ListState<Long> processedRecordCount;

    long oldCounterValue = 0L;

    Class<T> pojoClass;
    String jsonSchema;
    String[] jsonFields;

    public DataProcessor(Class<T> pojoClass, String jsonSchema, String... jsonFields) {
        this.pojoClass = pojoClass;
        this.jsonSchema = jsonSchema;
        this.jsonFields = jsonFields;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // A Counter is used to count something.
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");

        // A Histogram measures the distribution of long values. ex: timestamp
        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(50000));
        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));

        // A Meter measures an average throughput.
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public void process(ProcessAllWindowFunction<String, T, TimeWindow>.Context context, Iterable<String> elements, Collector<T> out) throws Exception {
        elements.forEach(value -> {
            this.counter.inc();
            this.histogram.update(System.currentTimeMillis());
            this.meter.markEvent();

            JsonNode jsonValue = StreamUtils.getJsonNodeByFiledName(value, jsonFields);
            Set<ValidationMessage> groupErrors = StreamUtils.validateJsonByJsonSchema(jsonValue, jsonSchema);

            if (CollectionUtils.isNotEmpty(groupErrors)) {
                LOG.error("Errors while processing json {}", value);
                LOG.error("Errors {}", groupErrors);
            } else {
                out.collect(StreamUtils.mapJsonToPojo(jsonValue, pojoClass));
            }
        });
    }

    // Whenever a checkpoint has to be performed, snapshotState() is called.
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        if (oldCounterValue != counter.getCount()) {
            Iterator<Long> processedRecordCountIterator = processedRecordCount.get().iterator();
            Long oldProcessedRecordCount = 0L;
            if (processedRecordCountIterator.hasNext()) {
                oldProcessedRecordCount = processedRecordCountIterator.next();
            }
            long newProcessedRecordCount = oldProcessedRecordCount + counter.getCount();
            processedRecordCount.clear();
            processedRecordCount.add(newProcessedRecordCount);
            this.oldCounterValue = this.counter.getCount();
            LOG.info("processedRecordCount state value updated {}", newProcessedRecordCount);
        }

    }

    //initializeState(), is called every time the user-defined function is initialized
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                "processed-record-count",
                TypeInformation.of(new TypeHint<Long>() {
                }));

        processedRecordCount = context.getOperatorStateStore().getUnionListState(descriptor);
    }
}
