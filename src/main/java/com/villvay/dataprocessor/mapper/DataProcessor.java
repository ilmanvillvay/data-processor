package com.villvay.dataprocessor.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.ValidationMessage;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


/**
 * @author Ilman Iqbal
 * 11/16/2023
 */
public class DataProcessor<T> extends ProcessAllWindowFunction<String, T, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    Class<T> pojoClass;
    String jsonSchema;
    String[] jsonFields;

    public DataProcessor(Class<T> pojoClass, String jsonSchema, String... jsonFields) {
        this.pojoClass = pojoClass;
        this.jsonSchema = jsonSchema;
        this.jsonFields = jsonFields;
    }


    @Override
    public void process(ProcessAllWindowFunction<String, T, TimeWindow>.Context context, Iterable<String> elements, Collector<T> out) throws Exception {
        elements.forEach(value -> {
            long start = System.currentTimeMillis();
            JsonNode jsonValue = StreamUtils.getJsonNodeByFiledName(value, jsonFields);
            Set<ValidationMessage> groupErrors = StreamUtils.validateJsonByJsonSchema(jsonValue, jsonSchema);

            if (CollectionUtils.isNotEmpty(groupErrors)) {
                LOG.error("Errors while processing json {}", value);
                LOG.error("Errors {}", groupErrors);
            } else {
                T pojo = StreamUtils.mapJsonToPojo(jsonValue, pojoClass);
                long time = System.currentTimeMillis() - start;
                LOG.warn("============== time taken in millis for mapping {}", time);
                out.collect(pojo);
            }
        });
    }
}
