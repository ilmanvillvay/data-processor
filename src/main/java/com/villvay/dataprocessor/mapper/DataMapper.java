package com.villvay.dataprocessor.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.ValidationMessage;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


/**
 * @author Ilman Iqbal
 * 11/16/2023
 */
public class DataMapper<T> implements MapFunction<String, T> {

    private static final Logger LOG = LoggerFactory.getLogger(DataMapper.class);

    Class<T> pojoClass;
    String jsonSchema;
    String[] jsonFields;

    public DataMapper(Class<T> pojoClass, String jsonSchema, String... jsonFields) {
        this.pojoClass = pojoClass;
        this.jsonSchema = jsonSchema;
        this.jsonFields = jsonFields;
    }

    @Override
    public T map(String value) {
        long start = System.currentTimeMillis();
        JsonNode jsonValue = StreamUtils.getJsonNodeByFiledName(value, jsonFields);
        Set<ValidationMessage> groupErrors = StreamUtils.validateJsonByJsonSchema(jsonValue, jsonSchema);

        if (CollectionUtils.isNotEmpty(groupErrors)) {
            LOG.error("Errors while processing json {}", value);
            LOG.error("Errors {}", groupErrors);
            return null;
        } else {
            T pojo = StreamUtils.mapJsonToPojo(jsonValue, pojoClass);
            long time = System.currentTimeMillis() - start;
            LOG.warn("============== time taken in millis for mapping {}", time);
            return pojo;
        }
    }
}
