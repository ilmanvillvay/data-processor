package com.villvay.dataprocessor.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.villvay.dataprocessor.exception.DataProcessorException;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Set;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class StreamUtils {

    private StreamUtils() {
    }

    public static JsonNode getJsonNodeByFiledName(String jsonString, String... jsonFields) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            throw new DataProcessorException("Error when extracting fields [" + jsonFields + "] from json [" + jsonString + "]", e);
        }

        for (String jsonField : jsonFields) {
            jsonNode = jsonNode.get(jsonField);
        }
        return jsonNode;
    }

    public static <T> T mapJsonToPojo(JsonNode jsonNode, Class<T> targetType) {
        try {
            return new ObjectMapper().treeToValue(jsonNode, targetType);
        } catch (JsonProcessingException e) {
            throw new DataProcessorException("Error when mapping to pojo from json [" + jsonNode.toString() + "]", e);
        }
    }

    public static Set<ValidationMessage> validateJsonByJsonSchema(JsonNode jsonNode, String jsonSchemaPath) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        JsonSchema jsonSchema = factory.getSchema(StreamUtils.class.getResourceAsStream(jsonSchemaPath));
        return jsonSchema.validate(jsonNode);
    }

    public static ParameterTool getParamsFromPropertyFile(String propertyFilePath) {
        try {
            return ParameterTool.fromPropertiesFile(propertyFilePath);
        } catch (IOException e) {
            throw new DataProcessorException("Error when loading configs from property file [" + propertyFilePath + "]", e);
        }
    }
}
