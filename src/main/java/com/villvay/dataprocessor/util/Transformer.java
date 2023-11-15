package com.villvay.dataprocessor.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class Transformer {

    public static <T> T mapJsonToPojo(JsonNode jsonNode, Class<T> targetType) {
        try {
            return new ObjectMapper().treeToValue(jsonNode, targetType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
