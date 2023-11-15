package com.villvay.dataprocessor.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class Extractor {

    public static JsonNode getJsonNode(String jsonString, String... jsonFields) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        for (String jsonField : jsonFields) {
            jsonNode = jsonNode.get(jsonField);
        }
        return jsonNode;
    }
}
