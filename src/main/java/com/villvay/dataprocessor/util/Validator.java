package com.villvay.dataprocessor.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.util.Set;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class Validator {

    private Validator() {
    }

    public static Set<ValidationMessage> validateJsonByJsonSchema(JsonNode jsonNode, String jsonSchemaPath) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        JsonSchema jsonSchema = factory.getSchema(Validator.class.getResourceAsStream(jsonSchemaPath));
        return jsonSchema.validate(jsonNode);
    }
}
