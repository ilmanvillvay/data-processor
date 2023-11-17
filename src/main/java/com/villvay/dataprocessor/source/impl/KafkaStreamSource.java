package com.villvay.dataprocessor.source.impl;

import com.villvay.dataprocessor.source.StreamSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public class KafkaStreamSource implements StreamSource {

    @Override
    public KafkaSource<String> getSource(ParameterTool groupParameters) {
        OffsetsInitializer offsetsInitializer;

        switch (groupParameters.get("source.kafka.consumer.offset")) {
            case "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "latest":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case "committed":
                offsetsInitializer = OffsetsInitializer.committedOffsets();
                break;
            default:
                offsetsInitializer = OffsetsInitializer.committedOffsets();
        }

        return KafkaSource.<String>builder()
                .setBootstrapServers(groupParameters.get("source.kafka.url"))
                .setTopics(groupParameters.get("source.kafka.topic"))
                .setGroupId(groupParameters.get("source.kafka.consumer.group-id"))
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    }
}
