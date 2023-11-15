package com.villvay.dataprocessor.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.ValidationMessage;
import com.villvay.dataprocessor.dto.GroupPayload;
import com.villvay.dataprocessor.util.Extractor;
import com.villvay.dataprocessor.util.Transformer;
import com.villvay.dataprocessor.util.Validator;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.activemq.AMQSink;
import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class GroupExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GroupExecutor.class);

    private ParameterTool groupParameters;

    public GroupExecutor(StreamExecutionEnvironment env) {
        loadGroupConfig();
        configureGroupExecutor(env);
    }

    private void loadGroupConfig() {
        String propertiesFilePath = "src/main/resources/file_configs/group.properties";
        try {
            groupParameters = ParameterTool.fromPropertiesFile(propertiesFilePath);
            LOG.info("successfully loaded groups");
        } catch (IOException e) {
            LOG.error("Error while loading group config");
            throw new RuntimeException(e);
        }
    }

    private void configureGroupExecutor(StreamExecutionEnvironment env) {
        DataStreamSource<String> kafkaSource = env.fromSource(this.getSource(), WatermarkStrategy.noWatermarks(), "Kafka Source");

//        todo: the following map function should get this value like this, not in a hard coded way
//        this.groupParameters.get("sink[0].json_schema");

        SingleOutputStreamOperator<GroupPayload> stream = kafkaSource.map((MapFunction<String, GroupPayload>) value -> {

                    JsonNode jsonValue = Extractor.getJsonNode(value, "payload");
                    Set<ValidationMessage> groupErrors = Validator.validateJson(jsonValue, "/json_schemas/group_activemq.json");

                    if (CollectionUtils.isNotEmpty(groupErrors)) {
                LOG.error(groupErrors.toString());
                        return null;
                    } else {
                        return Transformer.mapJsonToPojo(jsonValue, GroupPayload.class);
                    }
                })
                .filter(Objects::nonNull);

        stream.print();

        stream.sinkTo((Sink<GroupPayload>) this.getSink());

    }

    private AMQSink<String> getSink() {
        AMQSinkConfig<String> sinkConnector = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
                .setConnectionFactory(new ActiveMQConnectionFactory(groupParameters.get("sink[0].url")))
                .setDestinationName("some_queue")
                .setSerializationSchema(new SimpleStringSchema())
                .build();

        return new AMQSink<>(sinkConnector);
    }

    private KafkaSource<String> getSource() {
        OffsetsInitializer offsetsInitializer;

        switch (groupParameters.get("kafka.consumer.offset")) {
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
                .setBootstrapServers(groupParameters.get("kafka.url"))
                .setTopics(groupParameters.get("kafka.topic"))
                .setGroupId(groupParameters.get("kafka.consumer.group-id"))
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    }


}
