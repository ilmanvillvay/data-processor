package com.villvay.dataprocessor.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.villvay.dataprocessor.dto.GroupPayload;
import com.villvay.dataprocessor.mapper.DataMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.activemq.AMQSink;
import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class GroupProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(GroupProcessor.class);

    private ParameterTool groupParameters;

    public GroupProcessor() {
        loadConfig();
        execute();
    }

    private void loadConfig() {
        String propertiesFilePath = "src/main/resources/file_configs/group.properties";
        try {
            groupParameters = ParameterTool.fromPropertiesFile(propertiesFilePath);
            LOG.info("successfully loaded groups");
        } catch (IOException e) {
            LOG.error("Error while loading group config");
            throw new RuntimeException(e);
        }
    }

    private void execute() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> kafkaSource = env.fromSource(this.getSource(), WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        SingleOutputStreamOperator<String> stream = kafkaSource.map(new DataMapper<>(GroupPayload.class,
                        groupParameters.get("sinks.activemq.json_schema"), "payload"))
                .returns(GroupPayload.class)
                .filter(Objects::nonNull)
                .map(value -> new ObjectMapper().writeValueAsString(value));

        stream.print();

        stream.addSink(this.getSink());

        try {
            env.execute("Group Processor Job");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private SinkFunction<String> getSink() {
        AMQSinkConfig<String> sinkConnector = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
                .setConnectionFactory(new ActiveMQConnectionFactory(groupParameters.get("sinks.activemq.url")))
                .setDestinationName(groupParameters.get("sinks.activemq.queue_name"))
                .setSerializationSchema(new SimpleStringSchema())
                .build();

        return new AMQSink<>(sinkConnector);
    }

    private KafkaSource<String> getSource() {
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
