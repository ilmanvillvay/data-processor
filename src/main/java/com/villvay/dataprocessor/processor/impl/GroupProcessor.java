package com.villvay.dataprocessor.processor.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.villvay.dataprocessor.dto.GroupDto;
import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.exception.DataProcessorException;
import com.villvay.dataprocessor.mapper.DataMapper;
import com.villvay.dataprocessor.processor.StreamProcessor;
import com.villvay.dataprocessor.sink.StreamSinkFactory;
import com.villvay.dataprocessor.sink.impl.ActiveMqStreamSink;
import com.villvay.dataprocessor.source.StreamSourceFactory;
import com.villvay.dataprocessor.source.impl.KafkaStreamSource;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Ilman Iqbal
 * 11/15/2023
 */
public class GroupProcessor implements StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(GroupProcessor.class);

    private final ParameterTool groupParameters;

    public GroupProcessor() {
        this.groupParameters = StreamUtils.getParamsFromPropertyFile("src/main/resources/file_configs/group.properties");
    }

    @Override
    public void start() {
        LOG.info("Started execution of group processor");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaStreamSource kafkaStreamSource = ((KafkaStreamSource) StreamSourceFactory.getInstance(ConnectorType.KAFKA));
        ActiveMqStreamSink activeMqStreamSink = (ActiveMqStreamSink) StreamSinkFactory.getInstance(ConnectorType.ACTIVE_MQ);

        DataStreamSource<String> kafkaSource = env.fromSource(kafkaStreamSource.getSource(this.groupParameters),
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        LOG.info("Successfully assigned kafka source connector for group processor");

        SingleOutputStreamOperator<String> stream = kafkaSource.map(new DataMapper<>(GroupDto.class,
                        groupParameters.get("sinks.activemq.json_schema"), "payload"))
                .returns(GroupDto.class)
                .filter(Objects::nonNull)
                .map(value -> new ObjectMapper().writeValueAsString(value));

        stream.addSink(activeMqStreamSink.getSink(this.groupParameters));

        stream.print();

        LOG.info("Successfully assigned kafka sink connector for group processor");

        try {
            env.execute("Group Processor Job");
        } catch (Exception e) {
            throw new DataProcessorException("Error when executing group processor job", e);
        }
    }
}
