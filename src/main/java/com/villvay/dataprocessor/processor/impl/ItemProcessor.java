package com.villvay.dataprocessor.processor.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.villvay.dataprocessor.dto.ItemDto;
import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.exception.DataProcessorException;
import com.villvay.dataprocessor.mapper.DataMapper;
import com.villvay.dataprocessor.processor.StreamProcessor;
import com.villvay.dataprocessor.sink.StreamSinkFactory;
import com.villvay.dataprocessor.sink.impl.ActiveMqStreamSink;
import com.villvay.dataprocessor.source.StreamSourceFactory;
import com.villvay.dataprocessor.source.impl.KafkaStreamSource;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Ilman Iqbal
 * 11/16/2023
 */
public class ItemProcessor implements StreamProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ItemProcessor.class);

    private final ParameterTool itemParameters;

    public ItemProcessor() {
        this.itemParameters = StreamUtils.getParamsFromPropertyFile("src/main/resources/file_configs/item.properties");
    }

    @Override
    public void start() {
        LOG.info("Started execution of item processor");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaStreamSource kafkaStreamSource = ((KafkaStreamSource) StreamSourceFactory.getInstance(ConnectorType.KAFKA));
        ActiveMqStreamSink activeMqStreamSink = (ActiveMqStreamSink) StreamSinkFactory.getInstance(ConnectorType.ACTIVE_MQ);

        DataStreamSource<String> kafkaSource = env.fromSource(kafkaStreamSource.getSource(this.itemParameters),
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        LOG.info("Successfully assigned kafka source connector for item processor");

        SingleOutputStreamOperator<String> stream = kafkaSource.map(new DataMapper<>(ItemDto.class,
                        itemParameters.get("sinks.activemq.json_schema"), "payload"))
                .returns(ItemDto.class)
                .filter(Objects::nonNull)
                .map(value -> new ObjectMapper().writeValueAsString(value));

        stream.addSink(activeMqStreamSink.getSink(this.itemParameters));

        stream.print();

        LOG.info("Successfully assigned kafka sink connector for item processor");

        try {
            JobExecutionResult itemProcessorJob = env.execute("Item Processor Job");
            System.out.println("=======================================================================================");
            System.out.println("=======================================================================================");
            System.out.println("=======================================================================================");
            System.out.println("=======================================================================================");
            System.out.println("=======================================================================================");
            System.out.println("The job took " + itemProcessorJob.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute");

            LOG.info("=================================================================================================");
            LOG.info("=================================================================================================");
            LOG.info("=================================================================================================");
            LOG.info("=================================================================================================");
            LOG.info("=================================================================================================");
            LOG.info("The job took " + itemProcessorJob.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute");
        } catch (Exception e) {
            throw new DataProcessorException("Error when executing item processor job", e);
        }
    }
}
