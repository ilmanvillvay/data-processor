package com.villvay.dataprocessor.processor.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.villvay.dataprocessor.dto.ItemDto;
import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.exception.DataProcessorException;
import com.villvay.dataprocessor.mapper.DataProcessor;
import com.villvay.dataprocessor.processor.StreamProcessor;
import com.villvay.dataprocessor.sink.StreamSinkFactory;
import com.villvay.dataprocessor.sink.impl.ActiveMqStreamSink;
import com.villvay.dataprocessor.source.StreamSourceFactory;
import com.villvay.dataprocessor.source.impl.KafkaStreamSource;
import com.villvay.dataprocessor.util.StreamUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

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
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0)),
                "Kafka Source");

        LOG.info("Successfully assigned kafka source connector for item processor");

        DataProcessor<ItemDto> itemDataProcessor = new DataProcessor<>(ItemDto.class,
                itemParameters.get("sinks.activemq.json_schema"), "payload");

        SingleOutputStreamOperator<ItemDto> itemDtoStream = kafkaSource
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .process(itemDataProcessor, TypeInformation.of(ItemDto.class))
                .returns(ItemDto.class);

        SingleOutputStreamOperator<String> itemDtoJsonStream = itemDtoStream
                .filter(Objects::nonNull)
                .map(value -> new ObjectMapper().writeValueAsString(value))
                .returns(String.class);

        itemDtoJsonStream.addSink(activeMqStreamSink.getSink(this.itemParameters));

        LOG.info("Successfully assigned kafka sink connector for item processor");

        try {
            env.execute("Item Processor Job");
        } catch (Exception e) {
            throw new DataProcessorException("Error when executing item processor job", e);
        }
    }
}
