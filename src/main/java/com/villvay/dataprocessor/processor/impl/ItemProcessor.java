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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

        env.enableCheckpointing(500); // Enable checkpointing with a 500ms interval
        CheckpointConfig checkpointConfig = env.getCheckpointConfig(); // Configure checkpointing behavior
        checkpointConfig.setMinPauseBetweenCheckpoints(500); // make sure 500 ms of progress happen between checkpoints
        checkpointConfig.setCheckpointTimeout(60000); // checkpoints have to complete within one minute, or are discarded
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // Retain the checkpoint when the job is cancelled.
        String projectDirectory = System.getProperty("user.dir");

        checkpointConfig.setCheckpointStorage("file:" + projectDirectory + "/checkpoints");

        // If checkpointing is enabled, the default value is fixed-delay with Integer.MAX_VALUE restart attempts and '1 s' delay.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // Number of restart attempts
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS) // Delay between restarts
        ));

        KafkaStreamSource kafkaStreamSource = ((KafkaStreamSource) StreamSourceFactory.getInstance(ConnectorType.KAFKA));
        ActiveMqStreamSink activeMqStreamSink = (ActiveMqStreamSink) StreamSinkFactory.getInstance(ConnectorType.ACTIVE_MQ);

        DataStreamSource<String> kafkaSource = env.fromSource(kafkaStreamSource.getSource(this.itemParameters),
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0)),
                "Kafka Source");

        LOG.info("Successfully assigned kafka source connector for item processor");

        DataProcessor<ItemDto> itemDataProcessor = new DataProcessor<>(ItemDto.class,
                itemParameters.get("sinks.activemq.json_schema"), "payload");

        SingleOutputStreamOperator<ItemDto> itemDtoStream = kafkaSource.uid("item-kafka-soruce")
                .setParallelism(4)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .process(itemDataProcessor, TypeInformation.of(ItemDto.class)).uid("item-process")
                .returns(ItemDto.class);

        SingleOutputStreamOperator<String> itemDtoJsonStream = itemDtoStream
                .filter(Objects::nonNull).uid("item-filter")
                .setParallelism(4)
                .map(value -> new ObjectMapper().writeValueAsString(value)).uid("item-map")
                .setParallelism(4)
                .returns(String.class);

        itemDtoJsonStream.addSink(activeMqStreamSink.getSink(this.itemParameters)).uid("item-activemq-sink")
                .setParallelism(4);

        LOG.info("Successfully assigned kafka sink connector for item processor");

        try {
            env.setMaxParallelism(4);
            env.disableOperatorChaining(); // if this is not disables, then flink job graph will show all operators in one thread if there is no shuffling/mixing involved.

            env.execute("Item Processor Job");
        } catch (Exception e) {
            throw new DataProcessorException("Error when executing item processor job", e);
        }
    }
}
