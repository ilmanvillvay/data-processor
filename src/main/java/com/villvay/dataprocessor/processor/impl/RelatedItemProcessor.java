package com.villvay.dataprocessor.processor.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.villvay.dataprocessor.dto.RelatedItemDto;
import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.exception.DataProcessorException;
import com.villvay.dataprocessor.mapper.DataMapper;
import com.villvay.dataprocessor.processor.StreamProcessor;
import com.villvay.dataprocessor.sink.StreamSinkFactory;
import com.villvay.dataprocessor.sink.impl.HttpAsyncStreamSink;
import com.villvay.dataprocessor.source.StreamSourceFactory;
import com.villvay.dataprocessor.source.impl.KafkaStreamSource;
import com.villvay.dataprocessor.util.StreamUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Ilman Iqbal
 * 1/5/2024
 */
public class RelatedItemProcessor implements StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RelatedItemProcessor.class);

    private final ParameterTool relatedItemParameters;

    public RelatedItemProcessor() {
        this.relatedItemParameters = StreamUtils.getParamsFromPropertyFile("src/main/resources/file_configs/related_item.properties");
    }

    @Override
    public void start() {
        LOG.info("Started execution of related item processor");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaStreamSource kafkaStreamSource = ((KafkaStreamSource) StreamSourceFactory.getInstance(ConnectorType.KAFKA));
        HttpAsyncStreamSink httpAsyncStreamSink = ((HttpAsyncStreamSink) StreamSinkFactory.getInstance(ConnectorType.HTTP_ASYNC));

        DataStreamSource<String> kafkaSource = env.fromSource(kafkaStreamSource.getSource(this.relatedItemParameters),
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        LOG.info("Successfully assigned kafka source connector for related item processor");

        SingleOutputStreamOperator<String> stream = kafkaSource.map(new DataMapper<>(RelatedItemDto.class,
                        relatedItemParameters.get("sinks.http-async.json_schema"), "payload"))
                .returns(RelatedItemDto.class)
                .filter(Objects::nonNull)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<RelatedItemDto, StringBuilder, String>() {

                    @Override
                    public StringBuilder createAccumulator() {
                        return new StringBuilder("[");
                    }

                    @SneakyThrows
                    @Override
                    public StringBuilder add(RelatedItemDto value, StringBuilder accumulator) {
                        if (accumulator.length() > 1) {
                            accumulator.append(",");
                        }
                        accumulator.append(new ObjectMapper().writeValueAsString(value));
                        return accumulator;
                    }

                    @Override
                    public String getResult(StringBuilder accumulator) {
                        accumulator.append("]");
                        return accumulator.toString();
                    }

                    @Override
                    public StringBuilder merge(StringBuilder a, StringBuilder b) {
                        return null;
                    }
                });

        stream.print();

        stream.addSink(httpAsyncStreamSink.getSink(this.relatedItemParameters));

        LOG.info("Successfully assigned kafka sink connector for related item processor");

        try {
            env.execute("Related Item Processor Job");
        } catch (Exception e) {
            throw new DataProcessorException("Error when executing related item processor job", e);
        }
    }
}
