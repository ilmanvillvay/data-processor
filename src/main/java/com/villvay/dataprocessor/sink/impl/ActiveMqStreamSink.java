package com.villvay.dataprocessor.sink.impl;

import com.villvay.dataprocessor.sink.StreamSink;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.activemq.AMQSink;
import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public class ActiveMqStreamSink implements StreamSink {
    @Override
    public SinkFunction<String> getSink(ParameterTool groupParameters) {
        AMQSinkConfig<String> sinkConnector = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
                .setConnectionFactory(new ActiveMQConnectionFactory(groupParameters.get("sinks.activemq.url")))
                .setDestinationName(groupParameters.get("sinks.activemq.queue_name"))
                .setSerializationSchema(new SimpleStringSchema())
                .build();

        return new AMQSink<>(sinkConnector);
    }
}
