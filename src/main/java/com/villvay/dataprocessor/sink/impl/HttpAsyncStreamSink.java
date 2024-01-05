package com.villvay.dataprocessor.sink.impl;

import com.villvay.dataprocessor.sink.StreamSink;
import com.villvay.dataprocessor.sink.config.CustomHttpAsyncSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Ilman Iqbal
 * 1/5/2024
 */
public class HttpAsyncStreamSink implements StreamSink {
    @Override
    public SinkFunction<String> getSink(ParameterTool relatedItemParameters) {
        return new CustomHttpAsyncSink(
                relatedItemParameters.get("sinks.http-async.url"),
                relatedItemParameters.toMap().getOrDefault("sinks.http-async.api-key", null));
    }
}
