package com.villvay.dataprocessor.sink;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public interface StreamSink {

    Object getSink(ParameterTool groupParameters);
}
