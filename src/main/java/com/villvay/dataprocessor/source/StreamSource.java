package com.villvay.dataprocessor.source;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public interface StreamSource {

    Object getSource(ParameterTool groupParameters);
}
