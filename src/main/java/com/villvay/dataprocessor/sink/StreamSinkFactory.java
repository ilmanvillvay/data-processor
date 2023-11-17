package com.villvay.dataprocessor.sink;

import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.sink.impl.ActiveMqStreamSink;
import org.apache.commons.lang3.NotImplementedException;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public class StreamSinkFactory {

    private StreamSinkFactory() {
    }

    public static StreamSink getInstance(ConnectorType connectorType) {
        if (ConnectorType.ACTIVE_MQ.equals(connectorType)) {
            return new ActiveMqStreamSink();
        }

        throw new NotImplementedException("Connector for Stream Sink not implemented");
    }
}
