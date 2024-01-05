package com.villvay.dataprocessor.sink;

import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.sink.impl.ActiveMqStreamSink;
import com.villvay.dataprocessor.sink.impl.HttpAsyncStreamSink;
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
        } else if (ConnectorType.HTTP_ASYNC.equals(connectorType)) {
            return new HttpAsyncStreamSink();
        }

        throw new NotImplementedException("Connector for Stream Sink not implemented");
    }
}
