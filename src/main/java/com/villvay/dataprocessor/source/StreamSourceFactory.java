package com.villvay.dataprocessor.source;

import com.villvay.dataprocessor.enums.ConnectorType;
import com.villvay.dataprocessor.source.impl.KafkaStreamSource;
import org.apache.commons.lang3.NotImplementedException;


/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public class StreamSourceFactory {

    private StreamSourceFactory() {
    }

    public static StreamSource getInstance(ConnectorType connectorType) {
        if (ConnectorType.KAFKA.equals(connectorType)) {
            return new KafkaStreamSource();
        }
        throw new NotImplementedException("Connector for Stream Source not implemented");
    }
}
