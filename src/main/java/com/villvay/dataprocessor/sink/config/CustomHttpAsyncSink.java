package com.villvay.dataprocessor.sink.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Ilman Iqbal
 * 1/5/2024
 */
public class CustomHttpAsyncSink extends RichSinkFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomHttpAsyncSink.class);

    private final String endpointUrl; // HTTP endpoint URL
    private final String apiKey; // HTTP endpoint URL
    private transient AsyncHttpClient asyncHttpClient;

    public CustomHttpAsyncSink(String endpointUrl, String apiKey) {
        this.endpointUrl = endpointUrl;
        this.apiKey = apiKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncHttpClient = new DefaultAsyncHttpClient();
    }

    @Override
    public void close() throws Exception {
        if (asyncHttpClient != null) {
            asyncHttpClient.close();
        }
    }

    @Override
    public void invoke(String value, Context sinkFunctionContext) throws Exception {
        // Prepare the HTTP request
        Request request = asyncHttpClient.preparePost(endpointUrl)
                .setBody("[\n" +
                        value +
                        "]")
                .build();

        if (Objects.nonNull(apiKey)) {
            request.getHeaders().add("Authorization", apiKey);
        }

        // Send the HTTP request asynchronously
        asyncHttpClient.executeRequest(request)
                .toCompletableFuture()
                .thenAccept(this::handleResponse); // Handle the response asynchronously
    }

    private void handleResponse(Response response) {
        // Process the response as needed (e.g., check status code, log, etc.)
        int statusCode = response.getStatusCode();

        if (statusCode != 200) {
            LOG.error("Response Body: {}, Response Status Code: {}", response.getResponseBody(), response.getStatusCode());
        } else {
            LOG.info("Response Body: {}, Response Status Code: {}", response.getResponseBody(), response.getStatusCode());
        }
    }
}
