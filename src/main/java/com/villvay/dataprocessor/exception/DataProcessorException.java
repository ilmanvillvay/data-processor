package com.villvay.dataprocessor.exception;

/**
 * @author Ilman Iqbal
 * 11/17/2023
 */
public class DataProcessorException extends RuntimeException {
    private static final long serialVersionUID = -7586311382009966306L;

    public DataProcessorException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
