package com.villvay.dataprocessor.enums;

/**
 * @author Ilman Iqbal
 * 11/20/2023
 */
public enum ConsumerOffset {

    EARLIEST("earliest"),
    LATEST("latest"),
    COMMITTED("committed");

    private final String value;

    ConsumerOffset(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
