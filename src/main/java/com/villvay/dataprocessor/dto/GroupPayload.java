package com.villvay.dataprocessor.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Ilman Iqbal
 * 11/14/2023
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class GroupPayload implements Serializable {

    @JsonProperty("MaterialNumber")
    private String materialNumber;

    @JsonProperty("GroupCode")
    private String groupCode;

    @JsonProperty("SAP#")
    private String sapNo;

    @JsonProperty("GroupTitle")
    private String groupTitle;

    @JsonProperty("ProductTitle")
    private String productTitle;

    @JsonProperty("CategoryCode")
    private String categoryCode;
}

