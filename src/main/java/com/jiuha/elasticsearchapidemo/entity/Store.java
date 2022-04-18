package com.jiuha.elasticsearchapidemo.entity;

import lombok.Data;

@Data
public class Store {

    /**
     * 店名
     */
    private String name;
    /**
     * 位置
     */
    private String location;
    /**
     * 城市
     */
    private String city;
}
