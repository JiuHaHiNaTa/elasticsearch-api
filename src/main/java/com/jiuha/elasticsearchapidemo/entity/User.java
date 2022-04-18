package com.jiuha.elasticsearchapidemo.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class User {

    /**
     * 姓名
     */
    private String name;
    /**
     * 身份码
     */
    private String code;
    /**
     * 性别
     */
    private Integer gender;
    /**
     * 地址
     */
    private String address;
    /**
     * 余额
     */
    private BigDecimal account;
    /**
     *  最后一次更新时间
     */
    private LocalDateTime lastUpdateTime;
}
