package org.example.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class Order implements Serializable {
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private long orderTime;
    private int price;
    private String item;
}
