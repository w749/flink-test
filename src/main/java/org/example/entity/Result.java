package org.example.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Result {
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private long orderTime;
    private float realPrice;
    private String item;
}
