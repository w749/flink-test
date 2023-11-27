package org.example.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class Rate {
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private long rateTime;
    private float rate;
}
