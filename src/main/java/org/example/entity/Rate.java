package org.example.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.example.utils.Utils;

@Getter
@Setter
@AllArgsConstructor
public class Rate {
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private long rateTime;
    private float rate;
    private String item;

    @Override
    public String toString() {
        return "Rate{" +
                "rateTime=" + Utils.timestampFormat(rateTime) +
                ", rate=" + rate +
                ", item='" + item + '\'' +
                '}';
    }
}
