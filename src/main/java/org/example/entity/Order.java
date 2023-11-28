package org.example.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.example.utils.Utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

@Getter
@Setter
public class Order implements Serializable {
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private long orderTime;
    private int price;
    private String item;

    @Override
    public String toString() {
        return "Order{" +
                "orderTime=" + Utils.timestampFormat(orderTime) +
                ", price=" + price +
                ", item='" + item + '\'' +
                '}';
    }
}
