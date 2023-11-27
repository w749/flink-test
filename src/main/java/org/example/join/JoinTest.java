package org.example.join;

import lombok.SneakyThrows;
import org.example.base.Base;
import org.example.utils.Utils;

public class JoinTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
//        dataStream1.map(Utils::entityToJson).print();
        rateDataStream.map(Utils::entityToJson).print();
        environment.execute();
    }
}
