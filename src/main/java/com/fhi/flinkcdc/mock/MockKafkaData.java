package com.fhi.flinkcdc.mock;

import com.alibaba.fastjson.JSONObject;
import com.fhi.flinkcdc.service.impl.Kafka2MySQLCdcImpl;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MockKafkaData {

    @SneakyThrows
    public static void writeToKafka(KafkaDataModel kafkaDataModel) {
        Properties props = Kafka2MySQLCdcImpl.getProps(kafkaDataModel.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < kafkaDataModel.getCycle(); i++) {
            Thread.sleep(100L);
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaDataModel.getTopic(), null, null, JSONObject.toJSONString(kafkaDataModel.getData()));
            producer.send(record);
            if (i % 10 == 0) {
                producer.flush();
            }
        }
    }
}
