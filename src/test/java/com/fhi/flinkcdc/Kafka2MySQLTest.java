package com.fhi.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.fhi.flinkcdc.mock.KafkaDataModel;
import com.fhi.flinkcdc.mock.MockKafkaData;
import com.fhi.flinkcdc.props.Kafka2MySQLProperties;
import com.fhi.flinkcdc.service.Icdc;
import com.fhi.flinkcdc.service.impl.Kafka2MySQLCdcImpl;
import org.junit.Test;

import java.util.Date;

public class Kafka2MySQLTest {

    public static final String broker_list = "127.0.0.1:9092";
    public static final String topic = "test01";

    private static final String url = "jdbc:mysql://127.0.0.1:3306/flink_cdc?allowMultiQueries=true&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8";
    private static final String username = "root";
    private static final String password = "123456";
    private static final String tableName = "test";

    @Test
    public void push() {
        JSONObject json = new JSONObject();
        json.put("user_id", 1);
        json.put("create_time", new Date());
        json.put("url", "http://www.baidu.com");

        KafkaDataModel kafkaDataModel = new KafkaDataModel();
        kafkaDataModel.setCycle(100);
        kafkaDataModel.setBootstrapServers(broker_list);
        kafkaDataModel.setTopic(topic);
        kafkaDataModel.setData(json);
        MockKafkaData.writeToKafka(kafkaDataModel);
    }

    @Test
    public void cdc() {
        Icdc icdc = new Kafka2MySQLCdcImpl();
        Kafka2MySQLProperties kafka2MySQLProperties = new Kafka2MySQLProperties();
        kafka2MySQLProperties.setJobName("test111");
        Kafka2MySQLProperties.Kafka kafka = kafka2MySQLProperties.getKafka();
        kafka.setBootstrapServers(broker_list);
        kafka.setTopic(topic);

        Kafka2MySQLProperties.MySQL mysql = kafka2MySQLProperties.getMysql();
        mysql.setUrl(url);
        mysql.setUsername(username);
        mysql.setPassword(password);
        mysql.setTableName(tableName);
        icdc.exec(kafka2MySQLProperties);
    }

}
