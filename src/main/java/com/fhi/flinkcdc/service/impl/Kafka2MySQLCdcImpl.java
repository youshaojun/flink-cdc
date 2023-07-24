package com.fhi.flinkcdc.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fhi.flinkcdc.service.Icdc;
import com.fhi.flinkcdc.props.BaseProperties;
import com.fhi.flinkcdc.props.Kafka2MySQLProperties;
import com.fhi.flinkcdc.util.CdcUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Scope("prototype")
public class Kafka2MySQLCdcImpl implements Icdc<SingleOutputStreamOperator<String>> {

    private StreamExecutionEnvironment env;

    private Kafka2MySQLProperties kafka2MySQLProperties;

    private Kafka2MySQLWriter kafka2MySQLWriter;

    private static ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>(CdcUtil.MAX_JOB_COUNT);

    public Kafka2MySQLCdcImpl() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Override
    public void exec(BaseProperties baseProperties) {
        this.kafka2MySQLProperties = (Kafka2MySQLProperties) baseProperties;
        this.kafka2MySQLWriter = new Kafka2MySQLWriter(kafka2MySQLProperties.getMysql(), baseProperties.getJobName());
        CdcUtil.tryLock(baseProperties.getJobName(), kafka2MySQLWriter);
        CdcUtil.run(() -> {
            SingleOutputStreamOperator<String> result = reader(baseProperties);
            writer(result, baseProperties);
        });
    }

    @Override
    public SingleOutputStreamOperator<String> reader(BaseProperties baseProperties) {
        Kafka2MySQLProperties.Kafka kafka = kafka2MySQLProperties.getKafka();
        Properties props = getProps(kafka.getBootstrapServers());
        if (StrUtil.isNotBlank(kafka.getGroupId())) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.getGroupId());
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return env.addSource(new FlinkKafkaConsumer<>(
                kafka.getTopic(),
                new SimpleStringSchema(),
                props))
                .setParallelism(1)
                .map(s -> s);
    }

    @SneakyThrows
    @Override
    public void writer(SingleOutputStreamOperator<String> source, BaseProperties baseProperties) {
        source.addSink(kafka2MySQLWriter);
        env.execute(kafka2MySQLProperties.getJobName());
    }

    @Slf4j
    public static class Kafka2MySQLWriter extends RichSinkFunction<String> implements Serializable {

        private Kafka2MySQLProperties.MySQL mysql;

        private String jobName;

        public Kafka2MySQLWriter(Kafka2MySQLProperties.MySQL mysql, String jobName) {
            this.mysql = mysql;
            this.jobName = jobName;
            if (!connections.containsKey(jobName)) {
                connections.put(jobName, getConnection());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            Connection connection = connections.get(jobName);
            if (connection != null) {
                connection.close();
            }
            connections.remove(jobName);
        }

        @Override
        public void invoke(String s, Context context) throws Exception {
            log.info("kafka data [{}]", s);
            try {
                int i = 1;
                Set<String> columns = mysql.getColumns();
                List<JSONObject> array = new ArrayList<>();
                try {
                    array.add(JSONObject.parseObject(s));
                } catch (Exception ex) {
                    array = JSON.parseArray(s, JSONObject.class);
                }
                for (JSONObject json : array) {
                    if (CollUtil.isEmpty(columns)) {
                        columns = json.keySet();
                    }
                    StringBuilder sb = new StringBuilder("insert into ");
                    sb.append(mysql.getTableName()).append("(");
                    columns.forEach(e -> sb.append(e).append(","));
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(") values (");
                    columns.forEach(e -> sb.append("?,"));
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(");");
                    Connection connection = connections.get(jobName);
                    if (connection == null) {
                        connection = getConnection();
                        connections.put(jobName, connection);
                    }
                    PreparedStatement ps = connection.prepareStatement(sb.toString());
                    for (String column : columns) {
                        ps.setObject(i++, json.get(column));
                    }
                    try {
                        ps.executeUpdate();
                    } catch (SQLIntegrityConstraintViolationException ex) {
                        log.error("主键冲突: ", ex);
                    }
                }
            } catch (Exception e) {
                log.error("处理失败: ", e);
                close();
                throw e;
            }
        }

        @SneakyThrows
        private Connection getConnection() {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                Properties props = new Properties();
                props.setProperty("user", mysql.getUsername());
                props.setProperty("password", mysql.getPassword());
                props.setProperty("remarks", "true");
                props.setProperty("connectTimeout", "30000");
                props.setProperty("socketTimeout", "30000");
                props.setProperty("useInformationSchema", "true");
                return DriverManager.getConnection(mysql.getUrl(), props);
            } catch (Exception e) {
                log.error(" get mysql connection has exception.", e);
                throw e;
            }
        }

    }

    public static Properties getProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

}
