package com.fhi.flinkcdc.controller;

import com.fhi.flinkcdc.mock.KafkaDataModel;
import com.fhi.flinkcdc.mock.MockKafkaData;
import com.fhi.flinkcdc.props.Kafka2MySQLProperties;
import com.fhi.flinkcdc.service.Icdc;
import com.fhi.flinkcdc.util.CdcUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("/cdc")
@RequiredArgsConstructor
public class Kafka2MySQLCdcController {

    private final Icdc icdc;

    @GetMapping("/list")
    public Set<String> list() {
        return CdcUtil.JOB_LOCK.keySet();
    }

    @PostMapping("/kafka2MySQL")
    public String kafka2MySQL(@RequestBody @Validated Kafka2MySQLProperties kafka2MySQLProperties) {
        icdc.exec(kafka2MySQLProperties);
        return "ok";
    }

    @GetMapping("/close")
    public String close(String jobName) {
        CdcUtil.close(jobName);
        return "ok";
    }

    @PostMapping("/mockKafkaData")
    public String mock(@RequestBody @Validated KafkaDataModel kafkaDataModel) {
        MockKafkaData.writeToKafka(kafkaDataModel);
        return "ok";
    }

}
