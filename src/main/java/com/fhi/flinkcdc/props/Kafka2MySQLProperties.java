package com.fhi.flinkcdc.props;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

@Data
@EqualsAndHashCode(callSuper = true)
public class Kafka2MySQLProperties extends BaseProperties implements Serializable {

    @Valid
    @NotNull(message = "kafka配置不能为空")
    private Kafka kafka = new Kafka();

    @Valid
    @NotNull(message = "MySQL配置不能为空")
    private MySQL mysql = new MySQL();

    @Data
    public static class Kafka implements Serializable {

        @NotBlank(message = "kafka server地址不能为空")
        private String bootstrapServers;

        @NotBlank(message = "kafka topic不能为空")
        private String topic;

        private String groupId;

    }

    @Data
    public static class MySQL implements Serializable {

        @NotBlank(message = "url 不能为空")
        private String url;

        @NotBlank(message = "username不能为空")
        private String username;

        @NotBlank(message = "password不能为空")
        private String password;

        @NotBlank(message = "tableName不能为空")
        private String tableName;

        private Set<String> columns = Collections.emptySet();

    }

}
