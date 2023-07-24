package com.fhi.flinkcdc.props;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.io.Serializable;

@Data
public class BaseProperties implements Serializable {

    @NotBlank(message = "jobName不能为空")
    @ApiModelProperty("任务名称")
    private String jobName;

}
