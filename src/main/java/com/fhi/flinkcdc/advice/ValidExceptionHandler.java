package com.fhi.flinkcdc.advice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class ValidExceptionHandler {

    @ExceptionHandler(Exception.class)
    public String handler(Exception ex) {
        if (ex instanceof BindException) {
            return ((BindException) ex).getAllErrors().get(0).getDefaultMessage();
        }
        log.error("服务器异常: ", ex);
        return ex.getMessage();
    }

}