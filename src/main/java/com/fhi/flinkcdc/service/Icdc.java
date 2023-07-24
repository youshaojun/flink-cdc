package com.fhi.flinkcdc.service;

import com.fhi.flinkcdc.props.BaseProperties;

import java.io.Serializable;

public interface Icdc<T> extends Serializable {

    void exec(BaseProperties baseProperties);

    T reader(BaseProperties baseProperties);

    void writer(T source, BaseProperties baseProperties);

}
