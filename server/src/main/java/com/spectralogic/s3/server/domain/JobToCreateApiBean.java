package com.spectralogic.s3.server.domain;

import java.util.List;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface JobToCreateApiBean extends SimpleBeanSafeToProxy
{
    String OBJECTS = "objects";

    S3ObjectToJobApiBean [] getObjects();

    void setObjects( final S3ObjectToJobApiBean [] value );
}
