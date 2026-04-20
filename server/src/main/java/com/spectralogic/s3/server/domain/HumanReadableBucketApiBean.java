package com.spectralogic.s3.server.domain;

import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

public interface HumanReadableBucketApiBean extends BucketApiBean {

    @MarshalXmlAsAttribute
    String getName();
}
