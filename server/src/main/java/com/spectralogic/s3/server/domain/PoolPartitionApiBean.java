package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@ExcludeDefaultsFromMarshaler
public interface PoolPartitionApiBean extends PoolPartition {

    @MarshalXmlAsAttribute
    String getName();


    String POOL_COUNT = "poolCount";

    int getPoolCount();

    void setPoolCount(final int poolCount);
}
