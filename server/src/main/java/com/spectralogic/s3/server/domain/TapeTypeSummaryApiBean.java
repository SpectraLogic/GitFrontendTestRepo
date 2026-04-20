package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "TapeTypeSummary" )
public interface TapeTypeSummaryApiBean extends SimpleBeanSafeToProxy {

    String TYPE = "type";

    TapeType getType();

    void setType(final TapeType value);

    String COUNT = "count";

    int getCount();

    void setCount(final int value);


    String TOTAL_STORAGE_CAPACITY = "totalStorageCapacity";

    long getTotalStorageCapacity();

    void setTotalStorageCapacity(final long value);


    String USED_STORAGE_CAPACITY = "usedStorageCapacity";

    long getUsedStorageCapacity();

    void setUsedStorageCapacity(final long value);


    String AVAILABLE_STORAGE_CAPACITY = "availableStorageCapacity";

    long getAvailableStorageCapacity();

    void setAvailableStorageCapacity(final long value);
}
