package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface OrderedEntry<T> extends SimpleBeanSafeToProxy {

    String ORDER_INDEX = "orderIndex";

    @Optional
    Integer getOrderIndex();

    T setOrderIndex(final Integer value );
}
