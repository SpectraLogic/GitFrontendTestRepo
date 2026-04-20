package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface OrderedBlobEntry extends OrderedEntry<OrderedBlobEntry>, BlobObservable<OrderedBlobEntry>, SimpleBeanSafeToProxy {
}
