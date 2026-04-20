package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

@ExcludeDefaultsFromMarshaler
public interface AbmConfigApiBean extends SimpleBeanSafeToProxy {

    String DATA_POLICIES = "dataPolicies";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "DataPolicy",
            collectionValue = "DataPoliciesThatHaveBuckets",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    DataPolicyApiBean[] getDataPolicies();

    void setDataPolicies(final DataPolicyApiBean[] dataPolicies);


    String STORAGE_DOMAINS = "storageDomains";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "StorageDomain",
            collectionValue = "StorageDomains",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    StorageDomainApiBean[] getStorageDomains();

    void setStorageDomains(final StorageDomainApiBean[] storageDomains);


    String TAPE_PARTITIONS = "tapePartitions";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "TapePartition",
            collectionValue = "TapePartitions",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    TapePartitionApiBean[] getTapePartitions();

    void setTapePartitions(final TapePartitionApiBean[] tapePartitions);


    String POOL_PARTITIONS = "poolPartitions";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "PoolPartition",
            collectionValue = "PoolPartitions",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    PoolPartitionApiBean[] getPoolPartitions();

    void setPoolPartitions(final PoolPartitionApiBean[] poolPartitions);


    String TARGETS = "targets";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "Target",
            collectionValue = "Targets",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    TargetApiBean[] getTargets();

    void setTargets(final TargetApiBean[] targets);


    String MESSAGE = "message";

    String getMessage();

    void setMessage(final String message);
}
