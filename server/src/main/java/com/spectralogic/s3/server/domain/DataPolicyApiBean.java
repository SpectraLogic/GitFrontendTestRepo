package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@ExcludeDefaultsFromMarshaler
public interface DataPolicyApiBean extends DataPolicy {

    @MarshalXmlAsAttribute
    String getName();


    String DATA_PERSISTENCE_RULES = "dataPersistenceRules";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "DataPersistenceRule",
            collectionValue = "LocalCopies",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    DataPersistenceRuleApiBean[] getDataPersistenceRules();

    void setDataPersistenceRules(final DataPersistenceRuleApiBean[] dataPersistenceRules);


    String DATA_REPLICATION_RULES = "dataReplicationRules";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "DataReplicationRule",
            collectionValue = "RemoteCopies",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    DataReplicationRuleApiBean[] getDataReplicationRules();

    void setDataReplicationRules(final DataReplicationRuleApiBean[] dataReplicationRules);

    String BUCKETS = "buckets";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "Bucket",
            collectionValue = "Buckets",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    HumanReadableBucketApiBean[] getBuckets();

    void setBuckets(final HumanReadableBucketApiBean[] buckets);
}
