package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

import java.util.UUID;

@ExcludeDefaultsFromMarshaler
public interface StorageDomainMemberApiBean extends StorageDomainMember {

    @MarshalXmlAsAttribute
    TapeType getTapeType();


    @ExcludeFromMarshaler(ExcludeFromMarshaler.When.ALWAYS)
    UUID getStorageDomainId();


    String PARTITION_NAME = "partitionName";

    @MarshalXmlAsAttribute
    String getPartitionName();

    void setPartitionName(final String partitionName);
}
