package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

import java.util.UUID;

@ExcludeDefaultsFromMarshaler
public interface DataReplicationRuleApiBean extends DataReplicationRule<DataReplicationRuleApiBean> {

    @ExcludeFromMarshaler(ExcludeFromMarshaler.When.ALWAYS)
    UUID getDataPolicyId();

    String TARGET_NAME = "targetName";

    @MarshalXmlAsAttribute
    String getTargetName();

    void setTargetName(final String targetName);


}
