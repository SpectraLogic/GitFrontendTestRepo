package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

import java.util.UUID;

@ExcludeDefaultsFromMarshaler
public interface DataPersistenceRuleApiBean extends DataPersistenceRule {
    @ExcludeFromMarshaler(ExcludeFromMarshaler.When.ALWAYS)
    UUID getDataPolicyId();

    String STORAGE_DOMAIN_NAME = "storageDomainName";

    @MarshalXmlAsAttribute
    String getStorageDomainName();

    void setStorageDomainName(final String storageDomainName);
}
