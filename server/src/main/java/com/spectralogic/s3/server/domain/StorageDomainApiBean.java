package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.WriteOptimization;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@ExcludeDefaultsFromMarshaler
public interface StorageDomainApiBean extends StorageDomain {

    @MarshalXmlAsAttribute
    String getName();


    String STORAGE_DOMAIN_MEMBERS = "storageDomainMembers";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "StorageDomainMember",
            collectionValue = "StorageDomainMembers",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    StorageDomainMemberApiBean[] getStorageDomainMembers();

    void setStorageDomainMembers(final StorageDomainMemberApiBean[] storageDomainMembers);
}
